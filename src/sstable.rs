use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{u8, usize};

use crate::mem_table::MemTable;

/// SSTableEntry is a single entry returned from an SSTable lookup.
///
/// A `None` value indicates the key was deleted (tombstone).
pub struct SSTableEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
}

/// SSTable is an immutable, sorted on-disk table flushed from a [`MemTable`].
///
/// Each SSTable file stores entries in sorted key order using the binary format:
///
/// ```text
/// [ key_len: usize ][ key: [u8] ][ deleted: u8 ][ val_len: usize ][ value: [u8] ][ timestamp: u128 ]
/// ```
///
/// `val_len` and `value` are omitted for deleted (tombstone) entries.
///
/// An in-memory offset index enables O(log n) binary search without scanning the file.
pub struct SSTable {
    file: BufReader<File>,
    path: PathBuf,
    /// Byte offset of each entry in the file, in sorted key order.
    offsets: Vec<u64>,
    /// Smallest key stored in this table.
    low_key: Vec<u8>,
    /// Largest key stored in this table.
    high_key: Vec<u8>,
}

impl SSTable {
    /// Flushes `memtable` to a new SSTable file under `dir/<level>/<timestamp>.sstable`.
    ///
    /// Entries are written in the sorted order of the MemTable. The offset of each
    /// entry is recorded so that [`get`](SSTable::get) can binary search without a
    /// full file scan.
    pub fn new(memtable: &MemTable, level: usize, dir: &Path) -> io::Result<SSTable> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let path = Path::new(dir).join(format!("{}/{}.sstable", level, timestamp.to_string()));

        create_dir_all(path.parent().unwrap())?;

        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let mut file = BufWriter::new(file);

        let mut offsets = Vec::new();
        let mut offset = 0;
        for entry in memtable.entries() {
            offsets.push(offset as u64);

            file.write_all(&entry.key.len().to_le_bytes())?;
            file.write_all(&entry.key)?;

            file.write_all(&(entry.deleted as u8).to_le_bytes())?;

            if !entry.deleted {
                let value = entry.value.as_ref().unwrap();
                file.write_all(&value.len().to_le_bytes())?;
                file.write_all(value)?;
            }

            file.write_all(&entry.timestamp.to_le_bytes())?;

            offset += size_of::<usize>()
                + size_of::<u8>()
                + size_of::<u128>()
                + entry.key.len()
                + if !entry.deleted {
                    size_of::<usize>() + entry.value.as_ref().unwrap().len()
                } else {
                    0
                }
        }

        file.flush()?;

        let file = OpenOptions::new().read(true).open(&path)?;
        let file = BufReader::new(file);

        Ok(SSTable {
            file,
            path,
            offsets,
            low_key: memtable.entries().first().unwrap().key.clone(),
            high_key: memtable.entries().last().unwrap().key.clone(),
        })
    }

    /// Reconstructs an `SSTable` from an existing file on disk.
    ///
    /// Scans the file once to rebuild the offset index and read the low/high keys,
    /// then seeks back to the beginning so the table is ready for lookups.
    pub fn load_from_path(path: &Path) -> io::Result<SSTable> {
        let file = OpenOptions::new().read(true).open(&path)?;
        let mut file = BufReader::new(file);

        let mut offsets = Vec::new();
        let mut offset = 0;
        while file.fill_buf()?.len() > 0 {
            offsets.push(offset as u64);

            let mut buf = [0u8; size_of::<usize>()];
            file.read_exact(&mut buf)?;

            let key_len = usize::from_le_bytes(buf);
            file.seek_relative(key_len as i64)?;

            let mut buf = [0u8; size_of::<u8>()];
            file.read_exact(&mut buf)?;
            let deleted = buf[0] == 1;

            let mut val_len = 0;
            if !deleted {
                let mut buf = [0u8; size_of::<usize>()];
                file.read_exact(&mut buf)?;

                val_len = usize::from_le_bytes(buf);
                file.seek_relative(val_len as i64)?;
            }

            file.seek_relative(size_of::<u128>() as i64)?;

            offset += size_of::<usize>()
                + size_of::<u8>()
                + size_of::<u128>()
                + key_len
                + if !deleted {
                    size_of::<usize>() + val_len
                } else {
                    0
                }
        }

        file.seek(SeekFrom::Start(0))?;

        let mut buf = [0u8; size_of::<usize>()];
        file.read_exact(&mut buf)?;

        let key_len = usize::from_le_bytes(buf);
        let mut low_key = vec![0u8; key_len];
        file.read_exact(&mut low_key)?;

        file.seek(SeekFrom::Start(*offsets.last().unwrap() as u64))?;

        let mut buf = [0u8; size_of::<usize>()];
        file.read_exact(&mut buf)?;

        let key_len = usize::from_le_bytes(buf);
        let mut high_key = vec![0u8; key_len];
        file.read_exact(&mut high_key)?;

        Ok(SSTable {
            file,
            path: path.to_owned(),
            offsets,
            low_key,
            high_key,
        })
    }

    /// Returns `true` if `key` falls within `[low_key, high_key]` (inclusive).
    ///
    /// Use this as a cheap pre-filter before calling [`get`](SSTable::get).
    pub fn key_in_range(&self, key: &[u8]) -> bool {
        key >= &self.low_key && key <= &self.high_key
    }

    /// Searches for `key` using binary search over the offset index.
    ///
    /// Returns:
    /// - `Ok(Some(entry))` if the key is found. For deleted keys, `entry.value` is `None`.
    /// - `Ok(None)` if the key is not present in this table.
    /// - `Err(_)` on I/O failure.
    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<SSTableEntry>> {
        let mut a = 0;
        let mut b = self.offsets.len() - 1;
        while a <= b {
            let m = ((b - a) / 2) + a;
            let offset = self.offsets[m];

            self.file.seek(SeekFrom::Start(offset))?;

            let mut buf = [0u8; size_of::<usize>()];
            self.file.read_exact(&mut buf)?;

            let key_len = usize::from_le_bytes(buf);
            let mut table_key = vec![0u8; key_len];
            self.file.read_exact(&mut table_key)?;

            if key == &table_key {
                let mut buf = [0u8; size_of::<u8>()];
                self.file.read_exact(&mut buf)?;

                let deleted = buf[0] == 1;

                let mut val = None;
                if !deleted {
                    let mut buf = [0u8; size_of::<usize>()];
                    self.file.read_exact(&mut buf)?;

                    let val_len = usize::from_le_bytes(buf);
                    let mut table_value = vec![0u8; val_len];
                    self.file.read_exact(&mut table_value)?;

                    val = Some(table_value)
                }

                let mut buf = [0u8; size_of::<u128>()];
                self.file.read_exact(&mut buf)?;

                let timestamp = u128::from_le_bytes(buf);

                return Ok(Some(SSTableEntry {
                    key: table_key,
                    value: val,
                    timestamp,
                }));
            } else if key > &table_key {
                // Check for overflows
                if m == usize::MAX {
                    return Ok(None);
                }

                a = m + 1;
            } else if key < &table_key {
                // Check for underflows
                if m == usize::MIN {
                    return Ok(None);
                }

                b = m - 1;
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::{mem_table::MemTable, sstable::SSTable};
    use std::{
        fs::{File, OpenOptions},
        io::{BufReader, Read, Seek},
    };
    use tempfile::tempdir;

    fn assert_next_entry(
        reader: &mut BufReader<File>,
        key: &[u8],
        value: Option<&[u8]>,
        timestamp: u128,
    ) {
        // Assert key lengths on disk are the same
        let mut len_buf = [0u8; size_of::<usize>()];
        reader.read_exact(&mut len_buf).unwrap();
        let key_len = usize::from_le_bytes(len_buf);
        assert_eq!(key_len, key.len());

        // Assert key values on disk are the same
        let mut file_key = vec![0u8; key_len];
        reader.read_exact(&mut file_key).unwrap();
        assert_eq!(file_key, key);

        // Assert deleted booleans on disk are the same
        let mut deleted_buf = [0u8; size_of::<u8>()];
        reader.read_exact(&mut deleted_buf).unwrap();
        let deleted = deleted_buf[0] == 1;
        assert_eq!(deleted, value.is_none());

        if !deleted {
            let mut len_buf = [0u8; size_of::<usize>()];
            reader.read_exact(&mut len_buf).unwrap();
            let val_len = usize::from_le_bytes(len_buf);
            assert_eq!(val_len, value.unwrap().len());

            // Assert key values on disk are the same
            let mut file_value = vec![0u8; val_len];
            reader.read_exact(&mut file_value).unwrap();
            assert_eq!(file_value, value.unwrap());
        }

        // Assert timestamps on disk are the same
        let mut timestamp_buf = [0u8; size_of::<u128>()];
        reader.read_exact(&mut timestamp_buf).unwrap();
        let file_timestamp = u128::from_le_bytes(timestamp_buf);
        assert_eq!(file_timestamp, timestamp);
    }

    #[test]
    fn test_new_sstable() {
        let dir = tempdir().unwrap();

        let entries: Vec<(&[u8], Option<&[u8]>, u128)> = vec![
            (b"a", Some(b"1"), 0),
            (b"b", Some(b"2"), 1),
            (b"c", Some(b"3"), 2),
            (b"d", Some(b"4"), 3),
        ];

        let mut memtable = MemTable::new();
        for entry in &entries {
            if let Some(value) = entry.1 {
                memtable.set(entry.0, value, entry.2);
            } else {
                memtable.delete(entry.0, entry.2);
            }
        }

        let table = SSTable::new(&memtable, 0, dir.path()).unwrap();

        let file = OpenOptions::new().read(true).open(&table.path).unwrap();
        let mut reader = BufReader::new(file);

        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(reader.stream_position().unwrap(), table.offsets[i]);
            assert_next_entry(&mut reader, entry.0, entry.1, entry.2);
        }
    }

    #[test]
    fn test_new_sstable_with_deleted() {
        let dir = tempdir().unwrap();

        let entries: Vec<(&[u8], Option<&[u8]>, u128)> = vec![
            (b"a", Some(b"1"), 0),
            (b"b", None, 1),
            (b"c", Some(b"3"), 2),
        ];

        let mut memtable = MemTable::new();
        for entry in &entries {
            if let Some(value) = entry.1 {
                memtable.set(entry.0, value, entry.2);
            } else {
                memtable.delete(entry.0, entry.2);
            }
        }

        let table = SSTable::new(&memtable, 0, dir.path()).unwrap();

        let file = OpenOptions::new().read(true).open(&table.path).unwrap();
        let mut reader = BufReader::new(file);

        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(reader.stream_position().unwrap(), table.offsets[i]);
            assert_next_entry(&mut reader, entry.0, entry.1, entry.2);
        }
    }

    #[test]
    fn test_load_sstable() {
        let dir = tempdir().unwrap();

        let entries: Vec<(&[u8], Option<&[u8]>, u128)> = vec![
            (b"a", Some(b"1"), 0),
            (b"b", Some(b"2"), 1),
            (b"c", Some(b"3"), 2),
            (b"d", Some(b"4"), 3),
        ];

        let mut memtable = MemTable::new();
        for entry in &entries {
            if let Some(value) = entry.1 {
                memtable.set(entry.0, value, entry.2);
            } else {
                memtable.delete(entry.0, entry.2);
            }
        }

        let table = SSTable::new(&memtable, 0, dir.path()).unwrap();

        let table = SSTable::load_from_path(&table.path).unwrap();

        let file = OpenOptions::new().read(true).open(&table.path).unwrap();
        let mut reader = BufReader::new(file);

        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(reader.stream_position().unwrap(), table.offsets[i]);
            assert_next_entry(&mut reader, entry.0, entry.1, entry.2);
        }
    }

    #[test]
    fn test_load_sstable_with_deleted() {
        let dir = tempdir().unwrap();

        let entries: Vec<(&[u8], Option<&[u8]>, u128)> = vec![
            (b"a", Some(b"1"), 0),
            (b"b", None, 1),
            (b"c", Some(b"3"), 2),
        ];

        let mut memtable = MemTable::new();
        for entry in &entries {
            if let Some(value) = entry.1 {
                memtable.set(entry.0, value, entry.2);
            } else {
                memtable.delete(entry.0, entry.2);
            }
        }

        let table = SSTable::new(&memtable, 0, dir.path()).unwrap();
        let table = SSTable::load_from_path(&table.path).unwrap();

        let file = OpenOptions::new().read(true).open(&table.path).unwrap();
        let mut reader = BufReader::new(file);

        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(reader.stream_position().unwrap(), table.offsets[i]);
            assert_next_entry(&mut reader, entry.0, entry.1, entry.2);
        }
    }

    #[test]
    fn test_key_in_range() {
        let dir = tempdir().unwrap();

        let entries: Vec<(&[u8], Option<&[u8]>, u128)> = vec![
            (b"a", Some(b"1"), 0),
            (b"b", Some(b"2"), 1),
            (b"c", Some(b"3"), 2),
            (b"d", Some(b"4"), 3),
        ];

        let mut memtable = MemTable::new();
        for entry in &entries {
            if let Some(value) = entry.1 {
                memtable.set(entry.0, value, entry.2);
            } else {
                memtable.delete(entry.0, entry.2);
            }
        }

        let table = SSTable::new(&memtable, 0, dir.path()).unwrap();

        assert!(!table.key_in_range(b"A"));
        assert!(table.key_in_range(b"c"));
        assert!(!table.key_in_range(b"AA"));
    }

    #[test]
    fn test_get() {
        let dir = tempdir().unwrap();

        let entries: Vec<(&[u8], Option<&[u8]>, u128)> = vec![
            (b"a", Some(b"1"), 0),
            (b"b", Some(b"2"), 1),
            (b"c", Some(b"3"), 2),
            (b"d", Some(b"4"), 3),
        ];

        let mut memtable = MemTable::new();
        for entry in &entries {
            if let Some(value) = entry.1 {
                memtable.set(entry.0, value, entry.2);
            } else {
                memtable.delete(entry.0, entry.2);
            }
        }

        let mut table = SSTable::new(&memtable, 0, dir.path()).unwrap();

        // Test keys in SSTable
        for entry in &entries {
            let res = table.get(entry.0).unwrap().unwrap();

            assert_eq!(res.key, entry.0);
            assert_eq!(res.value.is_some(), entry.1.is_some());
            assert_eq!(res.value.unwrap(), entry.1.unwrap());
            assert_eq!(res.timestamp, entry.2);
        }
    }

    #[test]
    fn test_get_deleted_entry() {
        let dir = tempdir().unwrap();

        let mut memtable = MemTable::new();
        memtable.set(b"a", b"1", 0);
        memtable.delete(b"b", 1);
        memtable.set(b"c", b"3", 2);

        let mut table = SSTable::new(&memtable, 0, dir.path()).unwrap();

        let res = table.get(b"b").unwrap().unwrap();
        assert_eq!(res.key, b"b");
        assert!(res.value.is_none());
        assert_eq!(res.timestamp, 1);
    }

    #[test]
    fn test_get_not_found() {
        let dir = tempdir().unwrap();

        let mut memtable = MemTable::new();
        memtable.set(b"b", b"2", 0);
        memtable.set(b"d", b"4", 1);
        memtable.set(b"f", b"6", 2);

        let mut table = SSTable::new(&memtable, 0, dir.path()).unwrap();

        // Key before all entries
        assert!(table.get(b"a").unwrap().is_none());
        // Key between entries
        assert!(table.get(b"c").unwrap().is_none());
        assert!(table.get(b"e").unwrap().is_none());
        // Key after all entries
        assert!(table.get(b"g").unwrap().is_none());
    }
}
