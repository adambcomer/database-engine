use crate::mem_table::MemTable;
use crate::utils::files_with_ext;
use crate::wal_iterator::WALEntry;
use crate::wal_iterator::WALIterator;
use std::fs::{remove_file, File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Write Ahead Log(WAL)
///
/// An append-only file that holds the operations performed on the MemTable.
/// The WAL is intended for recovery of the MemTable when the server is shutdown.
pub struct WAL {
  path: PathBuf,
  file: BufWriter<File>,
}

impl WAL {
  /// Creates a new WAL in a given directory.
  pub fn new(dir: &str) -> io::Result<WAL> {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();

    let path = Path::new(dir).join(timestamp.to_string() + ".wal");
    let file = OpenOptions::new().append(true).create(true).open(&path)?;
    let file = BufWriter::new(file);

    Ok(WAL { path, file })
  }

  /// Creates a WAL from an existing file path.
  pub fn from_path(path: &str) -> io::Result<WAL> {
    let file = OpenOptions::new().append(true).create(true).open(&path)?;
    let file = BufWriter::new(file);

    Ok(WAL {
      path: PathBuf::from(path),
      file,
    })
  }

  /// Loads the WAL(s) within a directory, returning a new WAL and the recovered MemTable.
  ///
  /// If multiple WALs exist in a directory, they are merged by file date.
  pub fn load_from_dir(dir: &str) -> io::Result<(WAL, MemTable)> {
    let mut wal_files = files_with_ext(dir, "wal");
    wal_files.sort();

    let mut new_mem_table = MemTable::new();
    let mut new_wal = WAL::new(dir)?;
    for w_f in wal_files.iter() {
      if let Ok(wal) = WAL::from_path(w_f.to_str().unwrap()) {
        for entry in wal.into_iter() {
          if entry.deleted {
            new_mem_table.delete(entry.key.as_slice(), entry.timestamp);
            new_wal.delete(entry.key.as_slice(), entry.timestamp)?;
          } else {
            new_mem_table.set(
              entry.key.as_slice(),
              entry.value.as_ref().unwrap().as_slice(),
              entry.timestamp,
            );
            new_wal.set(
              entry.key.as_slice(),
              entry.value.unwrap().as_slice(),
              entry.timestamp,
            )?;
          }
        }
      }
    }
    new_wal.flush().unwrap();
    wal_files.into_iter().for_each(|f| remove_file(f).unwrap());

    Ok((new_wal, new_mem_table))
  }

  /// Sets a Key-Value pair and the operation is appended to the WAL.
  pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
    self.file.write_all(&key.len().to_le_bytes())?;
    self.file.write_all(&(false as u8).to_le_bytes())?;
    self.file.write_all(&value.len().to_le_bytes())?;
    self.file.write_all(key)?;
    self.file.write_all(value)?;
    self.file.write_all(&timestamp.to_le_bytes())?;

    Ok(())
  }

  /// Deletes a Key-Value pair and the operation is appended to the WAL.
  ///
  /// This is achieved using tombstones.
  pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
    self.file.write_all(&key.len().to_le_bytes())?;
    self.file.write_all(&(true as u8).to_le_bytes())?;
    self.file.write_all(key)?;
    self.file.write_all(&timestamp.to_le_bytes())?;

    Ok(())
  }

  /// Flushes the WAL to disk.
  ///
  /// This is useful for applying bulk operations and flushing the final result to
  /// disk. Waiting to flush after the bulk operations have been performed will improve
  /// write performance substantially.
  pub fn flush(&mut self) -> io::Result<()> {
    self.file.flush()
  }
}

impl IntoIterator for WAL {
  type IntoIter = WALIterator;
  type Item = WALEntry;

  /// Converts a WAL into a `WALIterator` to iterate over the entries.
  fn into_iter(self) -> WALIterator {
    WALIterator::new(self.path).unwrap()
  }
}

#[cfg(test)]
mod tests {
  use crate::wal::WAL;
  use rand::Rng;
  use std::fs::{create_dir, remove_dir_all};
  use std::fs::{metadata, File, OpenOptions};
  use std::io::prelude::*;
  use std::io::BufReader;
  use std::time::{SystemTime, UNIX_EPOCH};

  fn check_entry(
    reader: &mut BufReader<File>,
    key: &[u8],
    value: Option<&[u8]>,
    timestamp: u128,
    deleted: bool,
  ) {
    let mut len_buffer = [0; 8];
    reader.read_exact(&mut len_buffer).unwrap();
    let file_key_len = usize::from_le_bytes(len_buffer);
    assert_eq!(file_key_len, key.len());

    let mut bool_buffer = [0; 1];
    reader.read_exact(&mut bool_buffer).unwrap();
    let file_deleted = bool_buffer[0] != 0;
    assert_eq!(file_deleted, deleted);

    if deleted {
      let mut file_key = vec![0; file_key_len];
      reader.read_exact(&mut file_key).unwrap();
      assert_eq!(file_key, key);
    } else {
      reader.read_exact(&mut len_buffer).unwrap();
      let file_value_len = usize::from_le_bytes(len_buffer);
      assert_eq!(file_value_len, value.unwrap().len());
      let mut file_key = vec![0; file_key_len];
      reader.read_exact(&mut file_key).unwrap();
      assert_eq!(file_key, key);
      let mut file_value = vec![0; file_value_len];
      reader.read_exact(&mut file_value).unwrap();
      assert_eq!(file_value, value.unwrap());
    }

    let mut timestamp_buffer = [0; 16];
    reader.read_exact(&mut timestamp_buffer).unwrap();
    let file_timestamp = u128::from_le_bytes(timestamp_buffer);
    assert_eq!(file_timestamp, timestamp);
  }

  #[test]
  fn test_write_one() {
    let mut rng = rand::thread_rng();
    let dir = format!("./{}/", rng.gen::<u32>());
    create_dir(&dir).unwrap();

    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();

    let mut wal = WAL::new(dir.as_str()).unwrap();
    wal.set(b"Lime", b"Lime Smoothie", timestamp).unwrap();
    wal.flush().unwrap();

    let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
    let mut reader = BufReader::new(file);

    check_entry(
      &mut reader,
      b"Lime",
      Some(b"Lime Smoothie"),
      timestamp,
      false,
    );

    remove_dir_all(&dir).unwrap();
  }

  #[test]
  fn test_write_many() {
    let mut rng = rand::thread_rng();
    let dir = format!("./{}/", rng.gen::<u32>());
    create_dir(&dir).unwrap();

    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();

    let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
      (b"Apple", Some(b"Apple Smoothie")),
      (b"Lime", Some(b"Lime Smoothie")),
      (b"Orange", Some(b"Orange Smoothie")),
    ];

    let mut wal = WAL::new(dir.as_str()).unwrap();

    for e in entries.iter() {
      wal.set(e.0, e.1.unwrap(), timestamp).unwrap();
    }
    wal.flush().unwrap();

    let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
    let mut reader = BufReader::new(file);

    for e in entries.iter() {
      check_entry(&mut reader, e.0, e.1, timestamp, false);
    }

    remove_dir_all(&dir).unwrap();
  }

  #[test]
  fn test_write_delete() {
    let mut rng = rand::thread_rng();
    let dir = format!("./{}/", rng.gen::<u32>());
    create_dir(&dir).unwrap();

    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();

    let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
      (b"Apple", Some(b"Apple Smoothie")),
      (b"Lime", Some(b"Lime Smoothie")),
      (b"Orange", Some(b"Orange Smoothie")),
    ];

    let mut wal = WAL::new(dir.as_str()).unwrap();

    for e in entries.iter() {
      wal.set(e.0, e.1.unwrap(), timestamp).unwrap();
    }
    for e in entries.iter() {
      wal.delete(e.0, timestamp).unwrap();
    }

    wal.flush().unwrap();

    let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
    let mut reader = BufReader::new(file);

    for e in entries.iter() {
      check_entry(&mut reader, e.0, e.1, timestamp, false);
    }
    for e in entries.iter() {
      check_entry(&mut reader, e.0, None, timestamp, true);
    }

    remove_dir_all(&dir).unwrap();
  }

  #[test]
  fn test_read_wal_none() {
    let mut rng = rand::thread_rng();
    let dir = format!("./{}/", rng.gen::<u32>());
    create_dir(&dir).unwrap();

    let (new_wal, new_mem_table) = WAL::load_from_dir(dir.as_str()).unwrap();
    assert_eq!(new_mem_table.len(), 0);

    let m = metadata(new_wal.path).unwrap();
    assert_eq!(m.len(), 0);

    remove_dir_all(&dir).unwrap();
  }

  #[test]
  fn test_read_wal_one() {
    let mut rng = rand::thread_rng();
    let dir = format!("./{}/", rng.gen::<u32>());
    create_dir(&dir).unwrap();

    let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
      (b"Apple", Some(b"Apple Smoothie")),
      (b"Lime", Some(b"Lime Smoothie")),
      (b"Orange", Some(b"Orange Smoothie")),
    ];

    let mut wal = WAL::new(dir.as_str()).unwrap();

    for (i, e) in entries.iter().enumerate() {
      wal.set(e.0, e.1.unwrap(), i as u128).unwrap();
    }
    wal.flush().unwrap();

    let (new_wal, new_mem_table) = WAL::load_from_dir(dir.as_str()).unwrap();

    let file = OpenOptions::new().read(true).open(&new_wal.path).unwrap();
    let mut reader = BufReader::new(file);

    for (i, e) in entries.iter().enumerate() {
      check_entry(&mut reader, e.0, e.1, i as u128, false);

      let mem_e = new_mem_table.get(e.0).unwrap();
      assert_eq!(mem_e.key, e.0);
      assert_eq!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
      assert_eq!(mem_e.timestamp, i as u128);
    }

    remove_dir_all(&dir).unwrap();
  }

  #[test]
  fn test_read_wal_multiple() {
    let mut rng = rand::thread_rng();
    let dir = format!("./{}/", rng.gen::<u32>());
    create_dir(&dir).unwrap();

    let entries_1: Vec<(&[u8], Option<&[u8]>)> = vec![
      (b"Apple", Some(b"Apple Smoothie")),
      (b"Lime", Some(b"Lime Smoothie")),
      (b"Orange", Some(b"Orange Smoothie")),
    ];
    let mut wal_1 = WAL::new(dir.as_str()).unwrap();
    for (i, e) in entries_1.iter().enumerate() {
      wal_1.set(e.0, e.1.unwrap(), i as u128).unwrap();
    }
    wal_1.flush().unwrap();

    let entries_2: Vec<(&[u8], Option<&[u8]>)> = vec![
      (b"Strawberry", Some(b"Strawberry Smoothie")),
      (b"Blueberry", Some(b"Blueberry Smoothie")),
      (b"Orange", Some(b"Orange Milkshake")),
    ];
    let mut wal_2 = WAL::new(dir.as_str()).unwrap();
    for (i, e) in entries_2.iter().enumerate() {
      wal_2.set(e.0, e.1.unwrap(), (i + 3) as u128).unwrap();
    }
    wal_2.flush().unwrap();

    let (new_wal, new_mem_table) = WAL::load_from_dir(dir.as_str()).unwrap();

    let file = OpenOptions::new().read(true).open(&new_wal.path).unwrap();
    let mut reader = BufReader::new(file);

    for (i, e) in entries_1.iter().enumerate() {
      check_entry(&mut reader, e.0, e.1, i as u128, false);

      let mem_e = new_mem_table.get(e.0).unwrap();
      if i != 2 {
        assert_eq!(mem_e.key, e.0);
        assert_eq!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
        assert_eq!(mem_e.timestamp, i as u128);
      } else {
        assert_eq!(mem_e.key, e.0);
        assert_ne!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
        assert_ne!(mem_e.timestamp, i as u128);
      }
    }
    for (i, e) in entries_2.iter().enumerate() {
      check_entry(&mut reader, e.0, e.1, (i + 3) as u128, false);

      let mem_e = new_mem_table.get(e.0).unwrap();
      assert_eq!(mem_e.key, e.0);
      assert_eq!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
      assert_eq!(mem_e.timestamp, (i + 3) as u128);
    }

    remove_dir_all(&dir).unwrap();
  }
}
