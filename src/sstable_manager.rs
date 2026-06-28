use std::{
    fs::read_dir,
    io::{self, Error},
    path::{Path, PathBuf},
};

use crate::{
    mem_table::MemTable,
    sstable::{SSTable, SSTableEntry},
};

pub struct SSTableManger {
    dir: PathBuf,
    levels: [Vec<SSTable>; 6],
}

impl SSTableManger {
    pub fn new(dir: PathBuf) -> io::Result<SSTableManger> {
        let mut levels = [const { vec![] }; 6];
        for level in 0..6 {
            let mut timestamped_level = Vec::new();

            let path = Path::new(&dir).join(format!("{}", level));
            for entry in read_dir(path)? {
                let entry = entry?;

                if let Some(stem) = entry.path().file_stem() {
                    if let Ok(timestamp) = u128::from_str_radix(&stem.to_string_lossy(), 10) {
                        let table = SSTable::load_from_path(&entry.path())?;

                        timestamped_level.push((timestamp, table));
                    } else {
                        return Err(Error::new(io::ErrorKind::InvalidFilename, ""));
                    }
                } else {
                    return Err(Error::new(io::ErrorKind::InvalidFilename, ""));
                }
            }

            timestamped_level.sort_by(|(a, _), (b, _)| a.cmp(b));

            levels[level] = timestamped_level
                .into_iter()
                .map(|(_, t)| t)
                .collect::<Vec<SSTable>>();
        }

        Ok(SSTableManger { dir, levels })
    }

    pub fn add_table(mut self, memtable: MemTable) -> io::Result<()> {
        let table = SSTable::new(&memtable, 0, &self.dir)?;

        self.levels[0].push(table);

        // TODO: Add compaction

        return Ok(());
    }

    pub fn get(mut self, key: &[u8]) -> io::Result<Option<SSTableEntry>> {
        for level in 0..6 {
            for table in &mut self.levels[level] {
                if let Some(res) = table.get(key)? {
                    return Ok(Some(res));
                }
            }
        }

        return Ok(None);
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{create_dir_all, File};

    use tempfile::tempdir;

    use crate::{mem_table::MemTable, sstable::SSTable, sstable_manager::SSTableManger};

    fn setup_level_dirs(dir: &std::path::Path) {
        for level in 0..6 {
            create_dir_all(dir.join(format!("{}", level))).unwrap();
        }
    }

    #[test]
    fn test_new_empty() {
        let dir = tempdir().unwrap();
        setup_level_dirs(dir.path());

        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();

        assert!(manager.get(b"key").unwrap().is_none());
    }

    #[test]
    fn test_new_missing_level_dir() {
        let dir = tempdir().unwrap();

        let result = SSTableManger::new(dir.path().to_path_buf());
        assert!(result.is_err());
    }

    #[test]
    fn test_new_invalid_filename_in_level_dir() {
        let dir = tempdir().unwrap();
        setup_level_dirs(dir.path());

        // Place a file whose stem is not a valid u128 inside level 0.
        File::create(dir.path().join("0/not_a_number.sstable")).unwrap();

        let result = SSTableManger::new(dir.path().to_path_buf());
        assert!(result.is_err());
    }

    #[test]
    fn test_new_loads_sstables_from_non_zero_level() {
        let dir = tempdir().unwrap();
        setup_level_dirs(dir.path());

        // Write an SSTable directly to level 3 (bypassing add_table which only writes level 0).
        let mut memtable = MemTable::new();
        memtable.set(b"x", b"level3", 0);
        SSTable::new(&memtable, 3, dir.path()).unwrap();

        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();
        let entry = manager.get(b"x").unwrap().unwrap();
        assert_eq!(entry.key, b"x");
        assert_eq!(entry.value.unwrap(), b"level3");
    }

    #[test]
    fn test_new_loads_tables_sorted_by_timestamp_ascending() {
        let dir = tempdir().unwrap();
        setup_level_dirs(dir.path());

        // Write an older table with key "k" → "old".
        let mut memtable = MemTable::new();
        memtable.set(b"k", b"old", 0);
        SSTable::new(&memtable, 0, dir.path()).unwrap();

        // Ensure a distinct microsecond timestamp for the second file.
        std::thread::sleep(std::time::Duration::from_millis(2));

        // Write a newer table with the same key "k" → "new".
        let mut memtable = MemTable::new();
        memtable.set(b"k", b"new", 1);
        SSTable::new(&memtable, 0, dir.path()).unwrap();

        // new() sorts tables ascending by filename timestamp, so the older table is
        // iterated first and its value is returned by get().
        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();
        let entry = manager.get(b"k").unwrap().unwrap();
        assert_eq!(entry.value.unwrap(), b"old");
    }

    #[test]
    fn test_add_table_persists_to_disk() {
        let dir = tempdir().unwrap();
        setup_level_dirs(dir.path());

        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();
        let mut memtable = MemTable::new();
        memtable.set(b"a", b"1", 0);
        memtable.set(b"b", b"2", 1);
        manager.add_table(memtable).unwrap();

        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();
        let entry = manager.get(b"a").unwrap().unwrap();
        assert_eq!(entry.key, b"a");
        assert_eq!(entry.value.unwrap(), b"1");
        assert_eq!(entry.timestamp, 0);
    }

    #[test]
    fn test_get_existing_key() {
        let dir = tempdir().unwrap();
        setup_level_dirs(dir.path());

        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();
        let mut memtable = MemTable::new();
        memtable.set(b"a", b"1", 0);
        memtable.set(b"b", b"2", 1);
        memtable.set(b"c", b"3", 2);
        manager.add_table(memtable).unwrap();

        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();
        let entry = manager.get(b"b").unwrap().unwrap();
        assert_eq!(entry.key, b"b");
        assert_eq!(entry.value.unwrap(), b"2");
        assert_eq!(entry.timestamp, 1);
    }

    #[test]
    fn test_get_not_found() {
        let dir = tempdir().unwrap();
        setup_level_dirs(dir.path());

        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();
        let mut memtable = MemTable::new();
        memtable.set(b"a", b"1", 0);
        memtable.set(b"b", b"2", 1);
        manager.add_table(memtable).unwrap();

        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();
        assert!(manager.get(b"z").unwrap().is_none());
    }

    #[test]
    fn test_get_deleted_key() {
        let dir = tempdir().unwrap();
        setup_level_dirs(dir.path());

        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();
        let mut memtable = MemTable::new();
        memtable.set(b"a", b"1", 0);
        memtable.delete(b"b", 1);
        memtable.set(b"c", b"3", 2);
        manager.add_table(memtable).unwrap();

        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();
        let entry = manager.get(b"b").unwrap().unwrap();
        assert_eq!(entry.key, b"b");
        assert!(entry.value.is_none());
        assert_eq!(entry.timestamp, 1);
    }

    #[test]
    fn test_get_searches_across_multiple_tables() {
        let dir = tempdir().unwrap();
        setup_level_dirs(dir.path());

        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();
        let mut memtable = MemTable::new();
        memtable.set(b"a", b"1", 0);
        memtable.set(b"b", b"2", 1);
        manager.add_table(memtable).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(2));

        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();
        let mut memtable = MemTable::new();
        memtable.set(b"c", b"3", 2);
        memtable.set(b"d", b"4", 3);
        manager.add_table(memtable).unwrap();

        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();

        let entry = manager.get(b"a").unwrap().unwrap();
        assert_eq!(entry.key, b"a");
        assert_eq!(entry.value.unwrap(), b"1");

        let manager = SSTableManger::new(dir.path().to_path_buf()).unwrap();
        let entry = manager.get(b"c").unwrap().unwrap();
        assert_eq!(entry.key, b"c");
        assert_eq!(entry.value.unwrap(), b"3");
    }
}
