use crate::mem_table::{MemEntry, MemTable};
use crate::wal::WALError::*;
use std::fs::{metadata, read_dir, remove_file, File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub enum WALError {
  FileNotFound,
  AppendEntryError,
  CorruptRecord,
}

pub struct WAL {
  path: PathBuf,
  file: BufWriter<File>,
}

impl WAL {
  pub fn new(dir: &str) -> WAL {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();

    let path = Path::new(dir).join(timestamp.to_string() + ".wal");
    let file = OpenOptions::new()
      .append(true)
      .create(true)
      .open(&path)
      .unwrap();
    let file = BufWriter::new(file);

    return WAL {
      path: path,
      file: file,
    };
  }

  pub fn load_wal(dir: &str) -> Result<(WAL, MemTable), WALError> {
    let mut wal_files = Vec::new();
    for file in read_dir(Path::new(dir)).unwrap() {
      let path = file.unwrap().path();
      if path.to_str().unwrap().ends_with(".wal") {
        wal_files.push(path);
      }
    }
    wal_files.sort();

    let mut new_mem_table = MemTable::new();
    let mut new_wal = WAL::new(dir);

    for w_f in wal_files.iter() {
      if let Ok(file) = OpenOptions::new().read(true).open(&w_f) {
        let mut reader = BufReader::new(file);

        let m = metadata(w_f).unwrap();
        let mut pos = 0;
        loop {
          if pos as u64 == m.len() {
            break;
          }

          let (entry, len) = WAL::read_mem_table_entry(&mut reader).unwrap();
          new_mem_table.set(
            entry.key.as_slice(),
            entry.value.as_slice(),
            entry.timestamp,
          );
          new_wal
            .append(
              entry.key.as_slice(),
              entry.value.as_slice(),
              entry.timestamp,
            )
            .unwrap();

          pos += len;
        }
      }
    }
    new_wal.flush().unwrap();
    wal_files.into_iter().for_each(|f| remove_file(f).unwrap());
    return Ok((new_wal, new_mem_table));
  }

  pub fn read_mem_table_entry(reader: &mut BufReader<File>) -> Result<(MemEntry, usize), WALError> {
    let mut len_buffer = [0; 8];
    if let Err(_) = reader.read_exact(&mut len_buffer) {
      return Err(CorruptRecord);
    }
    let key_len = usize::from_le_bytes(len_buffer);

    if let Err(_) = reader.read_exact(&mut len_buffer) {
      return Err(CorruptRecord);
    }
    let value_len = usize::from_le_bytes(len_buffer);

    let mut key = vec![0; key_len];
    if let Err(_) = reader.read_exact(&mut key) {
      return Err(CorruptRecord);
    }

    let mut value = vec![0; value_len];
    if let Err(_) = reader.read_exact(&mut value) {
      return Err(CorruptRecord);
    }

    let mut timestamp_buffer = [0; 16];
    if let Err(_) = reader.read_exact(&mut timestamp_buffer) {
      return Err(CorruptRecord);
    }
    let timestamp = u128::from_le_bytes(timestamp_buffer);

    return Ok((
      MemEntry {
        key: key,
        value: value,
        timestamp: timestamp,
      },
      key_len + value_len + 16 + 16,
    ));
  }

  pub fn append(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> Result<(), WALError> {
    if let Err(_) = self.file.write(&key.len().to_le_bytes()) {
      return Err(AppendEntryError);
    }
    if let Err(_) = self.file.write(&value.len().to_le_bytes()) {
      return Err(AppendEntryError);
    }
    if let Err(_) = self.file.write(key) {
      return Err(AppendEntryError);
    }
    if let Err(_) = self.file.write(value) {
      return Err(AppendEntryError);
    }
    if let Err(_) = self.file.write(&timestamp.to_le_bytes()) {
      return Err(AppendEntryError);
    }

    return Ok(());
  }

  pub fn flush(&mut self) -> Result<(), WALError> {
    if let Err(_) = self.file.flush() {
      return Err(AppendEntryError);
    }
    return Ok(());
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

  fn check_entry(reader: &mut BufReader<File>, key: &[u8], value: &[u8], timestamp: u128) {
    let mut len_buffer = [0; 8];
    reader.read_exact(&mut len_buffer).unwrap();
    let file_key_len = usize::from_le_bytes(len_buffer);
    assert_eq!(file_key_len, key.len());

    reader.read_exact(&mut len_buffer).unwrap();
    let file_value_len = usize::from_le_bytes(len_buffer);
    assert_eq!(file_value_len, value.len());

    let mut file_key = vec![0; file_key_len];
    reader.read_exact(&mut file_key).unwrap();
    assert_eq!(file_key, key);

    let mut file_value = vec![0; file_value_len];
    reader.read_exact(&mut file_value).unwrap();
    assert_eq!(file_value, value);

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

    let mut wal = WAL::new(dir.as_str());
    wal.append(b"Lime", b"Lime Smoothie", timestamp).unwrap();
    wal.flush().unwrap();

    let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
    let mut reader = BufReader::new(file);

    check_entry(&mut reader, b"Lime", b"Lime Smoothie", timestamp);

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

    let entries: Vec<(&[u8], &[u8])> = vec![
      (b"Apple", b"Apple Smoothie"),
      (b"Lime", b"Lime Smoothie"),
      (b"Orange", b"Orange Smoothie"),
    ];

    let mut wal = WAL::new(dir.as_str());

    for e in entries.iter() {
      wal.append(e.0, e.1, timestamp).unwrap();
    }
    wal.flush().unwrap();

    let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
    let mut reader = BufReader::new(file);

    for e in entries.iter() {
      check_entry(&mut reader, e.0, e.1, timestamp);
    }

    remove_dir_all(&dir).unwrap();
  }

  #[test]
  fn test_read_wal_none() {
    let mut rng = rand::thread_rng();
    let dir = format!("./{}/", rng.gen::<u32>());
    create_dir(&dir).unwrap();

    let (new_wal, new_mem_table) = WAL::load_wal(dir.as_str()).unwrap();
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

    let entries: Vec<(&[u8], &[u8])> = vec![
      (b"Apple", b"Apple Smoothie"),
      (b"Lime", b"Lime Smoothie"),
      (b"Orange", b"Orange Smoothie"),
    ];

    let mut wal = WAL::new(dir.as_str());

    for (i, e) in entries.iter().enumerate() {
      wal.append(e.0, e.1, i as u128).unwrap();
    }
    wal.flush().unwrap();

    let (new_wal, new_mem_table) = WAL::load_wal(dir.as_str()).unwrap();

    let file = OpenOptions::new().read(true).open(&new_wal.path).unwrap();
    let mut reader = BufReader::new(file);

    for (i, e) in entries.iter().enumerate() {
      check_entry(&mut reader, e.0, e.1, i as u128);

      let mem_e = new_mem_table.get(e.0).unwrap();
      assert_eq!(mem_e.key, e.0);
      assert_eq!(mem_e.value, e.1);
      assert_eq!(mem_e.timestamp, i as u128);
    }

    remove_dir_all(&dir).unwrap();
  }

  #[test]
  fn test_read_wal_multiple() {
    let mut rng = rand::thread_rng();
    let dir = format!("./{}/", rng.gen::<u32>());
    create_dir(&dir).unwrap();

    let entries_1: Vec<(&[u8], &[u8])> = vec![
      (b"Apple", b"Apple Smoothie"),
      (b"Lime", b"Lime Smoothie"),
      (b"Orange", b"Orange Smoothie"),
    ];
    let mut wal_1 = WAL::new(dir.as_str());
    for (i, e) in entries_1.iter().enumerate() {
      wal_1.append(e.0, e.1, i as u128).unwrap();
    }
    wal_1.flush().unwrap();

    let entries_2: Vec<(&[u8], &[u8])> = vec![
      (b"Strawberry", b"Strawberry Smoothie"),
      (b"Blueberry", b"Blueberry Smoothie"),
      (b"Orange", b"Orange Milkshake"),
    ];
    let mut wal_2 = WAL::new(dir.as_str());
    for (i, e) in entries_2.iter().enumerate() {
      wal_2.append(e.0, e.1, (i + 3) as u128).unwrap();
    }
    wal_2.flush().unwrap();

    let (new_wal, new_mem_table) = WAL::load_wal(dir.as_str()).unwrap();

    let file = OpenOptions::new().read(true).open(&new_wal.path).unwrap();
    let mut reader = BufReader::new(file);

    for (i, e) in entries_1.iter().enumerate() {
      check_entry(&mut reader, e.0, e.1, i as u128);

      let mem_e = new_mem_table.get(e.0).unwrap();
      if i != 2 {
        assert_eq!(mem_e.key, e.0);
        assert_eq!(mem_e.value, e.1);
        assert_eq!(mem_e.timestamp, i as u128);
      } else {
        assert_eq!(mem_e.key, e.0);
        assert_ne!(mem_e.value, e.1);
        assert_ne!(mem_e.timestamp, i as u128);
      }
    }
    for (i, e) in entries_2.iter().enumerate() {
      check_entry(&mut reader, e.0, e.1, (i + 3) as u128);

      let mem_e = new_mem_table.get(e.0).unwrap();
      assert_eq!(mem_e.key, e.0);
      assert_eq!(mem_e.value, e.1);
      assert_eq!(mem_e.timestamp, (i + 3) as u128);
    }

    remove_dir_all(&dir).unwrap();
  }
}
