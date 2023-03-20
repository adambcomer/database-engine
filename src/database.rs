use crate::mem_table::MemTable;
use crate::wal::WAL;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct DatabaseEntry {
  key: Vec<u8>,
  value: Vec<u8>,
  timestamp: u128,
}

impl DatabaseEntry {
  pub fn key(&self) -> &[u8] {
    &self.key
  }

  pub fn value(&self) -> &[u8] {
    &self.value
  }

  pub fn timestamp(&self) -> u128 {
    self.timestamp
  }
}

pub struct Database {
  dir: PathBuf,
  mem_table: MemTable,
  wal: WAL,
}

impl Database {
  pub fn new(dir: &str) -> Database {
    let dir = PathBuf::from(dir);

    let (wal, mem_table) = WAL::load_from_dir(&dir).unwrap();

    Database {
      dir: dir,
      mem_table,
      wal,
    }
  }

  pub fn get(&self, key: &[u8]) -> Option<DatabaseEntry> {
    if let Some(mem_entry) = self.mem_table.get(key) {
      return Some(DatabaseEntry {
        key: mem_entry.key.clone(),
        value: mem_entry.value.as_ref().unwrap().clone(),
        timestamp: mem_entry.timestamp,
      });
    }

    None
  }

  pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<usize, usize> {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();

    let wal_res = self.wal.set(key, value, timestamp);
    if wal_res.is_err() {
      return Err(0);
    }
    if self.wal.flush().is_err() {
      return Err(0);
    }

    self.mem_table.set(key, value, timestamp);

    Ok(1)
  }

  pub fn delete(&mut self, key: &[u8]) -> Result<usize, usize> {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();

    let wal_res = self.wal.delete(key, timestamp);
    if wal_res.is_err() {
      return Err(0);
    }
    if self.wal.flush().is_err() {
      return Err(0);
    }

    self.mem_table.delete(key, timestamp);

    Ok(1)
  }
}
