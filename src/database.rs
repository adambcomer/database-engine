use crate::mem_table::MemTable;
use crate::wal::WAL;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct DatabaseEntry {
  key: Vec<u8>,
  value: Vec<u8>,
  timestamp: u128,
}

impl DatabaseEntry {
  pub fn key(&self) -> &[u8] {
    return self.key.as_slice();
  }

  pub fn value(&self) -> &[u8] {
    return self.value.as_slice();
  }

  pub fn timestamp(&self) -> u128 {
    return self.timestamp;
  }
}

pub struct Database {
  dir: String,
  mem_table: MemTable,
  wal: WAL,
}

impl Database {
  pub fn new(dir: &str) -> Database {
    let (wal, mem_table) = WAL::load_wal(dir).unwrap();

    return Database {
      dir: dir.to_string(),
      mem_table: mem_table,
      wal: wal,
    };
  }

  pub fn get(&self, key: &[u8]) -> Option<DatabaseEntry> {
    if let Some(mem_entry) = self.mem_table.get(key) {
      return Some(DatabaseEntry {
        key: mem_entry.key.clone(),
        value: mem_entry.value.clone(),
        timestamp: mem_entry.timestamp,
      });
    }

    return None;
  }

  pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<usize, usize> {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();

    let wal_res = self.wal.append(key, value, timestamp);
    if wal_res.is_err() {
      return Err(0);
    }
    if let Err(_) = self.wal.flush() {
      return Err(0);
    }

    let bytes_stored = self.mem_table.put(key, value, timestamp);

    return Ok(bytes_stored);
  }
}
