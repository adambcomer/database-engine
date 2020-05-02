use crate::mem_table::MemTable;

pub struct DatabaseEntry {
  key: Vec<u8>,
  value: Vec<u8>,
  timestamp: u128,
}

pub struct Database {
  path: String,
  mem_table: MemTable,
}

impl Database {
  pub fn new(path: &str) -> Database {
    return Database {
      path: path.to_string(),
      mem_table: MemTable::new(),
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
    let bytes_stored = self.mem_table.put(key, value);

    return Ok(bytes_stored);
  }
}


