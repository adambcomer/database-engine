/// MemTable entry.
#[derive(Debug)]
pub struct MemEntry {
  pub key: Vec<u8>,
  pub value: Option<Vec<u8>>,
  pub timestamp: u128,
  pub deleted: bool,
}

/// MemTable holds a sorted list of the lasts written records.
///
/// Writes are duplicated to the WAL for recovery of the MemTable in the event of a restart.
///
/// MemTables have a max capacity and when that is reached, we flush the MemTable
/// to disk as a Table(SSTable).
///
/// Entries are stored in a Vector over a HashMap to support Scans.
pub struct MemTable {
  entries: Vec<MemEntry>,
}

impl MemTable {
  /// Creates a new empty MemTable
  pub fn new() -> MemTable {
    return MemTable {
      entries: Vec::new(),
    };
  }

  /// Sets a Key-Value pair in the MemTable.
  pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
    let entry = MemEntry {
      key: key.to_owned(),
      value: Some(value.to_owned()),
      timestamp: timestamp,
      deleted: false,
    };

    match self.get_index(key) {
      Ok(idx) => {
        self.entries[idx] = entry;
      }
      Err(idx) => self.entries.insert(idx, entry),
    }
  }

  /// Deletes a Key-Value pair in the MemTable.
  /// 
  /// This is achieved using tombstones.
  pub fn delete(&mut self, key: &[u8], timestamp: u128) {
    let entry = MemEntry {
      key: key.to_owned(),
      value: None,
      timestamp: timestamp,
      deleted: true,
    };
    match self.get_index(key) {
      Ok(idx) => {
        self.entries[idx] = entry;
      }
      Err(idx) => self.entries.insert(idx, entry),
    }
  }

  /// Gets a Key-Value pair from the MemTable.alloc
  /// 
  /// If no record with the same key exists in the MemTable, return None.
  pub fn get(&self, key: &[u8]) -> Option<&MemEntry> {
    if let Ok(idx) = self.get_index(key) {
      if self.entries[idx].deleted {
        return None;
      }
      return Some(&self.entries[idx]);
    }
    return None;
  }

  /// Performs Binary Search to find a record in the MemTable.
  ///
  /// If the record is found `[Result::Ok]` is returned, with the index of record. If the record is not
  /// found then `[Result::Err]` is returned, with the index to insert the record at.
  fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
    return self
      .entries
      .binary_search_by_key(&key, |e| e.key.as_slice());
  }

  /// Gets the number of records in the MemTable.
  pub fn len(&self) -> usize {
    return self.entries.len();
  }

  /// Gets all of the Records from the MemTable.
  pub fn entries(&self) -> &Vec<MemEntry> {
    return &self.entries;
  }
}

#[cfg(test)]
mod tests {
  use crate::mem_table::{MemTable};
  use std::time::{SystemTime, UNIX_EPOCH};

  #[test]
  fn test_mem_table_put_start() {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();
    let mut table = MemTable::new();
    table.set(b"Lime", b"Lime Smoothie", timestamp);
    table.set(b"Orange", b"Orange Smoothie", timestamp + 10);

    table.set(b"Apple", b"Apple Smoothie", timestamp + 20);

    assert_eq!(table.entries[0].key, b"Apple");
    assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
    assert_eq!(table.entries[0].timestamp, timestamp + 20);
    assert_eq!(table.entries[0].deleted, false);
    assert_eq!(table.entries[1].key, b"Lime");
    assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
    assert_eq!(table.entries[1].timestamp, timestamp);
    assert_eq!(table.entries[1].deleted, false);
    assert_eq!(table.entries[2].key, b"Orange");
    assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
    assert_eq!(table.entries[2].timestamp, timestamp + 10);
    assert_eq!(table.entries[2].deleted, false);
  }

  #[test]
  fn test_mem_table_put_middle() {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();
    let mut table = MemTable::new();
    table.set(b"Apple", b"Apple Smoothie", timestamp);
    table.set(b"Orange", b"Orange Smoothie", timestamp + 10);

    table.set(b"Lime", b"Lime Smoothie", timestamp + 20);

    assert_eq!(table.entries[0].key, b"Apple");
    assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
    assert_eq!(table.entries[0].timestamp, timestamp);
    assert_eq!(table.entries[0].deleted, false);
    assert_eq!(table.entries[1].key, b"Lime");
    assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
    assert_eq!(table.entries[1].timestamp, timestamp + 20);
    assert_eq!(table.entries[1].deleted, false);
    assert_eq!(table.entries[2].key, b"Orange");
    assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
    assert_eq!(table.entries[2].timestamp, timestamp + 10);
    assert_eq!(table.entries[2].deleted, false);
  }

  #[test]
  fn test_mem_table_put_end() {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();
    let mut table = MemTable::new();
    table.set(b"Apple", b"Apple Smoothie", timestamp);
    table.set(b"Lime", b"Lime Smoothie", timestamp + 10);

    table.set(b"Orange", b"Orange Smoothie", timestamp + 20);

    assert_eq!(table.entries[0].key, b"Apple");
    assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
    assert_eq!(table.entries[0].timestamp, timestamp);
    assert_eq!(table.entries[0].deleted, false);
    assert_eq!(table.entries[1].key, b"Lime");
    assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
    assert_eq!(table.entries[1].timestamp, timestamp + 10);
    assert_eq!(table.entries[1].deleted, false);
    assert_eq!(table.entries[2].key, b"Orange");
    assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
    assert_eq!(table.entries[2].timestamp, timestamp + 20);
    assert_eq!(table.entries[2].deleted, false);
  }

  #[test]
  fn test_mem_table_put_overwrite() {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();
    let mut table = MemTable::new();
    table.set(b"Apple", b"Apple Smoothie", timestamp);
    table.set(b"Lime", b"Lime Smoothie", timestamp + 10);
    table.set(b"Orange", b"Orange Smoothie", timestamp + 20);

    table.set(b"Lime", b"A sour fruit", timestamp + 30);

    assert_eq!(table.entries[0].key, b"Apple");
    assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
    assert_eq!(table.entries[0].timestamp, timestamp);
    assert_eq!(table.entries[0].deleted, false);
    assert_eq!(table.entries[1].key, b"Lime");
    assert_eq!(table.entries[1].value.as_ref().unwrap(), b"A sour fruit");
    assert_eq!(table.entries[1].timestamp, timestamp + 30);
    assert_eq!(table.entries[1].deleted, false);
    assert_eq!(table.entries[2].key, b"Orange");
    assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
    assert_eq!(table.entries[2].timestamp, timestamp + 20);
    assert_eq!(table.entries[2].deleted, false);
  }

  #[test]
  fn test_mem_table_get_exists() {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();
    let mut table = MemTable::new();
    table.set(b"Apple", b"Apple Smoothie", timestamp);
    table.set(b"Lime", b"Lime Smoothie", timestamp + 10);
    table.set(b"Orange", b"Orange Smoothie", timestamp + 20);

    let entry = table.get(b"Orange").unwrap();

    assert_eq!(entry.key, b"Orange");
    assert_eq!(entry.value.as_ref().unwrap(), b"Orange Smoothie");
    assert_eq!(entry.timestamp, timestamp + 20);
  }

  #[test]
  fn test_mem_table_get_not_exists() {
    let mut table = MemTable::new();
    table.set(b"Apple", b"Apple Smoothie", 0);
    table.set(b"Lime", b"Lime Smoothie", 0);
    table.set(b"Orange", b"Orange Smoothie", 0);

    let res = table.get(b"Potato");
    assert_eq!(res.is_some(), false);
  }

  #[test]
  fn test_mem_table_delete_exists() {
    let mut table = MemTable::new();
    table.set(b"Apple", b"Apple Smoothie", 0);

    table.delete(b"Apple", 10);

    let res = table.get(b"Apple");
    assert_eq!(res.is_some(), false);

    assert_eq!(table.entries[0].key, b"Apple");
    assert_eq!(table.entries[0].value, None);
    assert_eq!(table.entries[0].timestamp, 10);
    assert_eq!(table.entries[0].deleted, true);
  }

  #[test]
  fn test_mem_table_delete_empty() {
    let mut table = MemTable::new();

    table.delete(b"Apple", 10);

    let res = table.get(b"Apple");
    assert_eq!(res.is_some(), false);

    assert_eq!(table.entries[0].key, b"Apple");
    assert_eq!(table.entries[0].value, None);
    assert_eq!(table.entries[0].timestamp, 10);
    assert_eq!(table.entries[0].deleted, true);
  }
}
