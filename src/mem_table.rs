use std::time::{SystemTime, UNIX_EPOCH};

pub struct MemEntry {
  pub key: Vec<u8>,
  pub value: Vec<u8>,
  pub timestamp: u128,
}

pub struct MemTable {
  entries: Vec<MemEntry>,
}

fn find_next_index(v: &Vec<MemEntry>, key: &[u8]) -> usize {
  if v.len() == 0 {
    return 0;
  }

  let mut a = 0;
  let mut b = v.len() - 1;

  while a <= b {
    let m = (a + b) / 2;
    if v[m].key.as_slice() < key {
      a = m + 1;
    } else {
      if m == 0 {
        break;
      }
      b = m - 1;
    }
  }

  return a;
}

impl MemTable {
  pub fn new() -> MemTable {
    return MemTable {
      entries: Vec::new(),
    };
  }

  pub fn put(&mut self, key: &[u8], value: &[u8]) -> usize {
    let now = SystemTime::now();
    let entry = MemEntry {
      key: key.to_owned(),
      value: value.to_owned(),
      timestamp: now.duration_since(UNIX_EPOCH).unwrap().as_micros(),
    };
    if let Ok(pos) = self
      .entries
      .binary_search_by_key(&key, |e| e.key.as_slice())
    {
      self.entries[pos] = entry;
      return key.len() + value.len() + 16; // Returns number of bytes written plus 16 for the timestamp.
    }

    let idx = find_next_index(&self.entries, key);
    if idx == self.entries.len() {
      self.entries.push(entry);
    } else {
      self.entries.insert(idx, entry)
    }

    return key.len() + value.len() + 16; // Returns number of bytes written plus 16 for the timestamp.
  }

  pub fn get(&self, key: &[u8]) -> Option<&MemEntry> {
    if let Ok(pos) = self
      .entries
      .binary_search_by_key(&key, |e| e.key.as_slice())
    {
      return Some(&self.entries[pos]);
    }
    return None;
  }
}

#[cfg(test)]
mod tests {
  use crate::mem_table::find_next_index;
  use crate::mem_table::{MemEntry, MemTable};
  use std::time::{SystemTime, UNIX_EPOCH};

  #[test]
  fn test_find_index_empty() {
    let v: Vec<MemEntry> = Vec::new();
    let key = "Lime".as_bytes();

    assert_eq!(find_next_index(&v, key), 0)
  }

  #[test]
  fn test_find_index_start() {
    let v: Vec<MemEntry> = vec![
      MemEntry {
        key: "Lime".as_bytes().to_owned(),
        value: "Lime Smoothie".as_bytes().to_owned(),
        timestamp: 0,
      },
      MemEntry {
        key: "Orange".as_bytes().to_owned(),
        value: "Orange Smoothie".as_bytes().to_owned(),
        timestamp: 0,
      },
    ];
    let key = "Apple".as_bytes();

    assert_eq!(find_next_index(&v, key), 0);
  }

  #[test]
  fn test_find_index_middle() {
    let v: Vec<MemEntry> = vec![
      MemEntry {
        key: "Apple".as_bytes().to_owned(),
        value: "Apple Smoothie".as_bytes().to_owned(),
        timestamp: 0,
      },
      MemEntry {
        key: "Orange".as_bytes().to_owned(),
        value: "Orange Smoothie".as_bytes().to_owned(),
        timestamp: 0,
      },
    ];
    let key = "Lime".as_bytes();

    assert_eq!(find_next_index(&v, key), 1);
  }

  #[test]
  fn test_find_index_end() {
    let v: Vec<MemEntry> = vec![
      MemEntry {
        key: "Apple".as_bytes().to_owned(),
        value: "Apple Smoothie".as_bytes().to_owned(),
        timestamp: 0,
      },
      MemEntry {
        key: "Lime".as_bytes().to_owned(),
        value: "Lime Smoothie".as_bytes().to_owned(),
        timestamp: 0,
      },
    ];
    let key = "Orange".as_bytes();

    assert_eq!(find_next_index(&v, key), 2);
  }

  #[test]
  fn test_mem_table_put_start() {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();
    let mut table = MemTable::new();
    table.put("Lime".as_bytes(), "Lime Smoothie".as_bytes());
    table.put("Orange".as_bytes(), "Orange Smoothie".as_bytes());

    table.put("Apple".as_bytes(), "Apple Smoothie".as_bytes());

    assert_eq!(table.entries[0].key, "Apple".as_bytes());
    assert_eq!(table.entries[0].value, "Apple Smoothie".as_bytes());
    assert_eq!(table.entries[0].timestamp >= timestamp, true);
    assert_eq!(table.entries[0].timestamp <= timestamp + 1000, true);
    assert_eq!(table.entries[1].key, "Lime".as_bytes());
    assert_eq!(table.entries[1].value, "Lime Smoothie".as_bytes());
    assert_eq!(table.entries[1].timestamp >= timestamp, true);
    assert_eq!(table.entries[1].timestamp <= timestamp + 1000, true);
    assert_eq!(table.entries[2].key, "Orange".as_bytes());
    assert_eq!(table.entries[2].value, "Orange Smoothie".as_bytes());
    assert_eq!(table.entries[2].timestamp >= timestamp, true);
    assert_eq!(table.entries[2].timestamp <= timestamp + 1000, true);
  }

  #[test]
  fn test_mem_table_put_middle() {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();
    let mut table = MemTable::new();
    table.put("Apple".as_bytes(), "Apple Smoothie".as_bytes());
    table.put("Orange".as_bytes(), "Orange Smoothie".as_bytes());

    table.put("Lime".as_bytes(), "Lime Smoothie".as_bytes());

    assert_eq!(table.entries[0].key, "Apple".as_bytes());
    assert_eq!(table.entries[0].value, "Apple Smoothie".as_bytes());
    assert_eq!(table.entries[0].timestamp >= timestamp, true);
    assert_eq!(table.entries[0].timestamp <= timestamp + 1000, true);
    assert_eq!(table.entries[1].key, "Lime".as_bytes());
    assert_eq!(table.entries[1].value, "Lime Smoothie".as_bytes());
    assert_eq!(table.entries[1].timestamp >= timestamp, true);
    assert_eq!(table.entries[1].timestamp <= timestamp + 1000, true);
    assert_eq!(table.entries[2].key, "Orange".as_bytes());
    assert_eq!(table.entries[2].value, "Orange Smoothie".as_bytes());
    assert_eq!(table.entries[2].timestamp >= timestamp, true);
    assert_eq!(table.entries[2].timestamp <= timestamp + 1000, true);
  }

  #[test]
  fn test_mem_table_put_end() {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();
    let mut table = MemTable::new();
    table.put("Apple".as_bytes(), "Apple Smoothie".as_bytes());
    table.put("Lime".as_bytes(), "Lime Smoothie".as_bytes());

    table.put("Orange".as_bytes(), "Orange Smoothie".as_bytes());

    assert_eq!(table.entries[0].key, "Apple".as_bytes());
    assert_eq!(table.entries[0].value, "Apple Smoothie".as_bytes());
    assert_eq!(table.entries[0].timestamp >= timestamp, true);
    assert_eq!(table.entries[0].timestamp <= timestamp + 1000, true);
    assert_eq!(table.entries[1].key, "Lime".as_bytes());
    assert_eq!(table.entries[1].value, "Lime Smoothie".as_bytes());
    assert_eq!(table.entries[1].timestamp >= timestamp, true);
    assert_eq!(table.entries[1].timestamp <= timestamp + 1000, true);
    assert_eq!(table.entries[2].key, "Orange".as_bytes());
    assert_eq!(table.entries[2].value, "Orange Smoothie".as_bytes());
    assert_eq!(table.entries[2].timestamp >= timestamp, true);
    assert_eq!(table.entries[2].timestamp <= timestamp + 1000, true);
  }

  #[test]
  fn test_mem_table_put_overwrite() {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();
    let mut table = MemTable::new();
    table.put("Apple".as_bytes(), "Apple Smoothie".as_bytes());
    table.put("Lime".as_bytes(), "Lime Smoothie".as_bytes());
    table.put("Orange".as_bytes(), "Orange Smoothie".as_bytes());

    table.put("Lime".as_bytes(), "A sour fruit".as_bytes());

    assert_eq!(table.entries[0].key, "Apple".as_bytes());
    assert_eq!(table.entries[0].value, "Apple Smoothie".as_bytes());
    assert_eq!(table.entries[0].timestamp >= timestamp, true);
    assert_eq!(table.entries[0].timestamp <= timestamp + 1000, true);
    assert_eq!(table.entries[1].key, "Lime".as_bytes());
    assert_eq!(table.entries[1].value, "A sour fruit".as_bytes());
    assert_eq!(table.entries[1].timestamp >= timestamp, true);
    assert_eq!(table.entries[1].timestamp <= timestamp + 1000, true);
    assert_eq!(table.entries[2].key, "Orange".as_bytes());
    assert_eq!(table.entries[2].value, "Orange Smoothie".as_bytes());
    assert_eq!(table.entries[2].timestamp >= timestamp, true);
    assert_eq!(table.entries[2].timestamp <= timestamp + 1000, true);
  }

  #[test]
  fn test_mem_table_get_exists() {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();
    let mut table = MemTable::new();
    table.put("Apple".as_bytes(), "Apple Smoothie".as_bytes());
    table.put("Lime".as_bytes(), "Lime Smoothie".as_bytes());
    table.put("Orange".as_bytes(), "Orange Smoothie".as_bytes());

    let res = table.get("Orange".as_bytes());
    assert_eq!(res.is_some(), true);

    let entry = res.unwrap();
    assert_eq!(entry.key, "Orange".as_bytes());
    assert_eq!(entry.value, "Orange Smoothie".as_bytes());
    assert_eq!(entry.timestamp >= timestamp, true);
    assert_eq!(entry.timestamp <= timestamp + 1000, true);
  }

  #[test]
  fn test_mem_table_get_not_exists() {
    let mut table = MemTable::new();
    table.put("Apple".as_bytes(), "Apple Smoothie".as_bytes());
    table.put("Lime".as_bytes(), "Lime Smoothie".as_bytes());
    table.put("Orange".as_bytes(), "Orange Smoothie".as_bytes());

    let res = table.get("Potato".as_bytes());
    assert_eq!(res.is_some(), false);
  }
}
