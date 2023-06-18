use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, BufReader};
use std::path::PathBuf;

pub struct WALEntry {
  pub key: Vec<u8>,
  pub value: Option<Vec<u8>>,
  pub timestamp: u128,
  pub deleted: bool,
}

/// WAL iterator to iterate over the items in a WAL file.
pub struct WALIterator {
  reader: BufReader<File>,
}

impl WALIterator {
  /// Creates a new WALIterator from a path to a WAL file.
  pub fn new(path: PathBuf) -> io::Result<WALIterator> {
    let file = OpenOptions::new().read(true).open(path)?;
    let reader = BufReader::new(file);
    Ok(WALIterator { reader })
  }
}

impl Iterator for WALIterator {
  type Item = WALEntry;

  /// Gets the next entry in the WAL file.
  fn next(&mut self) -> Option<WALEntry> {
    let mut len_buffer = [0; 8];
    if self.reader.read_exact(&mut len_buffer).is_err() {
      return None;
    }
    let key_len = usize::from_le_bytes(len_buffer);

    let mut bool_buffer = [0; 1];
    if self.reader.read_exact(&mut bool_buffer).is_err() {
      return None;
    }
    let deleted = bool_buffer[0] != 0;

    let mut key = vec![0; key_len];
    let mut value = None;
    if deleted {
      if self.reader.read_exact(&mut key).is_err() {
        return None;
      }
    } else {
      if self.reader.read_exact(&mut len_buffer).is_err() {
        return None;
      }
      let value_len = usize::from_le_bytes(len_buffer);
      if self.reader.read_exact(&mut key).is_err() {
        return None;
      }
      let mut value_buf = vec![0; value_len];
      if self.reader.read_exact(&mut value_buf).is_err() {
        return None;
      }
      value = Some(value_buf);
    }

    let mut timestamp_buffer = [0; 16];
    if self.reader.read_exact(&mut timestamp_buffer).is_err() {
      return None;
    }
    let timestamp = u128::from_le_bytes(timestamp_buffer);

    Some(WALEntry {
      key,
      value,
      timestamp,
      deleted,
    })
  }
}
