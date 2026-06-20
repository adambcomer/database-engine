use crate::sstable::SSTableEntry;
use std::io::prelude::*;
use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader},
    path::PathBuf,
};

/// SSTableIterator to iterate over the items in a SSTable file.
pub struct SSTableIterator {
    reader: BufReader<File>,
}

impl SSTableIterator {
    /// Creates a new SSTableIterator from a path to a SSTable file.
    pub fn new(path: PathBuf) -> io::Result<SSTableIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(SSTableIterator { reader })
    }
}

impl Iterator for SSTableIterator {
    type Item = SSTableEntry;

    /// Gets the next entry in the SSTable file.
    fn next(&mut self) -> Option<Self::Item> {
        let mut len_buf = [0u8; size_of::<usize>()];
        if self.reader.read_exact(&mut len_buf).is_err() {
            return None;
        }
        let key_len = usize::from_le_bytes(len_buf);

        let mut key = vec![0u8; key_len];
        if self.reader.read_exact(&mut key).is_err() {
            return None;
        }

        let mut deleted_buf = [0u8; size_of::<u8>()];
        if self.reader.read_exact(&mut deleted_buf).is_err() {
            return None;
        }
        let deleted = deleted_buf[0] == 1;

        let mut value = None;
        if !deleted {
            let mut len_buf = [0u8; size_of::<usize>()];
            if self.reader.read_exact(&mut len_buf).is_err() {
                return None;
            }
            let value_len = usize::from_le_bytes(len_buf);

            let mut value_buf = vec![0u8; value_len];
            if self.reader.read_exact(&mut value_buf).is_err() {
                return None;
            }

            value = Some(value_buf);
        }

        let mut timestamp_buf = [0; size_of::<u128>()];
        if self.reader.read_exact(&mut timestamp_buf).is_err() {
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buf);

        Some(SSTableEntry {
            key,
            value,
            timestamp,
        })
    }
}
