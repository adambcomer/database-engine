use std::path::{Path, PathBuf};
use std::fs::{read_dir};

/// Gets the set of files with an extension for a given directory.
pub fn files_with_ext(dir: &Path, ext: &str) -> Vec<PathBuf> {
  let mut files = Vec::new();
  for file in read_dir(dir).unwrap() {
    let path = file.unwrap().path();
    if path.extension().unwrap() == ext {
      files.push(path);
    }
  }

  files
}