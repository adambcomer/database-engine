# How to Build a Simple Database

Databases are an integral part to software development and to better understand how they work, I made my own. In this project, I built a LSM-Tree Key-Value Store based on [RocksDB](https://github.com/facebook/rocksdb) in Rust.

## Documentation

While building this database, I wrote easy to understand articles explaining how each component works and fits into the larger project. Knowledge of database storage engines is very scattered online, and I spent several months reading papers, documentation, and code to get a basic understanding. I made this guide for my beginner self who was often lost trying to piece together how a database worked.

- [Build a Database Pt. 1: Motivation & Design](https://adambcomer.com/blog/simple-database/motivation-design/)
- [Build a Database Pt. 2: MemTable](https://adambcomer.com/blog/simple-database/memtable/)
- [Build a Database Pt. 3: Write Ahead Log(WAL)](https://adambcomer.com/blog/simple-database/wal/)
- Build a Database Pt. 4: SSTable
- Build a Database Pt. 5: Compaction
- Build a Database Pt. 6: Putting it Together
- Build a Database Pt. 7: Using the Database

## Tests

Running tests is very simple with cargo.

```shell
cargo test
```
