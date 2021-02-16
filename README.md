# Migrate Cassandra to RocksDB

## Install

Adapt command parameters in `docker-compose.yml`:
    
```bash
usage: create_sst_diffs.py [-h] [--table TABLE]
                           sst_dir keyspace nodes [nodes ...]

Create sst files diffing cassandra and rocksdb.

positional arguments:
  sst_dir        subdirectory in /sst_diffs with existing sst files
  keyspace       keyspace to grab
  nodes          cassandra nodes

optional arguments:
  -h, --help     show this help message and exit
  --table TABLE  optionally the table to grab
```

Run with `docker-compose run --rm cassandra_to_rocksdb`.

`./sst_diffs` then contains sst dumps per keyspace!
