# ScyllaBackup

## Overview
This is a tool written to take scylla snapshots and store the backups in cloud storage

## TODOs
- [ ] Do not rely on db for metadata.
  - [ ] Upload schema in s3 as a file. Store in db as a record.
  - [ ] Upload snapshot file list in s3 in a timestamp
  - [ ] Cleanup function should take care of these files when removing data
