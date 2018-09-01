Scyllabackup
============

## Overview

**Scyllabackup** is a backup management tool for scylladb. It allows taking
snapshots of scylladb and upload the resulting snapshot to cloud storage. The
intended usage is to use this tool via cronjob which takes regular snapshots and
uploads them to highly available cloud storage services. It also implements some
workflows to automate the restore of scylladb. All this is packaged in a simple
cli wrapped around a modular codebase.

It supports uploading snapshots to cloud storage via
[spongeblob](https://github.com/helpshift/spongeblob.py) library, thus
supporting AWS Simple Storage Service (*S3*) and Windows Azure Blob Storage
(*WABS*) services. It wraps nodetool and cqlsh utility to take backup with a
single call. It stores the metadata in a local sqlite file which is also
uploaded to remote server with each upload for disaster recovery.


## Installation

You can install scyllabackup via pip

```
pip install scyllabackup
```

## Dependencies

Scyllabackup manages cloud storage via `spongeblob` python module which
implements a common interface for both S3 and WABS.

## Usage

Scyllabackup implements its various operations as subcommands. Following
subcommands are available.


### take

To take backup run:

```
scyllabackup take -c /etc/scyllabackup.yml
```

This will create a snapshot and upload it to cloud storage as defined by
parameters in /etc/scyllabackup.yml. It also takes a schema dump of scylladb and
stores it in sqlite database. The schema backup is required when restoring.

Snapshots in scyllabackup are referenced by unix `epoch` when the snapshot was
taken. All backup metadata is stored against this `epoch` value, and the `epoch`
is required for other operations like download/restore of snapshots and deleting
old snapshots.

This command should be run regularly via a cronjob at same time across an entire
scylladb cluster for consistent backups.


### list

This command lists down all available snapshots

```
scyllabackup list -c /etc/scyllabackup.yml
```

Additionally, if you want a snapshot before a specific timestamp in unix epoch
integer, use the following variant. This lists the last 5 snapshots before the
specified timestamp:

```
scyllabackup list --latest-before <unix_timestamp> -c /etc/scyllabackup.yml
```

### download\_db

When restoring a cluster, you will first require to download the sqlite metadata
file. In case of a disaster, this file will be available in cloud storage. To
fetch this, run the following command:

```
scyllabackup download_db -c /etc/scyllabackup.yml <download_db_path>
```

The db file will be downloaded to `download_db_path` location. Please note that
the `db` parameter in /etc/scyllabackup.yml should be same as the db filename
used when taking backups. This is because the db file uploaded in cloud storage
has same basename as the file present on filesystem and this is configurable.

### download

Once the scyllabackup backup metadata db is downloaded, you can download a snapshot using
following command:

```
scyllabackup download -c /etc/scyllabackup.yml --snapshot <epoch> --download-dir <download_dir> --schema <schema_file>
```

This will download the snapshot referenced by `epoch` in the `download_dir`
directory. The schema file is also generated in the path specified by
`schema_file`. Please ensure that `download_dir` this is not the same directory
as the data directory for scylladb node where you want to restore. As it may
result in overwrite of files which may be unintended. This command can also take
`--keyspace` parameter if you want to download snapshotted sstable files for a
specific keyspace.

There is a variant of this command which takes a `--latest-before` parameter
instead of `--snapshot` parameter. These flags are mutually exclusive, and will
result in error if used together. When `--latest-before` parameter is used,
instead of downloading the exact snapshot, the download command will download
the latest snapshot before the specified timestamp. This function is useful for
building automation for restoring multiple nodes in a scylladb cluster to a
snapshot which was taken around same time.

### verify

Downloading snapshot is an expensive operations and sometimes you may find that
some files are missing (although unlikely). This command is implemented for a
sanity check that all files for a snapshot are existing, there is a verify
command, which verifies that all files for a snapshot in metadata db are present
on the cloud storage, without downloading them.

```
scyllabackup verify -c /etc/scyllabackup.yml <snapshot_epoch>
```

This verifies that all files for snapshot referenced by `snapshot_epoch` are present.

### restore\_schema

The schema file generated via the `download` subcommand can be restored via this
one. It wraps cqlsh to read the schema file and restore it. You may also use
cqlsh directly. This command is implemented to keep the interface uniform. Note
that schema should be restored before attempting to restore the data.

```
scyllabackup restore_schema -c /etc/scyllabackup.yml <schema_file>
```

This will restore schema in scylladb by running DDL statements in `schema_file`.
This command asks for a manual confirmation, which can be skipped with `-f`
flag.

### get\_restore\_mapping

The table directories in scylladb have a UUID in their name which is generated
when the tables are created. This UUID will be different from in a fresh
clustered where schema is restored from the UUID of tables in a snapshot.

It is cumbersome to manually figure this out and manually restore the backup
files. This is a helper command to automate restore. It generates a json file
which has a mapping of table directories between the downloaded snapshot and
schema restored fresh cluster. The scylladb service should be up to generate
this mapping.

```
scyllabackup get_restore_mapping -c /etc/scyllabackup.yml --restore-path <download_dir> --keyspace <keyspace> <restore_mapping_file>
```

Where `download_dir` is the directory where a snapshot files are downloaded via
`download` subcommand, `keyspace` is the keyspace you want to restore and
`restore_mapping_file` is the file where the json mapping will be written to.

### restore

Once the restore_mapping is available, you can restore the files from snapshot
to scylladb. Ensure that scylladb is stopped before restoring the files. There
is a basic sanity check would warn if scylladb is still up, but you should
double check that. The restore can be performed using following command:

```
scyllabackup restore -c /etc/scyllabackup.yml --restore-path <download_dir> --restore-mapping-file <restore_mapping_file>
```

`download_dir` is the directory where the snapshot was downloaded via `download`
subcommand. `restore_mapping_file` is the file which was generated via
`get_restore_mapping` subcommand. This will move all sstable files from download
dir to scylladb data dir. Please note that `restore_mapping_file` has mapping
for only one keyspace, you may need to run this command multiple times for other
keyspace.

### delete\_older\_than

This command delete snapshots older than specified number of days from cloud
storage. The snapshot references are also dropped from metadata db, thereby
performing a cleanup and keep the sqlite db size consistent.

```
scyllabackup delete_older_than -c /etc/scyllabackup.yml <number_of_days>
```

Usually, it's a good practice to delete snapshots older than 30 days for various
compliance (GDPR etc.).

## Common Options

There are some common options for all scyllabackup subcommands which can either
be passed as cli args or can be loaded from a config file for brevity. Some of
these options have sane default value and may not require to be passed. The
important parameters are documented in this section. A documentation of options
can be retrieved via `-h` for a subcommand

```
usage: scyllabackup take [-h] [-c CONF_FILE]
                         [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}] --path PATH
                         --db DB --provider {s3,wabs}
                         [--nodetool-path NODETOOL_PATH]
                         [--cqlsh-path CQLSH_PATH] [--cqlsh-host CQLSH_HOST]
                         [--cqlsh-port CQLSH_PORT]
                         [--s3-bucket-name BUCKET_NAME] [--s3-aws-key AWS_KEY]
                         [--s3-aws-secret AWS_SECRET]
                         [--wabs-container-name WABS_CONTAINER_NAME]
                         [--wabs-account-name WABS_ACCOUNT_NAME]
                         [--wabs-sas-token WABS_SAS_TOKEN] --prefix PREFIX
                         [--lock LOCK] [--lock-timeout LOCK_TIMEOUT]
                         [--max-workers MAX_WORKERS]

Args that start with '--' (eg. -l) can also be set in a config file (specified
via -c). Config file syntax allows: key=value, flag=true, stuff=[a,b,c] (for
details, see syntax at https://goo.gl/R74nmi). If an arg is specified in more
than one place, then commandline values override config file values which
override defaults.

optional arguments:
  -h, --help            show this help message and exit
  -c CONF_FILE, --conf-file CONF_FILE
                        Config file for scyllabackup (default: None)
  -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Log level for scyllabackup (default: WARNING)
  --path PATH           Path of scylla data directory (default: None)
  --db DB               Path of scyllabackup db file. The backup metadata is
                        stored in this file. (default: None)
  --provider {s3,wabs}  Cloud provider used for storage. It should be one of
                        `s3` or `wabs` (default: None)
  --nodetool-path NODETOOL_PATH
                        Path of nodetool utility on filesystem. (default:
                        /usr/bin/nodetool)
  --cqlsh-path CQLSH_PATH
                        Path of cqlsh utility on filesystem (default:
                        /usr/bin/cqlsh)
  --cqlsh-host CQLSH_HOST
                        Host to use for connecting cqlsh service (default:
                        127.0.0.1)
  --cqlsh-port CQLSH_PORT
                        Port to use for connecting cqlsh service (default:
                        9042)
  --prefix PREFIX       Mandatory prefix to store backups in cloud storage
                        (default: None)
  --lock LOCK           Lock file for scyllabackup. (default:
                        /var/run/lock/scyllabackup.lock)
  --lock-timeout LOCK_TIMEOUT
                        Timeout for taking lock. (default: 10)
  --max-workers MAX_WORKERS
                        Sets max workers for parallelizing storage api calls
                        (default: 4)

Required arguments if using 's3' provider:
  --s3-bucket-name BUCKET_NAME
                        Mandatory if provider is s3 (default: None)
  --s3-aws-key AWS_KEY  Mandatory if provider is s3 (default: None)
  --s3-aws-secret AWS_SECRET
                        Mandatory if provider is s3 (default: None)

Required arguments if using 'wabs' provider:
  --wabs-container-name WABS_CONTAINER_NAME
                        Mandatory if provider is wabs (default: None)
  --wabs-account-name WABS_ACCOUNT_NAME
                        Mandatory if provider is wabs (default: None)
  --wabs-sas-token WABS_SAS_TOKEN
                        Mandatory if provider is wabs (default: None)
```

## TODOs
- [ ] Support PITR with commitlogs (requires commitlog archiving support from scylladb)
- [ ] Some common cli opts are not required in all subcommands, require fixing.
- [ ] Do not rely on db for metadata.
  - [ ] Upload schema in s3 as a file. Store in db as a record.
  - [ ] Upload snapshot file list in s3 in a timestamp
  - [ ] Cleanup function should take care of these files when removing data
