## RocketChat GridFS to FileSystem/AmazonS3 Migration

This script migrates files uploaded to [RocketChat](https://rocket.chat/) from the default `GridFS` storage to FileSystem or AmazonS3.

    migrate -c [command] -d [s3 bucket|output directory] -r [dbname] -t [target]

### EGroupware GmbH fork

This is a fork of [arminfelder/gridfsmigrate](https://github.com/arminfelder/gridfsmigrate) (GPLv3, as the original) for our
Rocket.Chat instances (Rocket.Chat 8.x, MongoDB 8, S3 on IONOS). Branch `egroupware` contains the changes:

* **S3 compatible services**: `--s3-endpoint-url`, `--s3-region`, `--s3-path-style` (the original only talks to AWS), optional `--s3-key-prefix`;
  credentials as usual for boto3 (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
* **MongoDB**: `--mongo-uri` (replica set, `authSource`) or `--user` / `--auth-source`, password from `MONGO_PASSWORD`
* **FileSystem source**: `--source filesystem --source-dir /app/uploads` migrates the files of Rocket.Chat's FileSystem store
  (flat directory, file name = `_id`) to S3, not only GridFS
* **Streaming copies**: no whole file in memory; every copy is verified (size in the target against the bytes read)
* **Resumable**: the log (`-l`, default `migrate_<database>.csv`) is written after every file, a repeated `dump` skips what is in the
  log and accepts an identical object that already exists in the target
* **Failures are reported as failures**: a failed copy is no longer logged as success, a document without a file in GridFS is reported
  as `missing` (and does not stop the run); the exit code is 1 if copies failed
* **`dump --dry-run`** and `--limit N` for a first try, new command **`verify`**
* **`updatedb` and `removeblobs` verify before they change anything**: `updatedb` only points an upload to the target if the copy
  exists with the right size, `removeblobs` only deletes the GridFS file of an upload that `updatedb` already moved and whose copy is
  verified (`--skip-verify` switches the check off). `updatedb` is idempotent.
* **Key layout of Rocket.Chat 8**: `<uniqueID>/uploads/<rid>/<userId>/<fileId>` (lower case `uploads`), and `updatedb` keeps the
  `path` and `url` of an upload (host, prefix and file name) and only replaces the store name in them. Rocket.Chat reads S3 files by the
  key stored in `AmazonS3.path` and the store name `AmazonS3:Uploads`.
* `gridfs` is not in `requirements.txt` any more: that is an unrelated package on PyPI, GridFS is part of `pymongo`

#### Example: GridFS to an S3 bucket of an instance

Make a backup of the database first (`mongodump`). Then:

    export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... MONGO_PASSWORD=...
    S3="-t AmazonS3 -d <bucket> --s3-endpoint-url https://s3.eu-central-3.ionoscloud.com --s3-region eu-central-3"
    DB="--mongo-uri mongodb://<host>:27017/?replicaSet=rs0 --user <user> --auth-source admin -r <database>"
    ./migrate.py -c dump $DB $S3 --dry-run            # what would be copied
    ./migrate.py -c dump $DB $S3 --limit 20           # first try, repeat the same command without --limit later
    ./migrate.py -c dump $DB $S3                      # resumable, run it again after an error
    ./migrate.py -c verify $DB $S3
    # configure the instance for S3 (settings FileUpload_S3_Bucket, FileUpload_S3_BucketURL, FileUpload_S3_Region,
    # FileUpload_S3_ForcePathStyle, FileUpload_S3_AWSAccessKeyId, FileUpload_S3_AWSSecretAccessKey), then:
    ./migrate.py -c updatedb $DB $S3
    # check downloads of old attachments in Rocket.Chat, and only then, maybe much later:
    ./migrate.py -c removeblobs $DB $S3

#### Tests

    python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
    .venv/bin/python -m pytest tests

The tests use mongomock and a mocked S3 (moto), GridFS itself is not covered by them.

### Help

Run `./migrate.py -h` to see all available options.

### Commands

- **dump**:        Exports files from GridFS to the specified folder/S3 bucket and generates a log file.
- **updatedb**:    Updates database entries to point to the new storage instead of GridFS.
- **removeblobs**: Removes migrated files from GridFS.

### Requirements

#### Installing Dependencies

- python3 (e.g. `sudo apt install python3 python3-pip`)

- Install dependencies with:

      pip install -r requirements.txt

  Or manually:

      pip install pymongo tqdm boto3

#### Environment

- If using Amazon S3, set credentials as environment variables ([Boto3 configuration guide](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html)).
- Export `PYTHONIOENCODING=utf-8` to avoid issues with non-ASCII filenames.

#### Recommended MongoDB Index

To speed up file lookups in GridFS, create the following index in your Rocket.Chat database:

      db.rocketchat_uploads.chunks.createIndex({ files_id: 1, n: 1 }, { unique: true })

Not having this index can make migration very slow on large databases.

#### Default Rocket.Chat Database Name

- When installed via snap, the default database name is `parties` and usually there is no username or password.
- Example usage:

      ./migrate.py -c dump -r parties -t FileSystem -d /app/uploads

### Multi-threaded Execution

To speed up migration, you can configure the script to use multiple workers. Adjust according to your CPU core count. Example:

      ./migrate.py -c dump -r parties -t FileSystem -d /app/uploads --max-workers 8

### Steps

1. **Backup your MongoDB database** so you don't lose any data in case of issues. ([MongoDB Backup Methods](https://docs.mongodb.com/manual/core/backups/))

2. **Change the Storage Type** in Rocket.Chat under `Administration > File Upload` to `FileSystem` or `AmazonS3`. Update the relevant configuration settings.

3. **Start copying files to the new storage:**

   - **File System**

         ./migrate.py -c dump -r rocketchat -t FileSystem -d /app/uploads

   - **S3**

         ./migrate.py -c dump -r rocketchat -t AmazonS3 -d S3bucket_name

4. **Update the database to use the new storage** (use `-t AmazonS3` if migrating to S3):

      ./migrate.py -c updatedb -d /app/uploads -r rocketchat -t FileSystem

5. **Check if everything is working correctly** and ensure there are no missing files.

6. **Remove obsolete data from GridFS:**

      ./migrate.py -c removeblobs -d /app/uploads -r rocketchat

### Troubleshooting

In some configurations, it may help to add the parameters `directconnection=True` and `connect=False` to the MongoClient constructor, for example:

      MongoClient(..., retryWrites=False, directconnection=True, connect=False)[self.db]

This ensures the connection uses Single topology.

