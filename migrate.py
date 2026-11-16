#!/usr/bin/env python3
"""
This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.
This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses/>.

Migrates the uploads of a Rocket.Chat instance (collection rocketchat_uploads) from GridFS or from the FileSystem
store to an S3 bucket (any S3 compatible service) or to a directory.

This fork (EGroupware GmbH) is based on https://github.com/arminfelder/gridfsmigrate and adds: S3 endpoint, region and
path-style options, MongoDB authSource, a FileSystem source, streaming copies (no whole file in memory), size
verification of every copy, a resumable log (written after every file), a dry-run and a "verify" command, and
updatedb/removeblobs that only touch uploads whose copy was verified. The S3 key is the one Rocket.Chat itself
uses (uniqueID/uploads/rid/userId/fileId).
"""

__author__ = "Armin Felder"
__contact__ = "armin.felder@osalliance.com"
__copyright__ = "Copyright 2019, Armin Felder; 2026 modifications by EGroupware GmbH"
__credits__ = ["all contributors", "EGroupware GmbH"]
__date__ = "2019.05.05"
__deprecated__ = False
__email__ = "armin.felder@osalliance.com"
__license__ = "GPLv3"
__maintainer__ = "EGroupware GmbH"
__status__ = "Production"
__version__ = "1.2.0-egroupware"

import argparse
import csv
import logging
import os
import sys
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import BinaryIO, Dict, Iterable, List, Optional

logger = logging.getLogger("gridfsmigrate")

COLLECTION = "rocketchat_uploads"


# ---------------------------------------------------------------------------------------------------------------------
# pure helpers
# ---------------------------------------------------------------------------------------------------------------------

def s3_key(unique_id: str, upload: Dict, prefix: str = "") -> str:
    """The object key Rocket.Chat's own S3 store uses: <uniqueID>/uploads/<rid>/<userId>/<fileId>

    Rocket.Chat reads the file via the key stored in the upload as AmazonS3.path, so the layout is not critical,
    but using the same one keeps migrated and new files side by side. An optional prefix goes in front of it.
    """
    key = "{}/uploads/{}/{}/{}".format(unique_id, upload.get("rid", "unknown"), upload.get("userId", "unknown"), upload["_id"])
    prefix = (prefix or "").strip("/")
    return f"{prefix}/{key}" if prefix else key


def swap_store(value: Optional[str], old_store: str, new_store: str) -> Optional[str]:
    """Replace the store name in the path or url of an upload, keeping host, prefix and file name

    "rocketchat/ufs/GridFS:Uploads/<id>/<name>" and "https://host/rocketchat/ufs/GridFS:Uploads/<id>/<name>" and
    "/ufs/GridFS:Uploads/<id>/<name>" keep their form, only the store changes. Anything else is returned unchanged.
    """
    if not value:
        return value
    return value.replace(f"/{old_store}/", f"/{new_store}/", 1)


def encode_uri(string: str) -> str:
    """Like JavaScript's encodeURI(), as Rocket.Chat uses it for the Content-Disposition of S3 objects"""
    return urllib.parse.quote(string, safe="~@#$&()*!+=:;,.?/'")


class CountingReader:
    """Wraps a binary stream and counts the bytes read from it, boto3 only needs read()"""

    def __init__(self, stream: BinaryIO):
        self.stream = stream
        self.count = 0

    def read(self, size: int = -1) -> bytes:
        data = self.stream.read(size)
        self.count += len(data)
        return data


# ---------------------------------------------------------------------------------------------------------------------
# sources: where the file content is read from
# ---------------------------------------------------------------------------------------------------------------------

class SourceFileMissing(Exception):
    """The upload document exists, but its content does not"""


class GridFSSource:
    """Rocket.Chat's GridFS store: bucket rocketchat_uploads, file _id = _id of the upload"""

    kind = "GridFS"

    def __init__(self, db, store_name: str = "Uploads", collection: str = COLLECTION):
        import gridfs
        self.gridfs = gridfs
        self.fs = gridfs.GridFSBucket(db, bucket_name=collection)
        self.chunks = db[f"{collection}.chunks"]
        self.store = f"{self.kind}:{store_name}"

    def open(self, upload: Dict) -> BinaryIO:
        try:
            stream = self.fs.open_download_stream(upload["_id"])
        except self.gridfs.errors.NoFile:
            raise SourceFileMissing(f"no GridFS file {upload['_id']}")
        # the file document can exist without its chunks (seen in production): that is a missing, not a failed file
        if stream.length and self.chunks.find_one({"files_id": upload["_id"], "n": 0}, {"_id": 1}) is None:
            stream.close()
            raise SourceFileMissing(f"GridFS file {upload['_id']} has no chunks")
        return stream

    def delete(self, upload_id: str) -> bool:
        """delete file and chunks, returns False if it was already gone"""
        try:
            self.fs.delete(upload_id)
            return True
        except self.gridfs.errors.NoFile:
            return False


class FileSystemSource:
    """Rocket.Chat's FileSystem store: one file per upload in a flat directory, the file name is the _id"""

    kind = "FileSystem"

    def __init__(self, directory: str, store_name: str = "Uploads"):
        self.directory = Path(directory)
        if not self.directory.is_dir():
            raise ValueError(f"Source directory does not exist: {directory}")
        self.store = f"{self.kind}:{store_name}"
        self._index = None
        self._lock = threading.Lock()

    def open(self, upload: Dict) -> BinaryIO:
        path = self.directory / str(upload["_id"])
        try:
            return open(path, "rb")
        except FileNotFoundError:
            pass
        # old Rocket.Chat versions stored some files with an extension: <_id>.<ext>, use it if it is unique
        candidates = self._with_extension().get(str(upload["_id"]), [])
        if len(candidates) == 1:
            return open(self.directory / candidates[0], "rb")
        raise SourceFileMissing(f"no file {path}" + (f" ({len(candidates)} candidates with extension)" if candidates else ""))

    def _with_extension(self) -> Dict[str, list]:
        """index of the files with an extension by their basename, built once on first use"""
        with self._lock:  # called from the worker threads, the index must be complete before others use it
            if self._index is None:
                index: Dict[str, list] = {}
                for name in os.listdir(self.directory):
                    if "." in name:
                        index.setdefault(name.split(".", 1)[0], []).append(name)
                self._index = index
            return self._index

    def delete(self, upload_id: str) -> bool:
        raise NotImplementedError("removeblobs is only available for the GridFS source, remove the old files yourself")


# ---------------------------------------------------------------------------------------------------------------------
# targets: where the file content is written to
# ---------------------------------------------------------------------------------------------------------------------

class AmazonS3Target:
    """An S3 bucket, Amazon or any S3 compatible service (endpoint_url), credentials as usual for boto3
    (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY or a profile)"""

    name = "AmazonS3"

    def __init__(self, bucket: str, endpoint_url: Optional[str] = None, region: Optional[str] = None,
                 path_style: bool = False, prefix: str = "", client=None):
        self.bucket = bucket
        self.prefix = prefix
        if client is None:
            import boto3
            from botocore.config import Config
            client = boto3.client(
                "s3", endpoint_url=endpoint_url, region_name=region,
                config=Config(s3={"addressing_style": "path" if path_style else "auto"},
                              retries={"max_attempts": 5, "mode": "standard"}))
        self.client = client
        self.unique_id: Optional[str] = None
        logger.info(f"S3 target: bucket={bucket}, endpoint={endpoint_url or 'default'}, region={region or 'default'}, "
                    f"path-style={path_style}, key prefix='{prefix}'")

    def key_for(self, upload: Dict) -> str:
        if not self.unique_id:
            raise ValueError("uniqueID of the Rocket.Chat instance is not known")
        return s3_key(self.unique_id, upload, self.prefix)

    def put(self, key: str, stream: BinaryIO, upload: Dict) -> None:
        extra = {"ContentDisposition": f'inline; filename="{encode_uri(upload.get("name", upload["_id"]))}"'}
        if upload.get("type"):
            extra["ContentType"] = upload["type"]
        self.client.upload_fileobj(stream, self.bucket, key, ExtraArgs=extra)

    def size(self, key: str) -> Optional[int]:
        """size of the object, None if it does not exist"""
        from botocore.exceptions import ClientError
        try:
            return int(self.client.head_object(Bucket=self.bucket, Key=key)["ContentLength"])
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return None
            raise


class FileSystemTarget:
    """A directory with one file per upload, named by its _id like Rocket.Chat's FileSystem store"""

    name = "FileSystem"

    def __init__(self, directory: str):
        self.directory = Path(directory)
        if not self.directory.is_dir():
            raise ValueError(f"Destination directory does not exist: {directory}")
        self.unique_id: Optional[str] = None

    def key_for(self, upload: Dict) -> str:
        return str(self.directory / str(upload["_id"]))

    def put(self, key: str, stream: BinaryIO, upload: Dict) -> None:
        tmp = Path(key + ".part")
        with open(tmp, "wb") as out:
            while chunk := stream.read(1024 * 1024):
                out.write(chunk)
        tmp.replace(key)

    def size(self, key: str) -> Optional[int]:
        try:
            return os.stat(key).st_size
        except FileNotFoundError:
            return None


# ---------------------------------------------------------------------------------------------------------------------
# migration log: one row per copied file, written after every file, so a crashed run can be resumed
# ---------------------------------------------------------------------------------------------------------------------

@dataclass
class LogRow:
    id: str
    file: str
    collection: str
    key: str
    size: Optional[int] = None


class MigrationLog:
    def __init__(self, path: str):
        self.path = Path(path)
        self.lock = threading.Lock()

    def rows(self) -> List[LogRow]:
        if not self.path.exists():
            return []
        rows = []
        with open(self.path, newline="") as csvfile:
            for row in csv.reader(csvfile):
                if len(row) < 4:
                    continue
                size = int(row[4]) if len(row) > 4 and row[4].isdigit() else None
                rows.append(LogRow(row[0], row[1], row[2], row[3], size))
        return rows

    def append(self, row: LogRow) -> None:
        with self.lock:
            with open(self.path, "a", newline="") as csvfile:
                csv.writer(csvfile).writerow([row.id, row.file, row.collection, row.key, "" if row.size is None else row.size])


# ---------------------------------------------------------------------------------------------------------------------
# the migration
# ---------------------------------------------------------------------------------------------------------------------

class Migrator:
    def __init__(self, db, source, target, logfile: str, collection: str = COLLECTION, max_workers: int = 4,
                 dry_run: bool = False, store_name: str = "Uploads"):
        self.db = db
        self.source = source
        self.target = target
        self.collection = collection
        self.uploads = db[collection]
        self.log = MigrationLog(logfile)
        self.max_workers = max_workers
        self.dry_run = dry_run
        self.new_store = f"{target.name}:{store_name}"

    def unique_id(self) -> str:
        row = self.db.rocketchat_settings.find_one({"_id": "uniqueID"})
        if not row or not row.get("value"):
            raise ValueError("uniqueID not found in rocketchat_settings")
        return row["value"]

    # -- dump -------------------------------------------------------------------------------------------------------
    def _copy_one(self, upload: Dict) -> Dict:
        """Copy one upload, returns a dict with "status" in ok, exists, dry-run, missing, failed"""
        upload_id = upload["_id"]
        try:
            key = self.target.key_for(upload)
            expected = int(upload["size"]) if str(upload.get("size", "")).isdigit() else None
            if self.dry_run:
                return {"status": "dry-run", "id": upload_id, "key": key}
            # a previous run (or somebody else) already put the identical object there
            existing = self.target.size(key)
            if existing is not None and expected is not None and existing == expected:
                return {"status": "exists", "id": upload_id, "key": key, "size": existing,
                        "file": upload.get("name", upload_id)}
            try:
                stream = self.source.open(upload)
            except SourceFileMissing as e:
                return {"status": "missing", "id": upload_id, "reason": str(e)}
            with stream:
                counter = CountingReader(stream)
                self.target.put(key, counter, upload)
            written = self.target.size(key)
            if written is None or written != counter.count:
                return {"status": "failed", "id": upload_id,
                        "reason": f"size after copy is {written}, read {counter.count} bytes"}
            if expected is not None and expected != counter.count:
                logger.warning(f"{upload_id}: size in rocketchat_uploads is {expected}, but the source has {counter.count} bytes")
            return {"status": "ok", "id": upload_id, "key": key, "size": counter.count,
                    "file": upload.get("name", upload_id)}
        except Exception as e:  # one broken file must not stop the run
            logger.error(f"Error copying upload {upload_id}: {e}")
            return {"status": "failed", "id": upload_id, "reason": str(e)}

    def dump(self, limit: Optional[int] = None) -> Dict[str, int]:
        start = perf_counter()
        if self.target.name == "AmazonS3":
            self.target.unique_id = self.unique_id()
        done = {row.id for row in self.log.rows()}
        query = {"store": self.source.store, "complete": True}
        pending = [u for u in self.uploads.find(query) if u["_id"] not in done]
        incomplete = self.uploads.count_documents({"store": self.source.store, "complete": {"$ne": True}})
        if limit:
            pending = pending[:limit]
        logger.info(f"{len(pending)} uploads to copy from {self.source.store}, {len(done)} already in the log, "
                    f"{incomplete} incomplete ones are skipped{' (dry-run)' if self.dry_run else ''}")

        counts = {"ok": 0, "exists": 0, "dry-run": 0, "missing": 0, "failed": 0}
        problems: List[Dict] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._copy_one, upload) for upload in pending]
            for future in _progress(as_completed(futures), len(futures), "Copying"):
                result = future.result()
                counts[result["status"]] += 1
                if result["status"] in ("ok", "exists"):
                    self.log.append(LogRow(result["id"], result["file"], self.collection, result["key"], result["size"]))
                elif result["status"] in ("missing", "failed"):
                    problems.append(result)
        for problem in problems:
            logger.warning(f"{problem['status']}: {problem['id']}: {problem.get('reason', '')}")
        logger.info(f"Copied {counts['ok']}, already there {counts['exists']}, missing source {counts['missing']}, "
                    f"failed {counts['failed']} in {perf_counter() - start:.1f} seconds")
        counts["incomplete"] = incomplete
        return counts

    # -- verify / updatedb / removeblobs ---------------------------------------------------------------------------
    def _verify_row(self, row: LogRow) -> Optional[str]:
        """None if the copy of the row is fine, otherwise the reason it is not"""
        size = self.target.size(row.key)
        if size is None:
            return "object does not exist"
        if row.size is not None and size != row.size:
            return f"size is {size}, expected {row.size}"
        return None

    def verify(self) -> Dict[str, int]:
        rows = self.log.rows()
        bad = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._verify_row, row): row for row in rows}
            for future in _progress(as_completed(futures), len(rows), "Verifying"):
                reason = future.result()
                if reason:
                    bad += 1
                    logger.error(f"{futures[future].id}: {reason}")
        logger.info(f"Verified {len(rows)} copies, {bad} are not ok")
        return {"checked": len(rows), "bad": bad}

    def _update_one(self, row: LogRow, verify: bool) -> str:
        doc = self.uploads.find_one({"_id": row.id}, {"store": 1, "path": 1, "url": 1})
        if doc is None:
            return "unknown"
        if doc.get("store") == self.new_store:
            return "already"
        if doc.get("store") != self.source.store:
            return "other-store"
        if verify and self._verify_row(row):
            return "bad-copy"
        data = {
            "store": self.new_store,
            "path": swap_store(doc.get("path"), self.source.store, self.new_store),
            "url": swap_store(doc.get("url"), self.source.store, self.new_store),
        }
        data = {k: v for k, v in data.items() if v is not None}
        if self.target.name == "AmazonS3":
            data["AmazonS3"] = {"path": row.key}
        result = self.uploads.update_one({"_id": row.id, "store": self.source.store}, {"$set": data})
        return "updated" if result.modified_count else "already"

    def updatedb(self, verify: bool = True) -> Dict[str, int]:
        start = perf_counter()
        rows = self.log.rows()
        counts: Dict[str, int] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._update_one, row, verify): row for row in rows}
            for future in _progress(as_completed(futures), len(rows), "Updating DB"):
                try:
                    status = future.result()
                except Exception as e:
                    logger.error(f"Failed to update record {futures[future].id}: {e}")
                    status = "failed"
                counts[status] = counts.get(status, 0) + 1
                if status in ("bad-copy", "other-store", "unknown"):
                    logger.warning(f"{futures[future].id}: not updated ({status})")
        logger.info(f"Updated {counts.get('updated', 0)} of {len(rows)} records in {perf_counter() - start:.1f} seconds: {counts}")
        return counts

    def _remove_one(self, row: LogRow, verify: bool) -> str:
        doc = self.uploads.find_one({"_id": row.id}, {"store": 1})
        if doc is None or doc.get("store") != self.new_store:
            return "not-migrated"  # never delete the only copy: updatedb has to point the upload to the target first
        if verify and self._verify_row(row):
            return "bad-copy"
        return "removed" if self.source.delete(row.id) else "gone"

    def removeblobs(self, verify: bool = True) -> Dict[str, int]:
        start = perf_counter()
        rows = self.log.rows()
        counts: Dict[str, int] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._remove_one, row, verify): row for row in rows}
            for future in _progress(as_completed(futures), len(rows), "Removing blobs"):
                try:
                    status = future.result()
                except Exception as e:
                    logger.warning(f"Failed to remove {futures[future].id}: {e}")
                    status = "failed"
                counts[status] = counts.get(status, 0) + 1
                if status in ("not-migrated", "bad-copy"):
                    logger.warning(f"{futures[future].id}: NOT removed ({status})")
        logger.info(f"Removed {counts.get('removed', 0)} of {len(rows)} blobs in {perf_counter() - start:.1f} seconds: {counts}")
        return counts


def _progress(iterable: Iterable, total: int, desc: str):
    try:
        from tqdm import tqdm
        return tqdm(iterable, total=total, desc=desc, disable=not sys.stderr.isatty())
    except ImportError:  # pragma: no cover
        return iterable


# ---------------------------------------------------------------------------------------------------------------------
# command line
# ---------------------------------------------------------------------------------------------------------------------

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Migrate Rocket.Chat uploads from GridFS or the FileSystem store to S3 or a directory.")
    parser.add_argument("-c", "--command", required=True, choices=["dump", "verify", "updatedb", "removeblobs"],
                        help="dump: copy the files, verify: check the copies, updatedb: point the uploads in MongoDB to "
                             "the copies, removeblobs: delete the GridFS files of migrated uploads")
    mongo = parser.add_argument_group("MongoDB")
    mongo.add_argument("--mongo-uri", help="MongoDB URI incl. replicaSet and authSource, instead of host/port/user")
    mongo.add_argument("-s", "--host", default="localhost", help="MongoDB host (default: %(default)s)")
    mongo.add_argument("-p", "--port", type=int, default=27017, help="MongoDB port (default: %(default)s)")
    mongo.add_argument("-r", "--database", default="rocketchat", help="Database name (default: %(default)s)")
    mongo.add_argument("--user", help="MongoDB username")
    mongo.add_argument("--password", help="MongoDB password (better use the environment variable MONGO_PASSWORD)")
    mongo.add_argument("--auth-source", help="database the MongoDB user is defined in, e.g. admin")
    src = parser.add_argument_group("source")
    src.add_argument("--source", choices=["gridfs", "filesystem"], default="gridfs",
                     help="where the files are now (default: %(default)s)")
    src.add_argument("--source-dir", help="directory of Rocket.Chat's FileSystem store (/app/uploads), for --source filesystem")
    dst = parser.add_argument_group("target")
    dst.add_argument("-t", "--target", choices=["AmazonS3", "FileSystem"], default="FileSystem",
                     help="where the files go (default: %(default)s)")
    dst.add_argument("-d", "--destination", help="S3 bucket name or directory (required, all commands check the copies)")
    dst.add_argument("--s3-endpoint-url", help="endpoint of an S3 compatible service, e.g. https://s3.eu-central-3.ionoscloud.com")
    dst.add_argument("--s3-region", help="S3 region")
    dst.add_argument("--s3-path-style", action="store_true", help="use path-style instead of virtual-host-style addressing")
    dst.add_argument("--s3-key-prefix", default="", help="optional prefix for all object keys (default: none)")
    parser.add_argument("--store-name", default="Uploads", help="name of the Rocket.Chat store (default: %(default)s)")
    parser.add_argument("-l", "--log-file",
                        help="migration log (CSV), used to resume dump and as input of verify, updatedb and removeblobs "
                             "(default: migrate_<database>.csv)")
    parser.add_argument("--max-workers", type=int, default=4, help="parallel copies (default: %(default)s)")
    parser.add_argument("--limit", type=int, help="dump only that many files, e.g. for a first test")
    parser.add_argument("--dry-run", action="store_true", help="dump: only list what would be copied")
    parser.add_argument("--skip-verify", action="store_true",
                        help="updatedb / removeblobs: do not check the copy in the target before changing anything")
    args = parser.parse_args(argv)

    if args.source == "filesystem" and not args.source_dir:
        parser.error("--source-dir is required for --source filesystem")
    if not args.destination:
        parser.error("--destination (the bucket for --target AmazonS3, the directory for --target FileSystem) is required")
    if args.command == "removeblobs" and args.source != "gridfs":
        parser.error("removeblobs is only available for --source gridfs")
    if not args.log_file:
        args.log_file = f"migrate_{args.database}.csv"
    return args


def connect(args):
    from pymongo import MongoClient
    password = args.password or os.environ.get("MONGO_PASSWORD")
    if args.mongo_uri:
        client = MongoClient(args.mongo_uri, retryWrites=False)
    else:
        kwargs = {"host": args.host, "port": args.port, "retryWrites": False}
        if args.user:
            kwargs.update(username=args.user, password=password)
            if args.auth_source:
                kwargs["authSource"] = args.auth_source
        client = MongoClient(**kwargs)
    return client[args.database]


def build(args, db) -> Migrator:
    if args.source == "gridfs":
        source = GridFSSource(db, args.store_name)
    else:
        source = FileSystemSource(args.source_dir, args.store_name)
    if args.target == "AmazonS3":
        target = AmazonS3Target(args.destination, args.s3_endpoint_url, args.s3_region, args.s3_path_style,
                                args.s3_key_prefix)
    else:
        target = FileSystemTarget(args.destination)
    return Migrator(db, source, target, args.log_file, max_workers=args.max_workers, dry_run=args.dry_run,
                    store_name=args.store_name)


def main(argv=None) -> int:
    args = parse_args(argv)
    log_filename = f"migrate_{args.database}_{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.FileHandler(log_filename), logging.StreamHandler(sys.stdout)])
    db = connect(args)
    migrator = build(args, db)
    if args.command == "dump":
        counts = migrator.dump(args.limit)
        return 1 if counts["failed"] else 0
    if args.command == "verify":
        return 1 if migrator.verify()["bad"] else 0
    if args.command == "updatedb":
        counts = migrator.updatedb(verify=not args.skip_verify)
        return 1 if counts.get("failed") or counts.get("bad-copy") else 0
    counts = migrator.removeblobs(verify=not args.skip_verify)
    return 1 if counts.get("failed") or counts.get("bad-copy") else 0


if __name__ == "__main__":
    sys.exit(main())
