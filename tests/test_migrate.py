"""Tests of migrate.py with an in-memory MongoDB (mongomock) and a mocked S3 (moto)

The GridFS source itself is replaced by a fake here, GridFS is covered by the integration test in the README."""
import io
import sys
from pathlib import Path

import boto3
import mongomock
import pytest
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import migrate  # noqa: E402

UNIQUE_ID = "AbCdEfGhIjKlMnOpQ"
BUCKET = "test-bucket"


class FakeSource:
    store = "GridFS:Uploads"

    def __init__(self, files):
        self.files = dict(files)
        self.deleted = []

    def open(self, upload):
        if upload["_id"] not in self.files:
            raise migrate.SourceFileMissing(upload["_id"])
        return io.BytesIO(self.files[upload["_id"]])

    def delete(self, upload_id):
        self.deleted.append(upload_id)
        return self.files.pop(upload_id, None) is not None


def upload_doc(_id, name="file.png", size=None, complete=True, store="GridFS:Uploads", data=b"x"):
    return {
        "_id": _id, "name": name, "size": len(data) if size is None else size, "type": "image/png",
        "rid": "room1", "userId": "user1", "store": store, "complete": complete,
        "path": f"rocketchat/ufs/{store}/{_id}/{name}", "url": f"https://host/rocketchat/ufs/{store}/{_id}/{name}",
    }


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def db():
    database = mongomock.MongoClient().rocketchat
    database.rocketchat_settings.insert_one({"_id": "uniqueID", "value": UNIQUE_ID})
    return database


def make(db, s3, files, tmp_path, **kwargs):
    target = migrate.AmazonS3Target(BUCKET, client=s3)
    return migrate.Migrator(db, FakeSource(files), target, str(tmp_path / "log.csv"), **kwargs)


# -- helpers ---------------------------------------------------------------------------------------------------------

def test_s3_key_is_the_one_rocketchat_uses():
    assert migrate.s3_key("UID", {"_id": "f1", "rid": "r1", "userId": "u1"}) == "UID/uploads/r1/u1/f1"


def test_s3_key_with_prefix():
    assert migrate.s3_key("UID", {"_id": "f1", "rid": "r1", "userId": "u1"}, "/rc/") == "rc/UID/uploads/r1/u1/f1"


@pytest.mark.parametrize("value,expected", [
    ("rocketchat/ufs/GridFS:Uploads/id1/a b.png", "rocketchat/ufs/AmazonS3:Uploads/id1/a b.png"),
    ("/ufs/GridFS:Uploads/id1/a.png", "/ufs/AmazonS3:Uploads/id1/a.png"),
    ("https://lutop.example.org/rocketchat/ufs/GridFS:Uploads/id1/a.png",
     "https://lutop.example.org/rocketchat/ufs/AmazonS3:Uploads/id1/a.png"),
    ("something/else", "something/else"),
    (None, None),
])
def test_swap_store_keeps_host_prefix_and_name(value, expected):
    assert migrate.swap_store(value, "GridFS:Uploads", "AmazonS3:Uploads") == expected


def test_encode_uri_like_javascript():
    assert migrate.encode_uri("a b/ü.png") == "a%20b/%C3%BC.png"


def test_endpoint_region_and_path_style_are_used():
    target = migrate.AmazonS3Target("b", "https://s3.example.org", "eu-central-3", True)
    assert target.client.meta.endpoint_url == "https://s3.example.org"
    assert target.client.meta.region_name == "eu-central-3"
    assert target.client.meta.config.s3["addressing_style"] == "path"


# -- dump ------------------------------------------------------------------------------------------------------------

def test_dump_copies_streams_verifies_and_logs(db, s3, tmp_path):
    db.rocketchat_uploads.insert_many([upload_doc("a", data=b"hello"), upload_doc("b", data=b"world!!")])
    m = make(db, s3, {"a": b"hello", "b": b"world!!"}, tmp_path)
    counts = m.dump()
    assert counts["ok"] == 2 and counts["failed"] == 0
    body = s3.get_object(Bucket=BUCKET, Key=f"{UNIQUE_ID}/uploads/room1/user1/a")
    assert body["Body"].read() == b"hello" and body["ContentType"] == "image/png"
    assert "filename=" in body["ContentDisposition"]
    rows = m.log.rows()
    assert {(r.id, r.size) for r in rows} == {("a", 5), ("b", 7)}


def test_dump_is_resumable_and_does_not_copy_twice(db, s3, tmp_path):
    db.rocketchat_uploads.insert_many([upload_doc("a", data=b"hello")])
    m = make(db, s3, {"a": b"hello"}, tmp_path)
    assert m.dump()["ok"] == 1
    db.rocketchat_uploads.insert_one(upload_doc("b", data=b"more"))
    m.source.files["b"] = b"more"
    counts = m.dump()
    assert counts["ok"] == 1  # only the new one, "a" is in the log
    assert [r.id for r in m.log.rows()] == ["a", "b"]


def test_dump_finds_object_left_by_a_crashed_run(db, s3, tmp_path):
    db.rocketchat_uploads.insert_one(upload_doc("a", data=b"hello"))
    s3.put_object(Bucket=BUCKET, Key=f"{UNIQUE_ID}/uploads/room1/user1/a", Body=b"hello")
    m = make(db, s3, {}, tmp_path)  # the source does not even have it, the target already does
    counts = m.dump()
    assert counts["exists"] == 1 and counts["missing"] == 0
    assert [r.id for r in m.log.rows()] == ["a"]


def test_dump_reports_missing_source_and_skips_incomplete(db, s3, tmp_path):
    db.rocketchat_uploads.insert_many([
        upload_doc("a", data=b"hello"),
        upload_doc("gone", data=b"zzz"),                   # document without file
        upload_doc("half", data=b"q", complete=False),     # incomplete upload
        upload_doc("other", store="FileSystem:Uploads"),   # not our store
    ])
    m = make(db, s3, {"a": b"hello"}, tmp_path)
    counts = m.dump()
    assert counts["ok"] == 1 and counts["missing"] == 1 and counts["failed"] == 0 and counts["incomplete"] == 1
    assert [r.id for r in m.log.rows()] == ["a"]


def test_failed_copy_is_not_logged_as_success(db, s3, tmp_path):
    db.rocketchat_uploads.insert_many([upload_doc("a", data=b"hello"), upload_doc("b", data=b"boom")])
    m = make(db, s3, {"a": b"hello", "b": b"boom"}, tmp_path)
    real_put = m.target.put

    def put(key, stream, upload):
        if upload["_id"] == "b":
            raise RuntimeError("S3 is down")
        real_put(key, stream, upload)
    m.target.put = put
    counts = m.dump()
    assert counts["ok"] == 1 and counts["failed"] == 1
    assert [r.id for r in m.log.rows()] == ["a"]


def test_dry_run_writes_nothing(db, s3, tmp_path):
    db.rocketchat_uploads.insert_one(upload_doc("a", data=b"hello"))
    m = make(db, s3, {"a": b"hello"}, tmp_path, dry_run=True)
    assert m.dump()["dry-run"] == 1
    assert s3.list_objects_v2(Bucket=BUCKET).get("KeyCount", 0) == 0
    assert m.log.rows() == []


def test_limit(db, s3, tmp_path):
    db.rocketchat_uploads.insert_many([upload_doc(i, data=b"x") for i in "abc"])
    m = make(db, s3, {i: b"x" for i in "abc"}, tmp_path)
    assert m.dump(limit=2)["ok"] == 2


# -- verify / updatedb / removeblobs ---------------------------------------------------------------------------------

def migrated(db, s3, tmp_path, files=None):
    files = files or {"a": b"hello"}
    db.rocketchat_uploads.insert_many([upload_doc(i, data=d) for i, d in files.items()])
    m = make(db, s3, files, tmp_path)
    m.dump()
    return m


def test_verify_finds_missing_and_wrong_sized_objects(db, s3, tmp_path):
    m = migrated(db, s3, tmp_path, {"a": b"hello", "b": b"world"})
    assert m.verify()["bad"] == 0
    s3.delete_object(Bucket=BUCKET, Key=f"{UNIQUE_ID}/uploads/room1/user1/a")
    s3.put_object(Bucket=BUCKET, Key=f"{UNIQUE_ID}/uploads/room1/user1/b", Body=b"sh")  # truncated: 2 instead of 5 bytes
    assert m.verify()["bad"] == 2


def test_updatedb_points_upload_to_s3_and_keeps_path_and_url_shape(db, s3, tmp_path):
    m = migrated(db, s3, tmp_path)
    counts = m.updatedb()
    assert counts == {"updated": 1}
    doc = db.rocketchat_uploads.find_one({"_id": "a"})
    assert doc["store"] == "AmazonS3:Uploads"
    assert doc["AmazonS3"] == {"path": f"{UNIQUE_ID}/uploads/room1/user1/a"}
    assert doc["path"] == "rocketchat/ufs/AmazonS3:Uploads/a/file.png"
    assert doc["url"] == "https://host/rocketchat/ufs/AmazonS3:Uploads/a/file.png"


def test_updatedb_is_idempotent(db, s3, tmp_path):
    m = migrated(db, s3, tmp_path)
    m.updatedb()
    assert m.updatedb() == {"already": 1}


def test_updatedb_refuses_a_bad_copy(db, s3, tmp_path):
    m = migrated(db, s3, tmp_path)
    s3.delete_object(Bucket=BUCKET, Key=f"{UNIQUE_ID}/uploads/room1/user1/a")
    assert m.updatedb() == {"bad-copy": 1}
    assert db.rocketchat_uploads.find_one({"_id": "a"})["store"] == "GridFS:Uploads"


def test_removeblobs_only_after_updatedb_and_a_good_copy(db, s3, tmp_path):
    m = migrated(db, s3, tmp_path, {"a": b"hello", "b": b"world"})
    assert m.removeblobs() == {"not-migrated": 2}  # updatedb was not run, nothing may be deleted
    assert m.source.deleted == []
    m.updatedb()
    s3.delete_object(Bucket=BUCKET, Key=f"{UNIQUE_ID}/uploads/room1/user1/b")  # copy of b is gone
    counts = m.removeblobs()
    assert counts == {"removed": 1, "bad-copy": 1}
    assert m.source.deleted == ["a"]


# -- other source / target -------------------------------------------------------------------------------------------

def test_filesystem_source_reads_file_named_by_id(tmp_path):
    (tmp_path / "id1").write_bytes(b"data")
    source = migrate.FileSystemSource(str(tmp_path))
    assert source.store == "FileSystem:Uploads"
    with source.open({"_id": "id1"}) as stream:
        assert stream.read() == b"data"
    with pytest.raises(migrate.SourceFileMissing):
        source.open({"_id": "nope"})


def test_filesystem_source_falls_back_to_unique_file_with_extension(tmp_path):
    (tmp_path / "old1.png").write_bytes(b"png")
    (tmp_path / "old2.png").write_bytes(b"a")
    (tmp_path / "old2.txt").write_bytes(b"b")
    source = migrate.FileSystemSource(str(tmp_path))
    with source.open({"_id": "old1"}) as stream:
        assert stream.read() == b"png"
    with pytest.raises(migrate.SourceFileMissing):  # ambiguous
        source.open({"_id": "old2"})


def test_filesystem_to_s3(db, s3, tmp_path):
    src_dir = tmp_path / "uploads"
    src_dir.mkdir()
    (src_dir / "a").write_bytes(b"hello")
    db.rocketchat_uploads.insert_one(upload_doc("a", data=b"hello", store="FileSystem:Uploads"))
    target = migrate.AmazonS3Target(BUCKET, client=s3)
    m = migrate.Migrator(db, migrate.FileSystemSource(str(src_dir)), target, str(tmp_path / "log.csv"))
    assert m.dump()["ok"] == 1
    assert m.updatedb() == {"updated": 1}
    assert db.rocketchat_uploads.find_one({"_id": "a"})["store"] == "AmazonS3:Uploads"


def test_gridfs_to_filesystem_target(db, tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    db.rocketchat_uploads.insert_one(upload_doc("a", data=b"hello"))
    m = migrate.Migrator(db, FakeSource({"a": b"hello"}), migrate.FileSystemTarget(str(out)), str(tmp_path / "log.csv"))
    assert m.dump()["ok"] == 1
    assert (out / "a").read_bytes() == b"hello"
    assert m.updatedb() == {"updated": 1}
    doc = db.rocketchat_uploads.find_one({"_id": "a"})
    assert doc["store"] == "FileSystem:Uploads" and "AmazonS3" not in doc


# -- command line ----------------------------------------------------------------------------------------------------

def test_args_defaults_and_validation(capsys):
    args = migrate.parse_args(["-c", "dump", "-t", "AmazonS3", "-d", "bucket", "-r", "mydb"])
    assert args.log_file == "migrate_mydb.csv" and args.source == "gridfs" and args.s3_key_prefix == ""
    for bad in (["-c", "dump"], ["-c", "dump", "-d", "x", "--source", "filesystem"],
                ["-c", "removeblobs", "-d", "x", "--source", "filesystem", "--source-dir", "/tmp"]):
        with pytest.raises(SystemExit):
            migrate.parse_args(bad)
