use async_trait::async_trait;
use aws_sdk_s3::Client;
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request,
};
use libc::ENOENT;
use std::ffi::OsStr;
use std::time::{Duration, UNIX_EPOCH};

const TTL: Duration = Duration::from_secs(1); // 1 second

const HELLO_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
    blksize: 512,
};

const HELLO_TXT_CONTENT: &str = "Hello World!\n";

const HELLO_TXT_ATTR: FileAttr = FileAttr {
    ino: 2,
    size: 13,
    blocks: 1,
    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::RegularFile,
    perm: 0o644,
    nlink: 1,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
    blksize: 512,
};

pub struct MFS {
    client: Client,
    bucket_name: String,
}

struct DirEntry {
    full_key: String,
    name: String,
    children: Vec<FileAttr>
}

impl MFS {
    pub fn new(client: Client, bucket_name: String) -> MFS {
        MFS {
            client,
            bucket_name,
        }
    }
}

#[async_trait]
impl Filesystem for MFS {
    async fn lookup(&self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent == 1 && name.to_str() == Some("test") {
            reply.entry(&TTL, &HELLO_TXT_ATTR, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    async fn getattr(&self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match ino {
            1 => reply.attr(&TTL, &HELLO_DIR_ATTR),
            2 => reply.attr(&TTL, &HELLO_TXT_ATTR),
            _ => reply.error(ENOENT),
        }
    }

    async fn read(
        &self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        if ino == 2 {
            reply.data(&HELLO_TXT_CONTENT.as_bytes()[offset as usize..]);
        } else {
            reply.error(ENOENT);
        }
    }

    async fn readdir(
        &self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != 1 {
            reply.error(ENOENT);
            return;
        }

        let client = &self.client;
        let bucket_name = &self.bucket_name;
        let prefix = "";
        let prefix_len = prefix.len();
        let mut continuation_token:Option<String> = None;

        loop {
            println!("continuation token is {:?}", &continuation_token);
            let mut list_object = client.list_objects_v2().bucket(bucket_name).prefix(prefix);
            if let Some(token) = &continuation_token {
                list_object= list_object.continuation_token(token);
            }
            let objects = list_object.send().await.unwrap();

            for obj in objects.contents().unwrap_or_default() {
                let full_key = obj.key().unwrap();
                // println!("full_key:{:?}", &full_key);
                let mut key = full_key.clone();
                key = &key[prefix_len..];

                if key == "" || key.contains("/") {
                    // this key is itself or a sub directory
                    continue;
                }
                println!("{:?}", key);
            }

            if let Some(next_token) = objects.next_continuation_token() {
                continuation_token = Some(String::from(next_token));
            } else {
                break;
            }
        }

        let entries = vec![
            (1, FileType::Directory, "."),
            (1, FileType::Directory, ".."),
            (2, FileType::RegularFile, "test"),
        ];

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // i + 1 means the index of the next entry
            if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
                break;
            }
        }
        reply.ok();
    }
}
