use async_trait::async_trait;
use aws_sdk_s3::model::Object;
use aws_sdk_s3::Client;
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request,
};
use libc::ENOENT;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
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

pub struct MFS {
    client: Client,
    bucket_name: String,
    ino_map: RwLock<HashMap<u64, Inode>>,
    next_ino: AtomicU64,
}

#[derive(Debug)]
struct Inode {
    parent: u64,
    ino: u64,
    name: String,
    file_attr: FileAttr,
}

impl Inode {
    pub fn new(parent: u64, ino: u64, name: String, object: &Object, kind: FileType) -> Inode {
        Inode {
            parent,
            ino,
            name,
            file_attr: FileAttr {
                ino,
                size: object.size() as u64,
                blocks: 1,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: kind,
                perm: 0o644,
                nlink: 1,
                uid: 501,
                gid: 20,
                rdev: 0,
                blksize: 512,
                flags: 0,
            },
        }
    }
}

impl MFS {
    pub fn new(client: Client, bucket_name: String) -> MFS {
        let root_ino = 1;
        let mut ino_map: HashMap<u64, Inode> = HashMap::new();
        ino_map.insert(
            root_ino,
            Inode {
                parent: 0,
                ino: root_ino,
                name: String::from(""),
                file_attr: HELLO_DIR_ATTR,
            },
        );

        MFS {
            client,
            bucket_name,
            ino_map: RwLock::new(ino_map),
            next_ino: AtomicU64::new(root_ino + 1),
        }
    }

    pub fn next_ino(&self) -> u64 {
        return self.next_ino.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Filesystem for MFS {
    async fn lookup(&self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        println!("lookup parent={parent} name={:?}", name);
        let ino_map_reader = self.ino_map.read().unwrap();
        for (_, inode) in ino_map_reader.iter() {
            dbg!(&inode);
            if parent == inode.parent && name.to_str() == Some(&inode.name) {
                reply.entry(&TTL, &inode.file_attr, 0);
                return;
            }
        }
        reply.error(ENOENT);
    }

    async fn getattr(&self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        println!("getattr ino={ino}");
        let ino_map_reader = self.ino_map.read().unwrap();
        let inode = ino_map_reader.get(&ino);
        match inode {
            Some(node) => reply.attr(&TTL, &node.file_attr),
            None => reply.error(ENOENT),
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
        println!("readdir ino={ino} offset={offset}");
        if ino != 1 {
            reply.error(ENOENT);
            return;
        }

        let client = &self.client;
        let bucket_name = &self.bucket_name;
        let prefix = "";
        let prefix_len = prefix.len();
        let mut continuation_token: Option<String> = None;

        let mut entries: Vec<Inode> = Vec::new();
        loop {
            let mut list_object = client.list_objects_v2().bucket(bucket_name).prefix(prefix);
            if let Some(token) = &continuation_token {
                list_object = list_object.continuation_token(token);
            }
            let objects = list_object.send().await.unwrap();

            for obj in objects.contents().unwrap_or_default() {
                let full_key = obj.key().unwrap();
                let name = &full_key[prefix_len..];

                if name == "" || name.contains("/") {
                    // this key is itself or a sub directory
                    continue;
                }

                let parent = ino;
                let next_ino = self.next_ino();
                let mut ino_map_writer = self.ino_map.write().unwrap();
                ino_map_writer.insert(next_ino, Inode::new(parent, next_ino, String::from(name), obj, FileType::RegularFile));

                // FIXME: use inodes from MFS
                entries.push(Inode::new(parent, next_ino, String::from(name), obj, FileType::RegularFile));
            }

            if let Some(next_token) = objects.next_continuation_token() {
                continuation_token = Some(String::from(next_token));
            } else {
                break;
            }
        }

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // i + 1 means the index of the next entry
            if reply.add(entry.ino, (i + 1) as i64, entry.file_attr.kind, entry.name) {
                break;
            }
        }
        reply.ok();
    }
}
