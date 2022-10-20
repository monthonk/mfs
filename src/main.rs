mod fs;

use fs::MFS;
use fuser::MountOption;
use std::path::PathBuf;

fn main() {
    let mountpoint = "mnt";
    let mountpoint = PathBuf::from(mountpoint);
    let mut options = vec![MountOption::RO, MountOption::FSName("hello".to_string())];
    options.push(MountOption::AutoUnmount);

    fuser::mount2(MFS, mountpoint, &options).unwrap();
}
