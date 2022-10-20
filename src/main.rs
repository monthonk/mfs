mod fs;

use aws_sdk_s3 as s3;
use fs::MFS;
use fuser::MountOption;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), s3::Error> {
    let mountpoint = "mnt";
    let mountpoint = PathBuf::from(mountpoint);
    let mut options = vec![MountOption::RO, MountOption::FSName("mfs".to_string())];
    options.push(MountOption::AutoUnmount);

    let config = aws_config::load_from_env().await;
    let _ = s3::Client::new(&config);

    fuser::mount2(MFS, mountpoint, &options).unwrap();
    Ok(())
}
