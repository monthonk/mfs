mod fs;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3 as s3;
use fs::MFS;
use fuser::MountOption;
use s3::Region;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), s3::Error> {
    let mountpoint = "mnt";
    let bucket_name = String::from("s3-file-connector-github-test-bucket");
    let region = "us-east-1";

    let mountpoint = PathBuf::from(mountpoint);
    let mut options = vec![MountOption::RO, MountOption::FSName("mfs".to_string())];
    options.push(MountOption::AutoUnmount);

    let region_provider = RegionProviderChain::first_try(Region::new(region))
        .or_default_provider()
        .or_else(Region::new("us-east-1"));
    dbg!(&region_provider);

    let config = aws_config::from_env().region(region_provider).load().await;
    let client = s3::Client::new(&config);
    let fs = MFS::new(client, bucket_name);

    fuser::mount2(fs, mountpoint, &options).unwrap();
    Ok(())
}
