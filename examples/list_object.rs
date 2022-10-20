use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3 as s3;
use aws_sdk_s3::Region;

#[tokio::main]
async fn main() -> Result<(), s3::Error> {
    let bucket_name = String::from("s3-file-connector-github-test-bucket");
    let region = "us-east-1";

    let region_provider = RegionProviderChain::first_try(Region::new(region))
        .or_default_provider()
        .or_else(Region::new("us-east-1"));
    dbg!(&region_provider);

    let config = aws_config::from_env().region(region_provider).load().await;
    let client = s3::Client::new(&config);
    // let fs = MFS::new(client, bucket_name);

    let objects = client.list_objects_v2().bucket(bucket_name).send().await?;
    println!("Objects in bucket:");
    for obj in objects.contents().unwrap_or_default() {
        println!("{:?}", obj.key().unwrap());
    }
    Ok(())
}