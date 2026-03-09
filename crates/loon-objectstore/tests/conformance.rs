use loon_objectstore::provider::{AWS_S3, CLOUDFLARE_R2, LOCAL_FS};

#[test]
fn provider_profiles_exist() {
    assert_eq!(LOCAL_FS.name, "local-fs");
    assert_eq!(AWS_S3.name, "aws-s3");
    assert_eq!(CLOUDFLARE_R2.name, "cloudflare-r2");
}
