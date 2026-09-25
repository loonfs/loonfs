//! Embedded requests without networking or bearer credentials.

use crate::config::StoreConfig;
use crate::resolve::ResolvedTarget;
use bytes::Bytes;
use futures::StreamExt as _;
use loonfs_api::NamespaceAccess;
use loonfs_client::{NamespacePath, PayloadSource, PutFileOptions, ReadFileOptions};

#[test]
fn embedded_requests_need_no_socket_or_token_and_stream_past_the_server_body_limit() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime without a networking driver");
    runtime.block_on(async {
        let directory = tempfile::tempdir().expect("store directory");
        let target = ResolvedTarget::embedded(
            &StoreConfig::LocalFs {
                root: directory.path().display().to_string(),
                key_prefix: None,
            },
            None,
            false,
        )
        .await
        .expect("embedded profile without credentials");
        let path = NamespacePath::parse("demo", "/large.bin").expect("path");
        let actor = loonfs_test_support::test_actor();
        target
            .client
            .create_namespace(path.namespace(), &actor, NamespaceAccess::unrestricted())
            .await
            .expect("namespace without a listener");
        let chunk = Bytes::from(vec![42; 1024 * 1024]);
        let source = PayloadSource::stream(
            futures::stream::iter((0..257).map(move |_| Ok(chunk.clone()))).boxed(),
        );
        target
            .client
            .put_file_stream(&path, source, &PutFileOptions::new(actor))
            .await
            .expect("upload past 256 MiB");
        let mut stream = target
            .client
            .read_file_stream(&path, &ReadFileOptions::default())
            .await
            .expect("download past 256 MiB");
        let mut size_bytes = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("verified content");
            assert!(chunk.iter().all(|byte| *byte == 42));
            size_bytes += chunk.len();
        }
        assert_eq!(size_bytes, 257 * 1024 * 1024);
    });
}
