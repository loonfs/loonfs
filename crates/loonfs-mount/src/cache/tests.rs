use super::*;
use loonfs_types::{ChecksumAlgorithm, NamespaceId};

fn content(bytes: &[u8]) -> ContentRef {
    ContentRef::blob_v1(
        NamespaceId::parse("demo").expect("a namespace ID"),
        ContentId::generate(),
        bytes,
        ChecksumAlgorithm::Crc64nvme,
    )
}

fn order(cache: &Cache) -> Vec<(ContentId, u64)> {
    cache
        .least_recently_used_first()
        .map(|(content_id, stored_bytes)| (content_id.clone(), stored_bytes))
        .collect()
}

/// Checks that the cache keeps the total of its stored bytes as files fill,
/// fail, and leave, and lists its files least recently used first, so status
/// and eviction don't walk every file.
#[test]
fn the_cache_keeps_its_stored_bytes_and_its_order_of_use() {
    let directory = tempfile::tempdir().expect("a cache directory");
    let (mut cache, missing) = Cache::load(directory.path(), Vec::new()).expect("a cache");
    assert!(missing.is_empty());
    let a = content(b"aaaa");
    let b = content(b"bbbbbbbb");
    cache.prepare(&a).expect("a partial file");
    cache.prepare(&b).expect("a partial file");
    assert_eq!(cache.stored_bytes(), 0);
    cache.record_present(&a.content_id, 0..4);
    cache.record_present(&b.content_id, 0..3);
    cache.record_present(&b.content_id, 2..5);
    assert_eq!(cache.stored_bytes(), 9);
    assert_eq!(
        order(&cache),
        [(a.content_id.clone(), 4), (b.content_id.clone(), 5)]
    );

    cache.touch(&a.content_id);
    assert_eq!(
        order(&cache),
        [(b.content_id.clone(), 5), (a.content_id.clone(), 4)]
    );
    cache.fail(&b.content_id);
    assert_eq!(cache.stored_bytes(), 4);
    cache.remove(&b.content_id).expect("a removal");
    cache.complete(&a.content_id).expect("a complete file");
    assert_eq!(cache.stored_bytes(), 4);
    assert_eq!(order(&cache), [(a.content_id.clone(), 4)]);
    cache.remove(&a.content_id).expect("a removal");
    assert_eq!(cache.stored_bytes(), 0);
    assert!(order(&cache).is_empty());
}
