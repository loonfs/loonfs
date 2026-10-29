//! Memory sizing under a limit, a cgroup limit, and no limit.

use super::{MemorySource, MIB};
use crate::config::tests::assert_invalid_field;
use crate::config::{JobMode, ServerConfig};
use std::fs;
use tempfile::tempdir;

fn memory_config(settings: &str) -> ServerConfig {
    toml::from_str(&format!(
        r#"
bind = "127.0.0.1:9400"
writer_id = "memory-test"
content_token_secret = "test-secret"
{settings}
[store]
kind = "local_fs"
root = "unused"
"#
    ))
    .expect("parse memory config")
}

#[test]
fn four_gib_derives_each_pool_and_folds_unused_grep_into_segments() {
    let mut config = memory_config("memory_limit_bytes = 4294967296");
    for mode in [JobMode::ServeOnly, JobMode::Disabled, JobMode::MaintainOnly] {
        config.grep.mode = mode;
        let memory = config.memory_sizing(Some(1)).expect("explicit limit wins");
        assert_eq!(
            memory.fixed.map(|(_, bytes)| bytes),
            [
                134_217_728,
                494_927_872,
                201_326_592,
                134_217_728,
                536_870_912,
                33_554_432,
                0
            ]
        );
        assert_eq!(memory.headroom, 719_742_566);
        assert_eq!(
            (memory.head.bytes, memory.head.source),
            (16_777_216, MemorySource::Default),
            "the head budget isn't derived"
        );
        assert_eq!(
            (memory.manifest.bytes, memory.manifest.source),
            (33_554_432, MemorySource::Default),
            "the manifest budget isn't derived"
        );
        assert_eq!(memory.managed, 1_989_777_818);
        assert_eq!(
            memory.segment.bytes,
            if mode.serves() {
                596_933_345
            } else {
                795_911_127
            }
        );
        assert_eq!(memory.wal_tail.bytes, 198_977_781);
        assert_eq!(memory.content_object.bytes, 198_977_781);
        assert_eq!(
            memory.grep.bytes,
            if mode.serves() { 198_977_781 } else { 0 }
        );
        assert_eq!(memory.read_working.bytes, 397_955_563);
        assert_eq!(memory.publication.bytes, 198_977_781);
        assert_eq!(memory.merge_input.bytes, 99_488_890);
        for (term, _) in memory.terms() {
            if ![memory.head.name, memory.manifest.name].contains(&term.name) {
                assert_eq!(term.source, MemorySource::Derived);
            }
        }
    }
    assert_eq!(config.max_concurrent_reads.get(), 64);
    assert_eq!(config.max_concurrent_folds.get(), 2);
    assert_eq!(config.max_concurrent_compactions.get(), 2);
    assert_eq!(config.max_concurrent_content_writes.get(), 32);
    assert_eq!(config.max_in_flight_requests.get(), 256);
    assert_eq!(config.max_connections.get(), 1024);
    assert_eq!(config.max_concurrent_uploads.get(), 8);
    assert_eq!(config.max_concurrent_downloads.get(), 16);
    assert_eq!(config.publication.max_concurrent_publications, None);
}

#[test]
fn explicit_memory_limits_survive_derivation_and_still_must_fit() {
    let mut config = memory_config(
        r#"
memory_limit_bytes = 4294967296
[metadata_cache]
max_segment_bytes = 67108864
"#,
    );
    let memory = config
        .clone()
        .validate()
        .expect("smaller segment cache fits")
        .memory;
    assert_eq!(memory.segment.bytes, 67_108_864);
    assert_eq!(memory.segment.source, MemorySource::Set);
    assert_eq!(memory.wal_tail.bytes, 198_977_781);
    config.metadata_cache.max_segment_bytes = Some(4 * 1024 * MIB);
    let error = config.validate().expect_err("overrides must fit");
    assert!(error
        .to_string()
        .contains("`metadata_cache.max_segment_bytes` = 4294967296"));
    assert_invalid_field(error, "memory_limit_bytes");

    let mut config = memory_config(
        r#"
memory_limit_bytes = 4294967296
max_connections = 512
max_concurrent_uploads = 4
max_concurrent_downloads = 4
max_concurrent_compactions = 4
max_concurrent_content_writes = 8
max_read_working_bytes = 0
max_merge_input_bytes = 67108864
[metadata_cache]
max_segment_bytes = 67108864
max_manifest_bytes = 4194304
max_head_bytes = 8388608
max_wal_tail_bytes = 0
max_content_object_bytes = 12582912
[publication]
max_estimated_bytes = 8388608
[grep]
mode = "serve_only"
max_cache_bytes = 0
"#,
    );
    let memory = config
        .clone()
        .validate()
        .expect("explicit pools fit")
        .memory;
    assert_eq!(
        memory.fixed.map(|(_, bytes)| bytes),
        [
            134_217_728,
            247_463_936,
            67_108_864,
            201_326_592,
            134_217_728,
            33_554_432,
            0
        ]
    );
    assert_eq!(
        memory.terms().map(|(term, count)| (term.bytes, count)),
        [
            (67_108_864, 1),
            (4_194_304, 1),
            (8_388_608, 1),
            (0, 1),
            (12_582_912, 1),
            (0, 1),
            (0, 1),
            (8_388_608, 1),
            (67_108_864, 4)
        ]
    );
    assert!(memory
        .terms()
        .iter()
        .all(|(term, _)| term.source == MemorySource::Set));
    config.max_merge_input_bytes = Some(1024 * MIB);
    assert_invalid_field(
        config.validate().expect_err("all four merges must fit"),
        "memory_limit_bytes",
    );
}

/// The terms that the error for a failed memory check lists.
const EVERY_MEMORY_TERM: [&str; 23] = [
    "memory_limit_bytes",
    "process",
    "max_connections",
    "max_concurrent_uploads",
    "max_concurrent_downloads",
    "max_concurrent_folds",
    "max_concurrent_compactions",
    "max_concurrent_content_writes",
    "completed upload sessions",
    "metadata_cache.ssd.max_segment_bytes",
    "headroom",
    "managed",
    "268435456",
    "metadata_cache.max_segment_bytes",
    "metadata_cache.max_manifest_bytes",
    "metadata_cache.max_head_bytes",
    "metadata_cache.max_wal_tail_bytes",
    "metadata_cache.max_content_object_bytes",
    "grep.max_cache_bytes",
    "max_read_working_bytes",
    "publication.max_estimated_bytes",
    "max_merge_input_bytes",
    "total",
];

#[test]
fn insufficient_memory_names_every_term_before_startup() {
    for limit in [512 * MIB, 1024 * MIB] {
        let config = memory_config(&format!("memory_limit_bytes = {limit}"));
        let error = config
            .validate()
            .expect_err("managed memory is below 256 MiB");
        let message = error.to_string();
        for term in EVERY_MEMORY_TERM {
            assert!(message.contains(term), "missing {term}: {message}");
        }
        assert_invalid_field(error, "memory_limit_bytes");
    }
}

#[test]
fn two_gib_fits_the_default_counts() {
    let memory = memory_config("memory_limit_bytes = 2147483648")
        .validate()
        .expect("2 GiB fits the default counts")
        .memory;
    assert_eq!(memory.headroom, 290_245_836);
    assert_eq!(memory.managed, 271_790_900);
}

#[test]
fn content_writes_reserve_one_part_and_one_read_each_and_72_mib_on_r2() {
    let local = memory_config("max_concurrent_content_writes = 4");
    let mut r2 = local.clone();
    r2.store = toml::from_str(
        r#"
kind = "cloudflare_r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"

[credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
    )
    .expect("parse an R2 store");
    for (config, write_bytes) in [(local, 16 * MIB), (r2, 72 * MIB)] {
        let memory = config.memory_sizing(None).expect("unsized config");
        let (name, bytes) = memory.fixed[4];
        assert!(name.contains("max_concurrent_content_writes"), "{name}");
        assert_eq!(bytes, 4 * write_bytes as u128);
    }
}

#[test]
fn no_limit_or_cgroup_file_keeps_the_unsized_defaults() {
    let directory = tempdir().expect("tempdir");
    let missing = directory.path().join("memory.max");
    let config = memory_config("");
    let memory = config
        .memory_sizing(super::read_cgroup_memory_limit(&[&missing]))
        .expect("unsized config");
    assert!(memory.limit.is_none());
    assert_eq!(memory.segment.bytes, 268_435_456);
    assert_eq!(memory.manifest.bytes, 33_554_432);
    assert_eq!(memory.head.bytes, 16_777_216);
    assert_eq!(memory.wal_tail.bytes, 67_108_864);
    assert_eq!(memory.content_object.bytes, 100_663_296);
    assert_eq!(memory.grep.bytes, 268_435_456);
    assert_eq!(memory.read_working.bytes, 268_435_456);
    assert_eq!(memory.publication.bytes, 67_108_864);
    assert_eq!(memory.merge_input.bytes, 67_108_864);
    assert!(memory
        .terms()
        .iter()
        .all(|(term, _)| term.source == MemorySource::Default));
}

#[test]
fn cgroup_limits_accept_finite_numbers_and_skip_absent_or_unlimited_files() {
    let directory = tempdir().expect("tempdir");
    let v2 = directory.path().join("memory.max");
    let v1 = directory.path().join("memory.limit_in_bytes");
    assert_eq!(super::read_cgroup_memory_limit(&[&v2, &v1]), None);
    fs::write(&v1, "2147483648\n").expect("write v1 limit");
    for value in [
        "max\n",
        "9223372036854771712",
        "1152921504606846976",
        "invalid",
    ] {
        fs::write(&v2, value).expect("write v2 limit");
        assert_eq!(super::read_cgroup_memory_limit(&[&v2]), None);
        assert_eq!(
            super::read_cgroup_memory_limit(&[&v2, &v1]),
            Some(2_147_483_648)
        );
    }
    fs::write(&v2, "4294967296\n").expect("write finite v2 limit");
    let limit = super::read_cgroup_memory_limit(&[&v2, &v1]);
    assert_eq!(limit, Some(4_294_967_296));
    let memory = memory_config("")
        .memory_sizing(limit)
        .expect("cgroup sizes config");
    assert_eq!(memory.managed, 1_989_777_818);
    assert_eq!(
        memory.limit.expect("cgroup limit").source,
        MemorySource::Derived
    );
}

/// Checks that the SSD tier adds its write queue and the estimated
/// memory of its index to `fixed`, which the pools are sized after.
#[test]
fn the_ssd_tier_adds_its_write_queue_and_index_to_fixed() {
    let without = memory_config("memory_limit_bytes = 8589934592");
    let with = memory_config(
        "memory_limit_bytes = 8589934592\n[metadata_cache.ssd]\ndirectory = \"/ssd\"\nmax_segment_bytes = 10737418240",
    );
    let without = without.memory_sizing(None).expect("sized without a tier");
    let with = with.memory_sizing(None).expect("sized with a tier");
    let (name, bytes) = with.fixed[6];
    assert!(
        name.contains("metadata_cache.ssd.max_segment_bytes"),
        "{name}"
    );
    // 64 MiB of queue, and 10 GiB / 4 KiB = 2,621,440 entries of 168 B.
    assert_eq!(bytes, 67_108_864 + 2_621_440 * 168);
    assert_eq!(without.fixed[6].1, 0);
    assert!(with.managed < without.managed);
}
