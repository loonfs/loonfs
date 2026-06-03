pub(crate) fn sync_phase<T>(phase: &'static str, f: impl FnOnce() -> T) -> T {
    tracing::info_span!("loon.phase", phase).in_scope(f)
}

pub(crate) fn sync_phase_with_key_class<T>(
    phase: &'static str,
    key_class: &'static str,
    f: impl FnOnce() -> T,
) -> T {
    tracing::info_span!("loon.phase", phase, key_class).in_scope(f)
}
