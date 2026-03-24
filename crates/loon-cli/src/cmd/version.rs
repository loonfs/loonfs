pub(crate) fn render_version() -> String {
    format!("loon {}\n", env!("CARGO_PKG_VERSION"))
}
