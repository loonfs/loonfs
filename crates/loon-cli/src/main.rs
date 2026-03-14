fn main() -> std::process::ExitCode {
    eprintln!(
        "loon-cli is intentionally unavailable during the semantic-core reset.\n\
         Active surfaces today are library crates, scenario fixtures, and `cargo run -p xtask -- ...`."
    );
    std::process::ExitCode::FAILURE
}
