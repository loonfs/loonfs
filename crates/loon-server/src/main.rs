fn main() -> std::process::ExitCode {
    eprintln!(
        "loond is intentionally unavailable during the semantic-core reset.\n\
         The active server-side surface today is `loon_server::mutation`, not a runnable binary or HTTP shell."
    );
    std::process::ExitCode::FAILURE
}
