use clap::{CommandFactory, Parser, ValueHint};
use clap_complete::Shell;
use loonfs_cli::Cli;

#[test]
fn completions_command_parses() {
    assert!(Cli::try_parse_from(["loonfs", "completions", "zsh"]).is_ok());
}

#[test]
fn completions_generate_for_zsh_and_bash() {
    for shell in [Shell::Zsh, Shell::Bash] {
        let mut script = Vec::new();
        clap_complete::generate(shell, &mut Cli::command(), "loonfs", &mut script);

        assert!(!script.is_empty());
        assert!(String::from_utf8_lossy(&script).contains("loonfs"));
    }
}

#[test]
fn completions_use_local_and_remote_path_hints() {
    let command = Cli::command();

    assert_hint(&command, "config", ValueHint::FilePath);

    let put = subcommand(&command, "put");
    assert_hint(put, "local_path", ValueHint::AnyPath);
    assert_hint(put, "remote_path", ValueHint::Other);

    let get = subcommand(&command, "get");
    assert_hint(get, "local_destination", ValueHint::AnyPath);
    assert_hint(get, "remote_path", ValueHint::Other);

    let cat = subcommand(&command, "cat");
    assert_hint(cat, "path", ValueHint::Other);

    let mv = subcommand(&command, "mv");
    assert_hint(mv, "source_path", ValueHint::Other);
    assert_hint(mv, "destination_path", ValueHint::Other);
}

fn subcommand<'a>(command: &'a clap::Command, name: &str) -> &'a clap::Command {
    command
        .get_subcommands()
        .find(|subcommand| subcommand.get_name() == name)
        .unwrap_or_else(|| panic!("missing {name} subcommand"))
}

fn assert_hint(command: &clap::Command, id: &str, expected: ValueHint) {
    let argument = command
        .get_arguments()
        .find(|argument| argument.get_id() == id)
        .unwrap_or_else(|| panic!("missing {id} argument"));
    assert_eq!(argument.get_value_hint(), expected);
}
