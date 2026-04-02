mod app;
mod cmd;
mod error;

fn main() -> std::process::ExitCode {
    match app::run(std::env::args_os()) {
        Ok(output) => {
            match output {
                app::CliOutput::Text(text) => {
                    if !text.is_empty() {
                        if text.ends_with('\n') {
                            print!("{text}");
                        } else {
                            println!("{text}");
                        }
                    }
                }
                app::CliOutput::Bytes(bytes) => {
                    use std::io::Write;

                    if !bytes.is_empty() {
                        let mut stdout = std::io::stdout().lock();
                        if let Err(error) = stdout.write_all(&bytes) {
                            eprintln!("failed to write stdout bytes: {error}");
                            return std::process::ExitCode::from(1);
                        }
                        if let Err(error) = stdout.flush() {
                            eprintln!("failed to flush stdout bytes: {error}");
                            return std::process::ExitCode::from(1);
                        }
                    }
                }
            }
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            let rendered = error.render();
            if error.use_stderr() {
                if rendered.ends_with('\n') {
                    eprint!("{rendered}");
                } else {
                    eprintln!("{rendered}");
                }
            } else if rendered.ends_with('\n') {
                print!("{rendered}");
            } else {
                println!("{rendered}");
            }
            std::process::ExitCode::from(error.exit_code())
        }
    }
}
