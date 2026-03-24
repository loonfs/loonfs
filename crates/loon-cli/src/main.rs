mod app;
mod cmd;
mod error;

fn main() -> std::process::ExitCode {
    match app::run(std::env::args_os()) {
        Ok(output) => {
            if !output.is_empty() {
                if output.ends_with('\n') {
                    print!("{output}");
                } else {
                    println!("{output}");
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
