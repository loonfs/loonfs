#[derive(Debug)]
pub enum CliError {
    Parse(clap::Error),
    Runtime(anyhow::Error),
}

impl CliError {
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Parse(error) => error
                .exit_code()
                .try_into()
                .expect("clap exit code should fit in u8"),
            Self::Runtime(_) => 1,
        }
    }

    pub fn use_stderr(&self) -> bool {
        match self {
            Self::Parse(error) => error.use_stderr(),
            Self::Runtime(_) => true,
        }
    }

    pub fn render(&self) -> String {
        match self {
            Self::Parse(error) => error.to_string(),
            Self::Runtime(error) => error.to_string(),
        }
    }
}

impl From<clap::Error> for CliError {
    fn from(value: clap::Error) -> Self {
        Self::Parse(value)
    }
}

impl From<anyhow::Error> for CliError {
    fn from(value: anyhow::Error) -> Self {
        Self::Runtime(value)
    }
}

pub type CliResult<T> = Result<T, CliError>;
