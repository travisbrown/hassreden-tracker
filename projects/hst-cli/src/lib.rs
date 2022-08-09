//! Opinionated helpers for building consistent command-line interfaces with [`clap`][clap] and [`simplelog`][simplelog].
//!
//! ## Example
//!
//! The [`prelude`] module exports a minimal subset of these two crates.
//!
//! ```rust,no_run
//! use hst_cli::prelude::*;
//!
//! #[derive(Debug, Parser)]
//! #[clap(name = "demo", version, author)]
//! struct Opts {
//!     #[clap(flatten)]
//!     verbose: Verbosity,
//! }
//!
//! fn main() -> Result<(), log::SetLoggerError> {
//!     let opts: Opts = Opts::parse();
//!     opts.verbose.init_logging()?;
//!     Ok(())
//! }
//! ```
//!
//! [clap]: https://docs.rs/clap/latest/clap/
//! [simplelog]: https://docs.rs/simplelog/latest/simplelog/

use simplelog::LevelFilter;

fn select_log_level_filter(verbosity: i8) -> LevelFilter {
    match verbosity {
        0 => LevelFilter::Off,
        1 => LevelFilter::Error,
        2 => LevelFilter::Warn,
        3 => LevelFilter::Info,
        4 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    }
}

#[derive(clap::Args, Debug, Clone)]
pub struct Verbosity {
    /// Level of verbosity
    #[clap(long, short = 'v', parse(from_occurrences), global = true)]
    verbose: i8,
}

impl Verbosity {
    pub fn new(verbose: i8) -> Self {
        Self { verbose }
    }

    /// Initialize a default terminal logger with the indicated log level.
    pub fn init_logging(&self) -> Result<(), log::SetLoggerError> {
        simplelog::TermLogger::init(
            select_log_level_filter(self.verbose),
            simplelog::Config::default(),
            simplelog::TerminalMode::Stderr,
            simplelog::ColorChoice::Auto,
        )
    }
}

pub mod prelude {
    pub use super::Verbosity;
    pub use ::clap::Parser;
    pub mod clap {
        pub use clap::{
            builder, AppSettings, Arg, ArgAction, ArgMatches, Args, Command, CommandFactory, Error,
            ErrorKind, FromArgMatches, Parser, Subcommand,
        };
    }
    pub mod log {
        pub use log::{error, info, warn, SetLoggerError};
    }
}
