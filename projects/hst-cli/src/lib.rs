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

use clap::ArgAction;
use simplelog::LevelFilter;

fn select_log_level_filter(verbosity: u8) -> LevelFilter {
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
    #[clap(long, short = 'v', global = true, action = ArgAction::Count)]
    verbose: u8,
}

impl Verbosity {
    pub fn new(verbose: u8) -> Self {
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
            builder, error, value_parser, Arg, ArgAction, ArgGroup, ArgMatches, Args, Command,
            CommandFactory, Error, FromArgMatches, Id, Parser, Subcommand,
        };
    }
    pub mod log {
        pub use log::{error, info, warn, SetLoggerError};
    }
}
