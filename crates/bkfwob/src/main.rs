use std::process::ExitCode;

use bkfwob::cli::Cli;
use bkfwob_core::broker::BrokerError;
use bkfwob_core::desk::{broker_error_of, refusal_of};
use clap::Parser;

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            if refusal_of(&error).is_some() {
                ExitCode::from(3)
            } else if matches!(broker_error_of(&error), Some(BrokerError::Ambiguous(_))) {
                ExitCode::from(4)
            } else {
                ExitCode::FAILURE
            }
        }
    }
}
