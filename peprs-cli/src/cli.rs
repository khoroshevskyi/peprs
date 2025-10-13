use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Inspect project
    Inspect {
        /// Path to the project configuration yaml file.
        path: String,
        /// Optional name parameter
        #[arg(short = 'n', long = "sample-name")]
        name: Option<String>,
    },

    /// Validate a project, ensuring it meets the required schema
    Validate {
        /// Path to the project configuration yaml file.
        path: String,
    },
}
