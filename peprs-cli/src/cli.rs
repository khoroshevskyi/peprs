use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(clap::ValueEnum, Clone)]
pub enum ConvertFormat {
    /// WDL (Workflow Description Language) format
    Wdl,
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

        /// Sample table index to use (default: "sample_name")
        #[arg(long = "st-index")]
        st_index: Option<String>,

        /// Subsample table index to use
        #[arg(long = "sst-index")]
        sst_index: Option<String>,

        /// Names of the amendments to activate
        #[arg(long = "amendments", num_args = 1..)]
        amendments: Option<Vec<String>>,
    },

    /// Validate a project against an eido schema
    Validate {
        /// Path to the project configuration yaml file.
        path: String,

        /// Path to the eido schema file (YAML or JSON).
        #[arg(short = 's', long = "schema")]
        schema: String,
    },

    /// Convert samples into an input format for pipeline
    Convert {
        /// Path to the project configuration yaml file.
        path: String,

        /// WDL workflow file for schema parsing
        schema: String,

        /// Format to convert to. Formats currently supported: ["wdl"]
        #[arg(value_enum, short = 'f', long = "to")]
        format: ConvertFormat,

        /// Task or workflow entrypoint in the WDL file
        #[arg(short = 'n', long = "name")]
        name: Option<String>,

        /// Include nested inputs in the conversion
        #[arg(long = "nested-inputs")]
        nested_inputs: Option<bool>,

        /// Show non-literal expressions in the output
        #[arg(long = "show-non-literals")]
        show_non_literals: Option<bool>,

        /// Hide default values from the output
        #[arg(long = "hide-defaults")]
        hide_defaults: Option<bool>,
    },
}
