mod cli;

use clap::Parser;
use peprs_core::{project::Project, sample};

use crate::cli::{Cli, Commands};

fn main() {
    let cli = Cli::parse();
    
    match &cli.command {
        Commands::Inspect { path, name } => {
            let proj = Project::from_config(path).build();
            match proj {
                Ok(proj) => {
                    if let Some(name) = name {
                        let sample = proj.get_sample(name);
                        match sample {
                            Ok(sample) => {
                                if let Some(sample) = sample {
                                    for (k, v) in sample.into_map() {
                                        println!("{}: {}", k, v);
                                    }
                                } else {
                                    eprintln!("Could not find sample with name '{}'", name);
                                }
                            },
                            Err(error) => {
                                eprintln!("There was an unexpected error retreiving the sample: {}", error);
                            }
                        }
                    } else {
                        let pep_version = proj.get_pep_version();
                        let samples = proj.samples.height();
                        let attributes = proj.samples.width();
                        println!("Using PEP version: {}", pep_version);
                        println!("Number of samples: {}", samples);
                        println!("Number of attributes: {}", attributes);
                        println!("{}", proj.samples);
                    }
                },
                Err(err) => {
                    let msg = format!("Error parsing PEP: {}", err);
                    eprintln!("{}", msg);
                }
            }
        },
        Commands::Validate { path } => todo!(),
    }
}
