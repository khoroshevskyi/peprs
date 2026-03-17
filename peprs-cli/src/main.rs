mod cli;

use clap::Parser;
use peprs_core::project::Project;
use peprs_core::wdl::WdlInputParsingOptions;

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
                            }
                            Err(error) => {
                                eprintln!(
                                    "There was an unexpected error retreiving the sample: {}",
                                    error
                                );
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
                }
                Err(err) => {
                    let msg = format!("Error parsing PEP: {}", err);
                    eprintln!("{}", msg);
                }
            }
        }
        Commands::Validate { path, schema } => {
            let proj = Project::from_config(path).build();
            match proj {
                Ok(proj) => match peprs_eido::validate(&proj, schema) {
                    Ok(()) => {
                        println!("Validation successful.");
                    }
                    Err(peprs_eido::error::EidoError::Validation(errors)) => {
                        eprintln!("Validation failed with {} error(s):", errors.len());
                        for err in &errors {
                            eprintln!("  - {}", err);
                        }
                        std::process::exit(1);
                    }
                    Err(peprs_eido::error::EidoError::MissingFiles(missing)) => {
                        eprintln!("Missing required files ({}):", missing.len());
                        for m in &missing {
                            eprintln!("  - {}", m);
                        }
                        std::process::exit(1);
                    }
                    Err(err) => {
                        eprintln!("Validation error: {}", err);
                        std::process::exit(1);
                    }
                },
                Err(err) => {
                    eprintln!("Error parsing PEP: {}", err);
                    std::process::exit(1);
                }
            }
        }
        Commands::Convert {
            path,
            schema,
            format,
            name,
            nested_inputs,
            show_non_literals,
            hide_defaults,
        } => {
            let proj = Project::from_config(path).build();
            match proj {
                Ok(proj) => match format {
                    cli::ConvertFormat::Wdl => {
                        let hide_defaults = hide_defaults.unwrap_or(false);
                        let nested_inputs = nested_inputs.unwrap_or(false);
                        let show_non_literals = show_non_literals.unwrap_or(false);

                        let wdl_parse_opts = WdlInputParsingOptions::new(schema)
                            .with_hide_defaults(hide_defaults)
                            .with_nested_inputs(nested_inputs)
                            .with_show_non_literals(show_non_literals);

                        let wdl_parse_opts = match name {
                            Some(name) => wdl_parse_opts.with_name(name),
                            None => wdl_parse_opts,
                        };

                        let input_string = proj.to_mapped_wdl_input(wdl_parse_opts);

                        match input_string {
                            Ok(res) => {
                                println!("{}", res);
                            }
                            Err(error) => {
                                eprintln!("{}", error);
                            }
                        }
                    }
                },
                Err(err) => {
                    let msg = format!("Error parsing PEP: {}", err);
                    eprintln!("{}", msg);
                }
            }
        }
    }
}
