mod cli;

use clap::Parser;
use peprs_core::project::Project;
use peprs_core::wdl::WdlInputParsingOptions;

use crate::cli::{Cli, Commands};

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Inspect {
            path,
            name,
            st_index,
            sst_index,
            amendments,
        } => {
            let mut builder = if path.ends_with(".csv") {
                match Project::from_csv(path) {
                    Ok(b) => b,
                    Err(err) => {
                        eprintln!("Error parsing PEP: {}", err);
                        return;
                    }
                }
            } else {
                Project::from_config(path)
            };
            if let Some(st_index) = st_index {
                builder = builder.with_sample_table_index(st_index.clone());
            }
            if let Some(sst_index) = sst_index {
                builder = builder.with_subsample_table_index(&[sst_index.clone()]);
            }
            if let Some(amendments) = amendments {
                builder = builder.with_amendments(amendments);
            }
            let proj = builder.build();
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
        Commands::Validate {
            path,
            schema,
            sample_name,
            st_index,
            sst_index,
            amendments,
        } => {
            let mut builder = if path.ends_with(".csv") {
                match Project::from_csv(path) {
                    Ok(b) => b,
                    Err(err) => {
                        eprintln!("Error parsing PEP: {}", err);
                        std::process::exit(1);
                    }
                }
            } else {
                Project::from_config(path)
            };
            if let Some(st_index) = st_index {
                builder = builder.with_sample_table_index(st_index.clone());
            }
            if let Some(sst_index) = sst_index {
                builder = builder.with_subsample_table_index(&[sst_index.clone()]);
            }
            if let Some(amendments) = amendments {
                builder = builder.with_amendments(amendments);
            }
            let proj = builder.build();
            match proj {
                Ok(proj) => {
                    let result = if let Some(name) = sample_name {
                        let eido_schema = match peprs_eido::load_schema(schema) {
                            Ok(s) => s,
                            Err(err) => {
                                eprintln!("Error loading schema: {}", err);
                                std::process::exit(1);
                            }
                        };
                        let sample = match proj.get_sample(name) {
                            Ok(Some(s)) => s,
                            Ok(None) => {
                                eprintln!("Sample '{}' not found in sample table", name);
                                std::process::exit(1);
                            }
                            Err(err) => {
                                eprintln!("Error retrieving sample: {}", err);
                                std::process::exit(1);
                            }
                        };
                        let json_map: serde_json::Map<String, serde_json::Value> = sample
                            .iter()
                            .map(|(k, v)| {
                                (k.clone(), peprs_core::utils::any_value_to_json(v.clone()))
                            })
                            .collect();
                        let sample_json = serde_json::Value::Object(json_map);
                        peprs_eido::validate_single_sample(&sample_json, &eido_schema, name)
                    } else {
                        peprs_eido::validate(&proj, schema)
                    };
                    match result {
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
                    }
                }
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
