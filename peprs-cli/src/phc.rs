use crate::cli::PHC;
use colored::Colorize;
use pephub_client::api;
use std::path::PathBuf;
use pephub_client::auth::{CacheBuilder, Cache};

use peprs_core::utils::save_raw_pep;

pub fn phc_handler(command: &PHC) {
    match command {
        PHC::Login { token, url } => {
            println!("Login was provided");
            println!("token: {:?}", token);
            println!("url: {:?}", url);

            println!("Testing token saver...");
            let cache: Cache;
            if let Some(provided_token) = token {
                cache = CacheBuilder::new().with_token(provided_token).build().unwrap();
            } else {
                cache = CacheBuilder::new().build().unwrap();
            }
            cache.save_token().unwrap();
        }

        PHC::Logout {} => {
            let cache = Cache::default();
            cache.logout().unwrap();
        }

        PHC::Pull {
            registry,
            path,
            zip,
        } => {
            let mut path: PathBuf = path.into();
            let api_client = match api::ApiBuilder::default().build() {
                Ok(client) => client,
                Err(e) => {
                    eprintln!("Failed to create PepHub client: {}", e);
                    std::process::exit(1);
                }
            };

            let raw = match api_client.get_raw(registry) {
                Ok(raw) => raw,
                Err(e) => {
                    eprintln!("Failed to fetch '{}' from PepHub: {}", registry, e);
                    std::process::exit(1);
                }
            };

            let mut file_name = registry.to_string();
            file_name = file_name.replace("/", "_").replace(":", "_");

            if *zip {
                if !path.ends_with(".zip") {
                    file_name.push_str(".zip");
                    path = path.join(file_name);
                }
            } else {
                path = path.join(file_name);
            }

            match save_raw_pep(&path, &raw, *zip) {
                Ok(()) => println!(
                    "{}",
                    format!(
                        "Project '{}' successfully saved to {}",
                        registry,
                        path.display()
                    )
                    .green()
                ),
                Err(e) => {
                    eprintln!(
                        "{}",
                        format!("Failed to save project to {}: {}", path.display(), e).red()
                    );
                    std::process::exit(1);
                }
            }
        }
    }
}
