use crate::cli::PHC;
use colored::Colorize;
use pephub_client::api;
use pephub_client::auth::{Cache, CacheBuilder};
use std::path::PathBuf;

use peprs_core::utils::save_raw_pep;

pub fn phc_handler(command: &PHC) {
    match command {
        PHC::Login { token, url } => {
            let cache_builder = match url {
                Some(base_url) => CacheBuilder::new().with_url(base_url.to_string()),
                None => CacheBuilder::new(),
            };
            match token {
                Some(token_string) => {
                    println!("Token provided. Registering... ");
                    let cache = cache_builder
                        .with_token(token_string.to_string())
                        .build()
                        .unwrap();
                    cache.save_token().unwrap();
                    println!("Token successfully registered.");
                }
                None => {
                    cache_builder.build().unwrap().login().unwrap();
                }
            };
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
                let has_zip_ext = path.extension().and_then(|e| e.to_str()) == Some("zip");
                if !has_zip_ext {
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
