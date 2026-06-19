use pephub_client::api;
use crate::cli::PHC;

pub fn phc_handler(command: &PHC) {
    match command {
        PHC::Login { token, url } => {
            println!("Login was provided");
            println!("token: {:?}", token);
            println!("url: {:?}", url);
        }
        PHC::Logout {} => {
            println!("Logout was provided");
        }

        PHC::Pull { path } => {
            println!("Pull path: {}", path);

            let api_builder = api::ApiBuilder::default();
            let cache_client = api_builder.cache.clone();
            println!("Cache path: {:?}", cache_client.token_path());
            println!("Cache token: {:?}", cache_client.token().unwrap());

            let api_client = api_builder.build().map_err(|e| {
                peprs_core::error::Error::Processing(format!("Failed to create PepHub client: {}", e))
                    }).unwrap();

            let project = api_client.get_raw(path).map_err(|e| {
                peprs_core::error::Error::Processing(format!("Failed to fetch from PepHub: {}", e))
            }).unwrap();

            println!("{:?}", project);

        }
    }
}