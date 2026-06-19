use std::path::PathBuf;

use dirs::home_dir;

// All of this should be changed to toml file, same way as logfire works

const PH_HOME_ENV_VAR: &str = "PH_HOME";
const PH_HOME_DEFAULT: &str = ".pephubclient";
const PH_TOKEN_FILE_NAME: &str = "jwt.txt";

#[derive(Clone, Debug)]
pub struct Cache {
    path: PathBuf,
}

impl Cache {
    /// Creates a new cache object location
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Creates cache from environment variable PH_HOME (if defined) otherwise
    /// defaults to [`home_dir`]/.pephubclient
    pub fn from_env() -> Self {
        // TODO: verify if thats correct
        match std::env::var(PH_HOME_ENV_VAR) {
            Ok(home) => {
                let mut path: PathBuf = home.into();
                path.push("hub");
                Self::new(path)
            }
            Err(_) => Self::default(),
        }
    }

    /// Creates a new cache object location
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Returns the location of the token file
    /// For now token_path is the same as path
    pub fn token_path(&self) -> PathBuf {
        let mut path = self.path.clone();
        path
    }

    /// Returns the token value if it exists in the cache
    /// Use `peprs pephub login` to set it up.
    pub fn token(&self) -> Option<String> {
        let token_filename = self.token_path();
        // if token_filename.exists() {
        //     log::info!("Using token file found {token_filename:?}");
        // }
        match std::fs::read_to_string(token_filename) {
            Ok(token_content) => {
                let token_content = token_content.trim();
                if token_content.is_empty() {
                    None
                } else {
                    Some(token_content.to_string())
                }
            }
            Err(_) => None,
        }
    }
}

impl Default for Cache {
    fn default() -> Self {
        let mut path = home_dir().expect("Cache directory cannot be found");
        path.push(PH_HOME_DEFAULT);
        path.push(PH_TOKEN_FILE_NAME);
        Self::new(path)
    }
}
