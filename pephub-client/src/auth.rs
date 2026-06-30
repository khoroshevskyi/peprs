use dirs::home_dir;
use fs::read_to_string;
use jsonwebtoken::dangerous::insecure_decode;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::api::HeaderAgent;
use crate::error::CacheError;

const PH_HOME_ENV_VAR: &str = "PH_HOME";
const PH_HOME_DEFAULT: &str = ".pephubclient";
const PH_TOKEN_FILE_NAME: &str = "jwt.toml";
const DEFAULT_ENDPOINT: &str = "https://pephub-api.databio.org";
const DEVICE_INIT_PATH: &str = "auth/device/init";
const DEVICE_TOKEN_PATH: &str = "auth/device/token";

/// Response from the device-code init endpoint.
#[derive(Debug, Deserialize)]
struct InitializeDeviceCodeResponse {
    device_code: String,
    auth_url: String,
}

/// Response from the device-code token-exchange endpoint.
#[derive(Debug, Deserialize)]
struct DeviceTokenResponse {
    jwt_token: String,
}

/// Default token-file path: `$PH_HOME/jwt.toml` if `PH_HOME` is set,
/// otherwise `~/.pephubclient/jwt.toml`.
fn default_token_path() -> PathBuf {
    let mut path = match std::env::var(PH_HOME_ENV_VAR) {
        Ok(home) => PathBuf::from(home),
        Err(_) => {
            let mut path = home_dir().expect("Cache directory cannot be found");
            path.push(PH_HOME_DEFAULT);
            path
        }
    };
    path.push(PH_TOKEN_FILE_NAME);
    path
}

/// The subset of JWT claims we care about.
#[derive(Debug, Deserialize)]
struct JwtClaims {
    /// Expiration time as a unix timestamp (seconds since epoch).
    exp: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Token {
    pub token: Option<String>,
    pub base_url: String,
}

impl Default for Token {
    fn default() -> Self {
        Self {
            token: None,
            base_url: DEFAULT_ENDPOINT.to_string(),
        }
    }
}

impl Token {
    pub fn from_toml<P: AsRef<Path>>(path: P) -> Result<Self, CacheError> {
        let file_content = read_to_string(path)?;
        let config: Token = toml::from_str(&file_content)?;
        Ok(config)
    }

    /// Loads the token file, creating it (and any missing parent directories)
    /// with default contents if it does not yet exist.
    pub fn init_toml<P: AsRef<Path>>(path: P) -> Result<Self, CacheError> {
        let path = path.as_ref();
        if path.exists() {
            return Self::from_toml(path);
        }

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let config = Self::default();
        config.to_toml(path)?;
        Ok(config)
    }

    pub fn to_toml<P: AsRef<Path>>(&self, path: P) -> Result<(), CacheError> {
        let toml_string = toml::to_string(&self)?;
        fs::write(path, toml_string)?;
        Ok(())
    }

    /// Reads the `exp` claim from the JWT without verifying its signature.
    ///
    /// The token is issued and signed by PEPHub, so we have no key to verify it;
    /// we only decode the payload to inspect the expiration. Returns `None` if
    /// there is no token or it cannot be decoded.
    pub fn get_expiration(&self) -> Option<u64> {
        let token = self.token.as_ref()?;
        let data = insecure_decode::<JwtClaims>(token).ok()?;
        Some(data.claims.exp)
    }

    /// Returns `true` if the token is missing, undecodable, or past its `exp`.
    pub fn is_expired(&self) -> bool {
        match self.get_expiration() {
            Some(exp) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                exp <= now
            }
            None => true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Cache {
    pub token_path: PathBuf,
    pub token: Token,
}

impl Default for Cache {
    fn default() -> Self {
        CacheBuilder::new()
            .build()
            .expect("Failed to initialize token cache")
    }
}

impl Cache {
    pub fn new(path: PathBuf, token: Token) -> Self {
        Self {
            token_path: path,
            token,
        }
    }

    pub fn save_token(&self) -> Result<(), CacheError> {
        self.token.to_toml(&self.token_path)
    }

    /// Returns the cached JWT string, if one is present.
    pub fn token(&self) -> Option<String> {
        self.token.token.clone()
    }

    /// Returns the base url stored alongside the token.
    pub fn base_url(&self) -> &str {
        &self.token.base_url
    }

    /// Logs in to PEPHub via the OAuth device flow, then saves the JWT to disk.
    ///
    /// Mirrors the Python `pephubclient` flow: request a device code, ask the user to
    /// authenticate in the browser, poll the token endpoint a few times, then prompt the
    /// user to continue before a final attempt.
    pub fn login(&self) -> Result<(), CacheError> {
        let base = self.token.base_url.trim_end_matches('/').to_string();
        let client = HeaderAgent::unauthenticated()?;

        // 1. Request a device code.
        let init: InitializeDeviceCodeResponse = client
            .post(&format!("{base}/{DEVICE_INIT_PATH}"))
            .send_empty()
            .map_err(Box::new)?
            .body_mut()
            .read_json()
            .map_err(Box::new)?;

        // 2. Instruct the user.
        println!(
            "User verification code: {}, please go to the website: {} to authenticate.",
            init.device_code, init.auth_url
        );

        // 3-4. Initial polling: 3 attempts, 2s apart.
        thread::sleep(Duration::from_secs(2));
        for _ in 0..3 {
            match self.exchange(&client, &base, &init.device_code) {
                Ok(jwt) => return self.persist(jwt),
                Err(CacheError::AuthorizationPending) => {
                    thread::sleep(Duration::from_secs(2));
                }
                Err(e) => return Err(e),
            }
        }

        // 5. Manual continue.
        print!("If you logged in, press enter to continue...");
        io::stdout().flush().ok();
        let mut line = String::new();
        io::stdin().read_line(&mut line).ok();

        // 6. Final attempt.
        match self.exchange(&client, &base, &init.device_code) {
            Ok(jwt) => self.persist(jwt),
            Err(CacheError::AuthorizationPending) => {
                println!("Login failed. Please try again.");
                Err(CacheError::LoginFailed)
            }
            Err(e) => Err(e),
        }
    }

    /// Exchanges a device code for a JWT. Maps HTTP 401 to [`CacheError::AuthorizationPending`].
    fn exchange(
        &self,
        client: &HeaderAgent,
        base: &str,
        device_code: &str,
    ) -> Result<String, CacheError> {
        let resp = client
            .post(&format!("{base}/{DEVICE_TOKEN_PATH}"))
            .header("device-code", device_code)
            .send_empty();

        match resp {
            Ok(mut r) => {
                let token: DeviceTokenResponse = r.body_mut().read_json().map_err(Box::new)?;
                Ok(token.jwt_token)
            }
            Err(ureq::Error::StatusCode(401)) => Err(CacheError::AuthorizationPending),
            Err(e) => Err(CacheError::Request(Box::new(e))),
        }
    }

    /// Stores the obtained JWT in the token file.
    fn persist(&self, jwt: String) -> Result<(), CacheError> {
        let mut token = self.token.clone();
        token.token = Some(jwt);
        token.to_toml(&self.token_path)?;
        println!("Successfully logged in!");
        Ok(())
    }

    /// Removes the cached token file, logging the user out. No-op if the file
    /// does not exist.
    pub fn logout(&self) -> Result<(), CacheError> {
        if !self.token_path.exists() {
            println!("Already logged out.");
            return Ok(());
        }

        fs::remove_file(&self.token_path)?;
        println!("Logged out.");
        Ok(())
    }
}

/// Builder for [`Cache`].
///
/// `CacheBuilder::new().build()` loads (or creates) the default token file at
/// `$PH_HOME/jwt.toml` (falling back to `~/.pephubclient/jwt.toml`). Use the
/// `with_*` methods to point at a different token file or to override the token
/// and base url in memory:
///
/// ```no_run
/// # use pephub_client::auth::CacheBuilder;
/// let cache = CacheBuilder::new()
///     .with_token("my-jwt")
///     .with_url("https://pephub-api.databio.org")
///     .build()
///     .unwrap();
/// ```
#[derive(Clone, Debug)]
pub struct CacheBuilder {
    token_path: PathBuf,
    token: Option<String>,
    base_url: Option<String>,
}

impl Default for CacheBuilder {
    fn default() -> Self {
        Self {
            token_path: default_token_path(),
            token: None,
            base_url: None,
        }
    }
}

impl CacheBuilder {
    /// Creates a builder using the default token path (honors `PH_HOME`).
    pub fn new() -> Self {
        Self::default()
    }

    /// Use a specific token file path instead of the default.
    pub fn with_token_path<P: Into<PathBuf>>(mut self, token_path: P) -> Self {
        self.token_path = token_path.into();
        self
    }

    /// Override the token string (in memory only; not written to disk).
    pub fn with_token<S: Into<String>>(mut self, token: S) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Override the base url (in memory only; not written to disk).
    pub fn with_url<S: Into<String>>(mut self, url: S) -> Self {
        self.base_url = Some(url.into());
        self
    }

    /// Loads/creates the token file, applies any overrides, and builds the [`Cache`].
    pub fn build(self) -> Result<Cache, CacheError> {
        let mut token = Token::init_toml(&self.token_path)?;
        if let Some(t) = self.token {
            token.token = Some(t);
        }
        if let Some(url) = self.base_url {
            token.base_url = url;
        }

        Ok(Cache::new(self.token_path, token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_cache() {
        // Use a dedicated temp-dir token path instead of `Cache::default()` (which
        // reads/writes the shared OS-default file), so this test can't race with
        // other tests that also build a default cache.
        let token_path = std::env::temp_dir().join("peprs_test_init_cache_jwt.toml");
        let _ = fs::remove_file(&token_path);

        let cache = CacheBuilder::new()
            .with_token_path(&token_path)
            .build()
            .expect("build should succeed");
        assert_eq!(cache.token_path, token_path);

        let _ = fs::remove_file(&token_path);
    }

    // Example PEPHub JWT (HS256) with `exp` = 1784833275.
    const EXAMPLE_JWT: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJvcmdzIjpbImRhdGFiaW8iXSwibG9naW4iOiJraG9yb3NoZXZza3lpIiwiaWQiOjQxNTczNjI4LCJub2RlX2lkIjoiTURRNlZYTmxjalF4TlRjek5qSTQiLCJjcmVhdGVkX2F0IjoiMjAxOC0wNy0yM1QxMDoyMjo1N1oiLCJ1cGRhdGVkX2F0IjoiMjAyNi0wNi0wNVQwMzowNDoxN1oiLCJleHAiOjE3ODQ4MzMyNzV9.WMIZsMMDbthqhXTVEf_2IcQ4NfsOjoLaTHcWnu4cASs";

    #[test]
    fn test_get_expiration() {
        let token = Token {
            token: Some(EXAMPLE_JWT.to_string()),
            base_url: DEFAULT_ENDPOINT.to_string(),
        };
        assert_eq!(token.get_expiration(), Some(1784833275));
    }

    #[test]
    fn test_get_expiration_none_when_no_token() {
        let token = Token::default();
        assert_eq!(token.get_expiration(), None);
    }

    #[test]
    fn test_is_expired_no_token() {
        assert!(Token::default().is_expired());
    }

    #[test]
    fn test_is_expired_garbage_token() {
        let token = Token {
            token: Some("not-a-jwt".to_string()),
            base_url: DEFAULT_ENDPOINT.to_string(),
        };
        assert!(token.is_expired());
    }

    #[test]
    fn test_cache_builder_overrides() {
        let token_path = std::env::temp_dir().join("peprs_cache_builder_test_jwt.toml");
        let _ = fs::remove_file(&token_path);

        let cache = CacheBuilder::new()
            .with_token_path(&token_path)
            .with_token("my-jwt")
            .with_url("https://example.org")
            .build()
            .expect("build should succeed");

        assert_eq!(cache.token_path, token_path);
        assert_eq!(cache.token.token.as_deref(), Some("my-jwt"));
        assert_eq!(cache.token.base_url, "https://example.org");

        let _ = fs::remove_file(&token_path);
    }
}
