use std::collections::HashMap;
use std::num::ParseIntError;
use std::path::PathBuf;

use thiserror::Error;

use peprs_core::config::ProjectConfig;
use serde::Deserialize;
use ureq::config::ConfigBuilder;
use ureq::config::RedirectAuthHeaders;
use ureq::tls::{TlsConfig, TlsProvider};
use ureq::typestate::{AgentScope, WithoutBody};
use ureq::{Agent, RequestBuilder};

use crate::cache::Cache;

const PH_ENDPOINT_ENV_VAR: &str = "PH_ENDPOINT";

/// Current version (used in user-agent)
const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Current name (used in user-agent)
const NAME: &str = env!("CARGO_PKG_NAME");
const USER_AGENT: &str = "User-Agent";
const AUTHORIZATION: &str = "Authorization";

type HeaderMap = HashMap<&'static str, String>;
type HeaderName = &'static str;

/// Simple wrapper over [`ureq::Agent`] to include default headers
#[derive(Clone, Debug)]
pub struct HeaderAgent {
    agent: Agent,
    headers: HeaderMap,
}

impl HeaderAgent {
    fn new(agent: Agent, headers: HeaderMap) -> Self {
        Self { agent, headers }
    }

    fn get(&self, url: &str) -> RequestBuilder<WithoutBody> {
        let mut request = self.agent.get(url);
        for (header, value) in &self.headers {
            request = request.header(*header, value);
        }
        request
    }
}

/// Helper to create [`Api`] with all the options.
#[derive(Debug)]
pub struct ApiBuilder {
    endpoint: String,
    cache: Cache,
    token: Option<String>,
    user_agent: Vec<(String, String)>,
}

impl Default for ApiBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ApiBuilder {
    /// Default api builder
    pub fn new() -> Self {
        let cache = Cache::default();
        Self::from_cache(cache)
    }

    /// Creates API with values potentially from environment variables.
    /// PH_HOME decides the location of the cache folder
    /// PH_ENDPOINT modifies the URL for the pephub location
    /// to download files from.
    pub fn from_env() -> Self {
        let cache = Cache::from_env();
        let mut builder = Self::from_cache(cache);
        if let Ok(endpoint) = std::env::var(PH_ENDPOINT_ENV_VAR) {
            builder = builder.with_endpoint(endpoint);
        }
        builder
    }

    /// From a given cache
    pub fn from_cache(cache: Cache) -> Self {
        let token = cache.token();

        let endpoint = "https://pephub-api.databio.org".to_string();

        let user_agent = vec![
            ("unknown".to_string(), "None".to_string()),
            (NAME.to_string(), VERSION.to_string()),
            ("rust".to_string(), "unknown".to_string()),
        ];

        Self {
            endpoint,
            cache,
            token,
            user_agent,
        }
    }

    /// Changes the endpoint of the API. Default is `https://pephub-api.databio.org/`.
    pub fn with_endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = endpoint;
        self
    }

    /// Changes the location of the cache directory. Defaults is `~/.cache/pephub/`.
    pub fn with_cache_dir(mut self, cache_dir: PathBuf) -> Self {
        self.cache = Cache::new(cache_dir);
        self
    }

    /// Sets the token to be used in the API
    pub fn with_token(mut self, token: Option<String>) -> Self {
        self.token = token;
        self
    }

    /// Adds custom fields to headers user-agent
    pub fn with_user_agent(mut self, key: &str, value: &str) -> Self {
        self.user_agent.push((key.to_string(), value.to_string()));
        self
    }

    fn build_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        let user_agent = self
            .user_agent
            .iter()
            .map(|(key, value)| format!("{key}/{value}"))
            .collect::<Vec<_>>()
            .join("; ");
        headers.insert(USER_AGENT, user_agent.to_string());
        if let Some(token) = &self.token {
            headers.insert(AUTHORIZATION, format!("Bearer {token}"));
        }
        headers
    }

    /// Consumes the builder and builds the final [`Api`]
    pub fn build(self) -> Result<Api, ApiError> {
        let headers = self.build_headers();

        let builder = builder()?.redirect_auth_headers(RedirectAuthHeaders::SameHost);
        let agent: Agent = builder.build().into();
        let client = HeaderAgent::new(agent, headers.clone());

        Ok(Api {
            endpoint: self.endpoint,
            client,
        })
    }
}

fn builder() -> Result<ConfigBuilder<AgentScope>, ApiError> {
    Ok(Agent::config_builder()
        .tls_config(TlsConfig::builder().provider(TlsProvider::Rustls).build()))
}

#[derive(Debug, Deserialize)]
struct ConfigResponse {
    config: String,
}

#[derive(Clone, Debug)]
pub struct Api {
    endpoint: String,
    client: HeaderAgent,
}

impl Api {
    /// Creates a default Api, for Api options See [`ApiBuilder`]
    pub fn new() -> Result<Self, ApiError> {
        ApiBuilder::new().build()
    }

    /// Get a configuration file from the specified pephub registry
    pub fn get_config(&self, registry: &str) -> Result<ProjectConfig, ApiError> {
        let endpoint = &self.endpoint;
        let url = format!("{endpoint}/api/v1/projects/{registry}/config");

        // First, deserialize the JSON response
        let response: ConfigResponse = self
            .client
            .get(&url)
            .call()
            .map_err(Box::new)?
            .body_mut()
            .read_json()
            .map_err(Box::new)?;

        // Then parse the YAML string into ProjectConfig
        let cfg: ProjectConfig = serde_yaml::from_str(&response.config)
            .map_err(|e| ApiError::YamlParseError(Box::new(e)))?;

        Ok(cfg)
    }

    /// Get samples data from the specified pephub registry in CSV format
    pub fn get_samples(&self, registry: &str) -> Result<Vec<u8>, ApiError> {
        let endpoint = &self.endpoint;
        let url = format!("{endpoint}/api/v1/projects/{registry}/samples?format=csv&raw=true");

        let mut response = self.client.get(&url).call().map_err(Box::new)?;

        let bytes = response.body_mut().read_to_vec().map_err(Box::new)?;

        Ok(bytes)
    }
}

#[derive(Debug, Error)]
/// All errors the API can throw
pub enum ApiError {
    /// Api expects certain header to be present in the results to derive some information
    #[error("Header {0} is missing")]
    MissingHeader(HeaderName),

    /// The header exists, but the value is not conform to what the Api expects.
    #[error("Header {0} is invalid")]
    InvalidHeader(HeaderName),

    /// Error in the request
    #[error("request error: {0}")]
    RequestError(#[from] Box<ureq::Error>),

    /// Error parsing some range value
    #[error("Cannot parse int")]
    ParseIntError(#[from] ParseIntError),

    /// I/O Error
    #[error("I/O error {0}")]
    IoError(#[from] std::io::Error),

    /// We tried to download chunk too many times
    #[error("Too many retries: {0}")]
    TooManyRetries(Box<ApiError>),

    /// The part file is corrupted
    #[error("Invalid part file - corrupted file")]
    InvalidResume,

    /// Error parsing YAML configuration
    #[error("YAML parse error: {0}")]
    YamlParseError(#[from] Box<serde_yaml::Error>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use rstest::*;

    #[rstest]
    fn test_get_config_databio_example() {
        let api = Api::new().expect("Failed to create API client");
        let result = api.get_config("databio/example");
        assert_eq!(result.is_ok(), true);
        assert_eq!(result.unwrap().pep_version, "2.1.0");
    }

    #[rstest]
    fn test_get_samples_databio_example() {
        let api = Api::new().expect("Failed to create API client");
        let result = api.get_samples("databio/example");
        assert_eq!(result.is_ok(), true);

        let expected_csv = b"sample_name,sample_library_strategy,genome,time_point\n4-1_11102016,miRNA-Seq,hg38,morning\n3-1_11102016,miRNA-Seq,hg38,morning\n2-2_11102016,miRNA-Seq,hg38,afternoon\n2-1_11102016,miRNA-Seq,hg38,morning\n8-3_11152016,miRNA-Seq,hg38,evening\n8-1_11152016,miRNA-Seq,hg38,morning\n";
        let actual_bytes = result.unwrap();
        assert_eq!(actual_bytes, expected_csv);
    }

    #[rstest]
    fn test_get_samples_invalid_registry() {
        let api = Api::new().expect("Failed to create API client");
        let result = api.get_samples("invalid/nonexistent");
        assert_eq!(result.is_err(), true);
    }

    #[rstest]
    fn test_api_builder_default() {
        let builder = ApiBuilder::default();
        assert_eq!(builder.endpoint, "https://pephub-api.databio.org");
        assert_eq!(builder.token, None);
    }

    #[rstest]
    fn test_api_builder_with_endpoint() {
        let custom_endpoint = "https://custom-endpoint.com";
        let api = ApiBuilder::new()
            .with_endpoint(custom_endpoint.to_string())
            .build()
            .expect("Failed to build API");
        assert_eq!(api.endpoint, custom_endpoint);
    }

    #[rstest]
    fn test_api_builder_with_token() {
        let token = "test-token-123";
        let builder = ApiBuilder::new().with_token(Some(token.to_string()));
        assert_eq!(builder.token, Some(token.to_string()));
    }

    #[rstest]
    fn test_get_config_invalid_registry() {
        let api = Api::new().expect("Failed to create API client");
        let result = api.get_config("invalid/nonexistent");
        assert_eq!(result.is_err(), true);
    }
}
