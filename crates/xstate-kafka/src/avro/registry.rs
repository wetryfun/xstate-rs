//! Schema Registry access: the Confluent REST API (which Redpanda's
//! built-in registry implements), or an in-memory registry for tests.

use std::sync::Mutex;

use apache_avro::Schema;

/// Where Avro schemas live, by id and subject.
pub trait Registry: Send + Sync {
    /// The schema registered under `id` (its JSON text).
    fn schema(&self, id: u32) -> Result<String, String>;
    /// Registers `schema` under `subject`, or finds it there if it already
    /// is, and returns its id. Fails when the subject's compatibility rules
    /// reject it.
    fn register(&self, subject: &str, schema: &str) -> Result<u32, String>;
    /// The id of `schema` under `subject`, which must already be registered.
    fn lookup(&self, subject: &str, schema: &str) -> Result<u32, String>;
}

fn canonical(schema: &str) -> String {
    Schema::parse_str(schema).map(|s| s.canonical_form()).unwrap_or_else(|_| schema.to_owned())
}

/// A registry in memory: ids from 1, the same schema (by canonical form)
/// reusing its id, any version accepted.
#[derive(Debug, Default)]
pub struct MemoryRegistry {
    /// Schemas by id - 1, with the subjects they were registered under.
    schemas: Mutex<Vec<(String, Vec<String>)>>,
}

impl Registry for MemoryRegistry {
    fn schema(&self, id: u32) -> Result<String, String> {
        let schemas = self.schemas.lock().unwrap_or_else(|e| e.into_inner());
        id.checked_sub(1)
            .and_then(|i| schemas.get(i as usize))
            .map(|s| s.0.clone())
            .ok_or_else(|| format!("schema {id} not found"))
    }

    fn register(&self, subject: &str, schema: &str) -> Result<u32, String> {
        let mut schemas = self.schemas.lock().unwrap_or_else(|e| e.into_inner());
        let form = canonical(schema);
        if let Some(i) = schemas.iter().position(|s| canonical(&s.0) == form) {
            if !schemas[i].1.iter().any(|s| s == subject) {
                schemas[i].1.push(subject.to_owned());
            }
            return Ok(i as u32 + 1);
        }
        schemas.push((schema.to_owned(), vec![subject.to_owned()]));
        Ok(schemas.len() as u32)
    }

    fn lookup(&self, subject: &str, schema: &str) -> Result<u32, String> {
        let schemas = self.schemas.lock().unwrap_or_else(|e| e.into_inner());
        let form = canonical(schema);
        schemas
            .iter()
            .position(|s| canonical(&s.0) == form && s.1.iter().any(|x| x == subject))
            .map(|i| i as u32 + 1)
            .ok_or_else(|| format!("the schema is not registered under {subject}"))
    }
}

/// The Schema Registry REST API over HTTP(S), with optional basic auth.
#[cfg(feature = "kafka")]
pub struct HttpRegistry {
    base: String,
    agent: ureq::Agent,
    authorization: Option<String>,
}

#[cfg(feature = "kafka")]
impl HttpRegistry {
    /// `url` like `http://localhost:8081`; `credentials` as `(user, password)`.
    pub fn new(url: &str, credentials: Option<(String, String)>, timeout: std::time::Duration) -> Self {
        let config = ureq::Agent::config_builder().timeout_global(Some(timeout)).http_status_as_error(false).build();
        let authorization =
            credentials.map(|(user, password)| format!("Basic {}", base64(&format!("{user}:{password}"))));
        Self { base: url.trim_end_matches('/').to_owned(), agent: config.into(), authorization }
    }

    fn call(&self, method: &str, path: &str, body: Option<&serde_json::Value>) -> Result<serde_json::Value, String> {
        let url = format!("{}{path}", self.base);
        let accept = "application/vnd.schemaregistry.v1+json";
        let response = match body {
            Some(body) => {
                let mut request = self.agent.post(&url).header("Accept", accept).header("Content-Type", accept);
                if let Some(a) = &self.authorization {
                    request = request.header("Authorization", a);
                }
                request.send(body.to_string().as_bytes())
            }
            None => {
                let mut request = self.agent.get(&url).header("Accept", accept);
                if let Some(a) = &self.authorization {
                    request = request.header("Authorization", a);
                }
                request.call()
            }
        };
        let mut response = response.map_err(|e| format!("{method} {url}: {e}"))?;
        let status = response.status().as_u16();
        let text = response.body_mut().read_to_string().map_err(|e| format!("{method} {url}: {e}"))?;
        let json: serde_json::Value = serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text));
        if !(200..300).contains(&status) {
            let message =
                json.get("message").and_then(serde_json::Value::as_str).map_or_else(|| json.to_string(), str::to_owned);
            return Err(format!("{method} {url}: {status} {message}"));
        }
        Ok(json)
    }
}

#[cfg(feature = "kafka")]
impl Registry for HttpRegistry {
    fn schema(&self, id: u32) -> Result<String, String> {
        let json = self.call("GET", &format!("/schemas/ids/{id}"), None)?;
        if json.get("schemaType").and_then(serde_json::Value::as_str).is_some_and(|t| t != "AVRO") {
            return Err(format!("schema {id} is not an Avro schema"));
        }
        json.get("schema")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| format!("schema {id}: no schema"))
    }

    fn register(&self, subject: &str, schema: &str) -> Result<u32, String> {
        let body = serde_json::json!({ "schema": schema });
        let json = self.call("POST", &format!("/subjects/{}/versions", encode_path(subject)), Some(&body))?;
        id_of(&json)
    }

    fn lookup(&self, subject: &str, schema: &str) -> Result<u32, String> {
        let body = serde_json::json!({ "schema": schema });
        let json = self.call("POST", &format!("/subjects/{}", encode_path(subject)), Some(&body))?;
        id_of(&json)
    }
}

#[cfg(feature = "kafka")]
fn id_of(json: &serde_json::Value) -> Result<u32, String> {
    json.get("id").and_then(serde_json::Value::as_u64).map(|id| id as u32).ok_or_else(|| format!("no id in {json}"))
}

/// Percent-encodes a path segment.
#[cfg(feature = "kafka")]
fn encode_path(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => (b as char).to_string(),
            b => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(feature = "kafka")]
fn base64(s: &str) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk.iter().enumerate().fold(0u32, |n, (i, &b)| n | (u32::from(b) << (16 - 8 * i)));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(all(test, feature = "kafka"))]
mod tests {
    use super::*;

    #[test]
    fn helpers() {
        assert_eq!(base64("user:pass"), "dXNlcjpwYXNz");
        assert_eq!(base64("ab"), "YWI=");
        assert_eq!(encode_path("orders-value"), "orders-value");
        assert_eq!(encode_path("a/b c"), "a%2Fb%20c");
    }
}
