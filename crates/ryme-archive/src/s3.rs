use ryme_error::{Result, RymeError};

#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub path_style: bool,
}

#[derive(Debug, Clone)]
pub struct S3Store {
    config: S3Config,
    client: reqwest::Client,
}

impl S3Store {
    pub fn new(config: S3Config) -> Result<Self> {
        if config.endpoint.is_empty() || config.bucket.is_empty() || config.region.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("s3 config")));
        }
        if config.access_key.is_empty() || config.secret_key.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("s3 credentials")));
        }
        let client = reqwest::Client::new();
        Ok(Self { config, client })
    }

    fn host(&self) -> Result<String> {
        let without_scheme = self
            .config
            .endpoint
            .split("://")
            .nth(1)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("endpoint")))?;
        let authority = without_scheme.split('/').next().unwrap_or(without_scheme);
        if self.config.path_style {
            Ok(authority.to_string())
        } else {
            Ok(format!("{}.{}", self.config.bucket, authority))
        }
    }

    fn url(&self, key: &str) -> Result<String> {
        let base = self.config.endpoint.trim_end_matches('/');
        let encoded = encode_key(key)?;
        if self.config.path_style {
            Ok(format!("{}/{}/{}", base, self.config.bucket, encoded))
        } else {
            Ok(format!("{base}/{encoded}"))
        }
    }

    async fn signed(
        &self,
        method: &str,
        key: &str,
        query: &str,
        body: Option<Vec<u8>>,
        now: u64,
    ) -> Result<reqwest::Response> {
        let payload_hash = match body.as_ref() {
            Some(bytes) => crate::sha256_hex(bytes),
            None => crate::sha256_hex(&[]),
        };
        let host = self.host()?;
        let path = if self.config.path_style {
            format!("/{}/{}", self.config.bucket, encode_key(key)?)
        } else {
            format!("/{}", encode_key(key)?)
        };
        let (datestamp, amztime) = amz_datetime(now);
        let headers = vec![
            (String::from("host"), host.clone()),
            (String::from("x-amz-content-sha256"), payload_hash.clone()),
            (String::from("x-amz-date"), amztime.clone()),
        ];
        let canonical = canonical_request(method, &path, query, &headers, &payload_hash);
        let scope = format!("{}/{}/{}/aws4_request", datestamp, self.config.region, "s3");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amztime}\n{scope}\n{}",
            crate::sha256_hex(canonical.as_bytes())
        );
        let signature = signature_hex(
            &self.config.secret_key,
            &datestamp,
            &self.config.region,
            "s3",
            &string_to_sign,
        );
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.config.access_key, scope, signed_headers, signature
        );
        let mut url = self.url(key)?;
        if !query.is_empty() {
            url.push('?');
            url.push_str(query);
        }
        let builder = match method {
            "PUT" => self.client.put(&url),
            "DELETE" => self.client.delete(&url),
            _ => self.client.get(&url),
        };
        let response = builder
            .header("host", host)
            .header("x-amz-date", amztime)
            .header("x-amz-content-sha256", payload_hash)
            .header("authorization", authorization)
            .body(body.unwrap_or_default())
            .send()
            .await
            .map_err(|e| RymeError::Unavailable(e.to_string()))?;
        Ok(response)
    }

    async fn list_page(
        &self,
        prefix: &str,
        token: Option<&str>,
        now: u64,
    ) -> Result<(Vec<String>, Option<String>)> {
        let mut query = format!("list-type=2&max-keys=1000&prefix={}", encode_query(prefix)?);
        if let Some(token) = token {
            query.push_str("&continuation-token=");
            query.push_str(&encode_query(token)?);
        }
        let response = self.signed("GET", "", &query, None, now).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok((Vec::new(), None));
        }
        if !response.status().is_success() {
            return Err(RymeError::Unavailable(format!("list {}", response.status())));
        }
        let body = response.bytes().await.map_err(|e| RymeError::Io(e.to_string()))?;
        let text = String::from_utf8_lossy(&body).into_owned();
        Ok((extract_keys(&text), extract_token(&text)))
    }
}

impl crate::ObjectStore for S3Store {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        let response = self.signed("PUT", key, "", Some(bytes), crate::now_unix()).await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(RymeError::Unavailable(format!("put {}", response.status())))
        }
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let response = self.signed("GET", key, "", None, crate::now_unix()).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(RymeError::NotFound(String::from("key")));
        }
        if !response.status().is_success() {
            return Err(RymeError::Unavailable(format!("get {}", response.status())));
        }
        response.bytes().await.map(|b| b.to_vec()).map_err(|e| RymeError::Io(e.to_string()))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let now = crate::now_unix();
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..100 {
            let (keys, next) = self.list_page(prefix, token.as_deref(), now).await?;
            out.extend(keys.into_iter().filter(|k| k.starts_with(prefix)));
            match next {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        out.sort();
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let response = self.signed("DELETE", key, "", None, crate::now_unix()).await?;
        if response.status().is_success() || response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(RymeError::Unavailable(format!("delete {}", response.status())))
        }
    }
}

pub fn amz_datetime(unix: u64) -> (String, String) {
    let (year, month, day, hour, minute, second) = split_unix(unix);
    (
        format!("{year:04}{month:02}{day:02}"),
        format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z"),
    )
}

fn split_unix(unix: u64) -> (i64, u64, u64, u64, u64, u64) {
    let days = (unix / 86400) as i64;
    let time = unix % 86400;
    let civil = days + 719468;
    let era = civil.div_euclid(146097);
    let doe = civil.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u64;
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u64;
    (if month <= 2 { year + 1 } else { year }, month, day, time / 3600, (time / 60) % 60, time % 60)
}

pub fn canonical_request(
    method: &str,
    path: &str,
    query: &str,
    headers: &[(String, String)],
    payload_hash: &str,
) -> String {
    let mut head = format!("{method}\n{path}\n{query}\n");
    let mut names = Vec::new();
    for (name, value) in headers {
        head.push_str(&format!("{}:{}\n", name.to_lowercase(), value.trim()));
        names.push(name.to_lowercase());
    }
    names.sort();
    head.push('\n');
    head.push_str(&names.join(";"));
    head.push('\n');
    head.push_str(payload_hash);
    head
}

pub fn signature_hex(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> String {
    use hmac::{Hmac, Mac};
    let mut key = format!("AWS4{secret}").into_bytes();
    for scope in [date, region, service, "aws4_request"] {
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&key)
            .unwrap_or_else(|_| Hmac::<sha2::Sha256>::new_from_slice(b"invalid").unwrap());
        mac.update(scope.as_bytes());
        key = mac.finalize().into_bytes().to_vec();
    }
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&key)
        .unwrap_or_else(|_| Hmac::<sha2::Sha256>::new_from_slice(b"invalid").unwrap());
    mac.update(string_to_sign.as_bytes());
    crate::hex_encode(&mac.finalize().into_bytes())
}

pub fn encode_key(key: &str) -> Result<String> {
    if key.contains("..") {
        return Err(RymeError::InvalidArgument(String::from("key")));
    }
    Ok(key.split('/').map(encode_segment).collect::<Vec<_>>().join("/"))
}

fn encode_segment(segment: &str) -> String {
    let mut out = String::new();
    for byte in segment.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(*byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn encode_query(value: &str) -> Result<String> {
    Ok(value.split('/').map(encode_segment).collect::<Vec<_>>().join("/"))
}

fn extract_keys(xml: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Key>") {
        rest = &rest[start + 5..];
        if let Some(end) = rest.find("</Key>") {
            out.push(rest[..end].to_string());
            rest = &rest[end + 6..];
        } else {
            break;
        }
    }
    out
}

fn extract_token(xml: &str) -> Option<String> {
    if !xml.contains("<IsTruncated>true</IsTruncated>") {
        return None;
    }
    let start = xml.find("<NextContinuationToken>")? + 24;
    let rest = &xml[start..];
    let end = rest.find("</NextContinuationToken>")?;
    Some(rest[..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectStore;

    #[test]
    fn aws_reference_vector() {
        let canonical = "GET\n/\n\nhost:example.amazonaws.com\nx-amz-date:20150830T123600Z\n\nhost;x-amz-date\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let built = canonical_request(
            "GET",
            "/",
            "",
            &[
                (String::from("host"), String::from("example.amazonaws.com")),
                (String::from("x-amz-date"), String::from("20150830T123600Z")),
            ],
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        );
        assert_eq!(built, canonical);
        let scope = "20150830/us-east-1/service/aws4_request";
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n20150830T123600Z\n{scope}\n{}",
            crate::sha256_hex(canonical.as_bytes())
        );
        assert!(
            to_sign.ends_with("bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63")
        );
        let signature = signature_hex(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "service",
            &to_sign,
        );
        assert_eq!(signature, "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31");
    }

    #[test]
    fn amz_date_known_value() {
        assert_eq!(
            amz_datetime(1440936000),
            (String::from("20150830"), String::from("20150830T120000Z"))
        );
        assert_eq!(
            amz_datetime(1440892800),
            (String::from("20150830"), String::from("20150830T000000Z"))
        );
    }

    #[test]
    fn list_xml_parsing() {
        let xml = "<?xml version=\"1.0\"?><ListBucketResult><Key>a</Key><Key>b/c</Key><IsTruncated>false</IsTruncated></ListBucketResult>";
        assert_eq!(extract_keys(xml), vec![String::from("a"), String::from("b/c")]);
        assert_eq!(extract_token(xml), None);
    }

    struct FakeS3 {
        files: std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>>,
        seen_auth: std::sync::Arc<tokio::sync::Mutex<Vec<String>>>,
    }

    async fn read_http_request(
        socket: &mut tokio::net::TcpStream,
    ) -> Option<(String, String, std::collections::HashMap<String, String>, Vec<u8>)> {
        use tokio::io::AsyncReadExt;
        let mut raw = Vec::new();
        let mut chunk = vec![0u8; 4096];
        loop {
            let read = socket.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            raw.extend_from_slice(&chunk[..read]);
            if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&raw[..end]).into_owned();
                let mut lines = head.lines();
                let request = lines.next().unwrap_or("").to_string();
                let mut headers = std::collections::HashMap::new();
                for line in lines {
                    if let Some((name, value)) = line.split_once(':') {
                        headers.insert(name.trim().to_lowercase(), value.trim().to_string());
                    }
                }
                let length: usize =
                    headers.get("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
                let mut body = raw[end + 4..].to_vec();
                while body.len() < length {
                    let read = socket.read(&mut chunk).await.ok()?;
                    if read == 0 {
                        break;
                    }
                    body.extend_from_slice(&chunk[..read]);
                }
                body.truncate(length);
                let mut parts = request.split_whitespace();
                let method = parts.next().unwrap_or("").to_string();
                let target = parts.next().unwrap_or("").to_string();
                return Some((method, target, headers, body));
            }
            if raw.len() > 1024 * 1024 {
                return None;
            }
        }
    }

    async fn run_fake_s3(listener: tokio::net::TcpListener, state: FakeS3) {
        use tokio::io::AsyncWriteExt;
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let files = state.files.clone();
            let seen = state.seen_auth.clone();
            tokio::spawn(async move {
                loop {
                    let Some((method, target, headers, body)) =
                        read_http_request(&mut socket).await
                    else {
                        return;
                    };
                    if let Some(auth) = headers.get("authorization") {
                        seen.lock().await.push(auth.clone());
                    }
                    let (path, query) = match target.split_once('?') {
                        Some((path, query)) => (path.to_string(), query.to_string()),
                        None => (target, String::new()),
                    };
                    let key = path.trim_start_matches("/test-bucket/").to_string();
                    let response = if method == "PUT" {
                        files.lock().await.insert(key, body);
                        "HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: keep-alive\r\n\r\n"
                            .to_string()
                    } else if method == "DELETE" {
                        files.lock().await.remove(&key);
                        "HTTP/1.1 204 No Content\r\ncontent-length: 0\r\nconnection: keep-alive\r\n\r\n".to_string()
                    } else if query.contains("list-type=2") {
                        let prefix =
                            query.split('&').find_map(|p| p.strip_prefix("prefix=")).unwrap_or("");
                        let guard = files.lock().await;
                        let mut body = String::from("<?xml version=\"1.0\"?><ListBucketResult>");
                        for name in guard.keys() {
                            if name.starts_with(prefix) {
                                body.push_str(&format!("<Key>{name}</Key>"));
                            }
                        }
                        body.push_str("<IsTruncated>false</IsTruncated></ListBucketResult>");
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: keep-alive\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    } else if let Some(bytes) = files.lock().await.get(&key).cloned() {
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: keep-alive\r\n\r\n{}",
                            bytes.len(),
                            String::from_utf8_lossy(&bytes)
                        )
                    } else {
                        let body = "<?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code></Error>";
                        format!(
                            "HTTP/1.1 404 Not Found\r\ncontent-length: {}\r\nconnection: keep-alive\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    };
                    if socket.write_all(response.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    }

    #[tokio::test]
    async fn s3_roundtrip_against_fake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = FakeS3 { files: Default::default(), seen_auth: Default::default() };
        let seen = state.seen_auth.clone();
        tokio::spawn(run_fake_s3(listener, state));
        let store = S3Store::new(S3Config {
            endpoint: format!("http://{addr}"),
            bucket: String::from("test-bucket"),
            region: String::from("test-1"),
            access_key: String::from("ak"),
            secret_key: String::from("sk"),
            path_style: true,
        })
        .unwrap();
        store.put("backups/a", b"hello".to_vec()).await.unwrap();
        assert_eq!(store.get("backups/a").await.unwrap(), b"hello");
        assert_eq!(store.list("backups/").await.unwrap(), vec![String::from("backups/a")]);
        store.delete("backups/a").await.unwrap();
        assert!(matches!(store.get("backups/a").await, Err(ryme_error::RymeError::NotFound(_))));
        let auths = seen.lock().await;
        assert!(!auths.is_empty());
        assert!(auths
            .iter()
            .all(|a| a.starts_with("AWS4-HMAC-SHA256 ") && a.contains("Signature=")));
    }
}
