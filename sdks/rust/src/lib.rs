use futures_util::{SinkExt, StreamExt};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("http: {0}")]
    Http(String),
    #[error("status {0}: {1}")]
    Status(u16, String),
    #[error("websocket: {0}")]
    WebSocket(String),
}

#[derive(Debug, Clone)]
pub struct RymeClient {
    base: String,
    api_key: String,
    http: reqwest::Client,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyRow {
    pub key: String,
    pub value: String,
}

pub struct RealtimeSubscription {
    stream: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
}

impl RealtimeSubscription {
    /// Receive the next complete JSON text frame, replying to WebSocket pings automatically.
    pub async fn recv(&mut self) -> Result<Option<String>, ClientError> {
        while let Some(message) = self.stream.next().await {
            match message.map_err(|error| ClientError::WebSocket(error.to_string()))? {
                Message::Text(text) => return Ok(Some(text)),
                Message::Ping(payload) => self
                    .stream
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|error| ClientError::WebSocket(error.to_string()))?,
                Message::Close(_) => return Ok(None),
                _ => {}
            }
        }
        Ok(None)
    }

    pub async fn close(mut self) -> Result<(), ClientError> {
        self.stream
            .close(None)
            .await
            .map_err(|error| ClientError::WebSocket(error.to_string()))
    }

    async fn send_json(&mut self, value: serde_json::Value) -> Result<(), ClientError> {
        self.stream
            .send(Message::Text(value.to_string()))
            .await
            .map_err(|error| ClientError::WebSocket(error.to_string()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupabasePostgresChange {
    pub event: String,
    pub schema: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub select: Option<Vec<String>>,
}

impl SupabasePostgresChange {
    pub fn new(table: Option<String>) -> Self {
        Self {
            event: String::from("*"),
            schema: String::from("public"),
            table,
            filter: None,
            select: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SupabaseChannelOptions {
    pub broadcast_ack: bool,
    pub broadcast_self: bool,
    pub presence_key: Option<String>,
    pub postgres_changes: Vec<SupabasePostgresChange>,
}

/// A Supabase-compatible realtime channel using the Phoenix wire protocol.
pub struct SupabaseChannel {
    stream: RealtimeSubscription,
    topic: String,
    join_ref: String,
    next_ref: u64,
}

impl SupabaseChannel {
    pub async fn recv(&mut self) -> Result<Option<String>, ClientError> {
        self.stream.recv().await
    }

    /// Send a broadcast event after the channel has been joined.
    pub async fn send_broadcast(
        &mut self,
        event: &str,
        payload: serde_json::Value,
    ) -> Result<(), ClientError> {
        let reference = self.next_reference();
        self.stream
            .send_json(serde_json::json!({
                "topic": self.topic,
                "event": "broadcast",
                "payload": { "event": event, "payload": payload },
                "ref": reference,
                "join_ref": self.join_ref,
            }))
            .await
    }

    /// Track a JSON presence state after the channel has been joined.
    pub async fn track(&mut self, state: serde_json::Value) -> Result<(), ClientError> {
        let reference = self.next_reference();
        self.stream
            .send_json(serde_json::json!({
                "topic": self.topic,
                "event": "presence",
                "payload": { "type": "presence", "event": "track", "payload": state },
                "ref": reference,
                "join_ref": self.join_ref,
            }))
            .await
    }

    /// Stop tracking this channel's configured presence key.
    pub async fn untrack(&mut self) -> Result<(), ClientError> {
        let reference = self.next_reference();
        self.stream
            .send_json(serde_json::json!({
                "topic": self.topic,
                "event": "presence",
                "payload": { "type": "presence", "event": "untrack", "payload": {} },
                "ref": reference,
                "join_ref": self.join_ref,
            }))
            .await
    }

    /// Leave the channel using the protocol's phx_leave event.
    pub async fn leave(mut self) -> Result<(), ClientError> {
        let reference = self.next_reference();
        self.stream
            .send_json(serde_json::json!({
                "topic": self.topic,
                "event": "phx_leave",
                "payload": {},
                "ref": reference,
                "join_ref": self.join_ref,
            }))
            .await?;
        self.stream.close().await
    }

    pub async fn close(self) -> Result<(), ClientError> {
        self.stream.close().await
    }

    fn next_reference(&mut self) -> String {
        let reference = self.next_ref.to_string();
        self.next_ref = self.next_ref.saturating_add(1);
        reference
    }
}

/// A table subscription that reconnects and resumes from the last received sequence.
pub struct ResumableRealtimeSubscription {
    client: RymeClient,
    table: String,
    branch: Option<String>,
    from: Option<u64>,
    cursor: Option<u64>,
    stream: Option<RealtimeSubscription>,
    reconnect_delay: Duration,
    closed: bool,
}

impl ResumableRealtimeSubscription {
    const INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(250);
    const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(5);
    const MAX_RECONNECT_ATTEMPTS: usize = 8;

    /// Receive the next change, reconnecting with an exact sequence cursor if needed.
    pub async fn recv(&mut self) -> Result<Option<String>, ClientError> {
        let mut attempts = 0;
        loop {
            if self.closed {
                return Ok(None);
            }
            let result = match self.stream.as_mut() {
                Some(stream) => stream.recv().await,
                None => Err(ClientError::WebSocket(String::from("subscription is closed"))),
            };
            match result {
                Ok(Some(text)) => {
                    self.update_cursor(&text);
                    return Ok(Some(text));
                }
                Ok(None) | Err(_) => {}
            }
            self.stream.take();
            if self.closed {
                return Ok(None);
            }
            attempts += 1;
            if attempts > Self::MAX_RECONNECT_ATTEMPTS {
                return Err(ClientError::WebSocket(String::from("realtime stream closed")));
            }
            tokio::time::sleep(self.reconnect_delay).await;
            if self.closed {
                return Ok(None);
            }
            match self
                .client
                .subscribe_table(&self.table, self.branch.as_deref(), self.from, self.cursor)
                .await
            {
                Ok(stream) => {
                    self.stream = Some(stream);
                    self.reconnect_delay = Self::INITIAL_RECONNECT_DELAY;
                    attempts = 0;
                }
                Err(error) => {
                    if attempts >= Self::MAX_RECONNECT_ATTEMPTS {
                        return Err(error);
                    }
                    self.reconnect_delay =
                        (self.reconnect_delay * 2).min(Self::MAX_RECONNECT_DELAY);
                }
            }
        }
    }

    fn update_cursor(&mut self, text: &str) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else { return };
        let Some(sequence) = value.get("sequence").and_then(serde_json::Value::as_u64) else {
            return;
        };
        self.cursor = Some(self.cursor.map_or(sequence, |current| current.max(sequence)));
    }

    pub async fn close(mut self) -> Result<(), ClientError> {
        self.closed = true;
        match self.stream.take() {
            Some(stream) => stream.close().await,
            None => Ok(()),
        }
    }
}

/// A durable topic subscription that reconnects from the next cursor after a disconnect.
pub struct ResumableDurableTopicSubscription {
    client: RymeClient,
    partition: String,
    cursor: Option<u64>,
    stream: Option<RealtimeSubscription>,
    reconnect_delay: Duration,
    closed: bool,
}

impl ResumableDurableTopicSubscription {
    const INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(250);
    const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(5);
    const MAX_RECONNECT_ATTEMPTS: usize = 8;

    pub async fn recv(&mut self) -> Result<Option<String>, ClientError> {
        let mut attempts = 0;
        loop {
            if self.closed {
                return Ok(None);
            }
            let result = match self.stream.as_mut() {
                Some(stream) => stream.recv().await,
                None => Err(ClientError::WebSocket(String::from("subscription is closed"))),
            };
            match result {
                Ok(Some(text)) => {
                    self.update_cursor(&text);
                    return Ok(Some(text));
                }
                Ok(None) | Err(_) => {}
            }
            self.stream.take();
            attempts += 1;
            if attempts > Self::MAX_RECONNECT_ATTEMPTS {
                return Err(ClientError::WebSocket(String::from("topic stream closed")));
            }
            tokio::time::sleep(self.reconnect_delay).await;
            if self.closed {
                return Ok(None);
            }
            match self.client.subscribe_durable_topic(&self.partition, self.cursor).await {
                Ok(stream) => {
                    self.stream = Some(stream);
                    self.reconnect_delay = Self::INITIAL_RECONNECT_DELAY;
                    attempts = 0;
                }
                Err(error) => {
                    if attempts >= Self::MAX_RECONNECT_ATTEMPTS {
                        return Err(error);
                    }
                    self.reconnect_delay =
                        (self.reconnect_delay * 2).min(Self::MAX_RECONNECT_DELAY);
                }
            }
        }
    }

    fn update_cursor(&mut self, text: &str) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else { return };
        let Some(cursor) = value.get("cursor").and_then(serde_json::Value::as_u64) else {
            return;
        };
        let next = cursor.saturating_add(1);
        self.cursor = Some(self.cursor.map_or(next, |current| current.max(next)));
    }

    pub async fn close(mut self) -> Result<(), ClientError> {
        self.closed = true;
        match self.stream.take() {
            Some(stream) => stream.close().await,
            None => Ok(()),
        }
    }
}

impl RymeClient {
    pub fn new(base: String, api_key: String) -> Self {
        Self { base, api_key, http: reqwest::Client::new() }
    }

    fn auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if self.api_key.is_empty() {
            request
        } else {
            request.header("authorization", format!("Bearer {}", self.api_key))
        }
    }

    fn websocket_url(
        &self,
        path: &str,
        params: &[(String, String)],
    ) -> Result<reqwest::Url, ClientError> {
        let mut url = reqwest::Url::parse(&self.base).map_err(|e| ClientError::Http(e.to_string()))?;
        let scheme = match url.scheme() {
            "https" => "wss",
            "http" => "ws",
            other => other,
        }
        .to_string();
        url.set_scheme(&scheme).map_err(|_| ClientError::Http(String::from("websocket scheme")))?;
        url.set_path(path);
        url.set_query(None);
        {
            let mut query = url.query_pairs_mut();
            for (key, value) in params {
                query.append_pair(key, value);
            }
            if !self.api_key.is_empty() {
                query.append_pair("api_key", &self.api_key);
            }
        }
        Ok(url)
    }

    async fn subscribe(
        &self,
        path: &str,
        params: Vec<(String, String)>,
    ) -> Result<RealtimeSubscription, ClientError> {
        let url = self.websocket_url(path, &params)?;
        let (stream, _) = connect_async(url)
            .await
            .map_err(|error| ClientError::WebSocket(error.to_string()))?;
        Ok(RealtimeSubscription { stream })
    }

    async fn check(response: reqwest::Response) -> Result<serde_json::Value, ClientError> {
        let status = response.status().as_u16();
        let text = response.text().await.map_err(|e| ClientError::Http(e.to_string()))?;
        if !(200..300).contains(&status) {
            return Err(ClientError::Status(status, text));
        }
        if text.is_empty() {
            return Ok(serde_json::Value::Null);
        }
        serde_json::from_str(&text)
            .map_err(|_| ClientError::Status(status, text.clone()))
            .or(Ok(serde_json::Value::String(text)))
    }

    pub async fn health(&self) -> Result<bool, ClientError> {
        let response = self
            .http
            .get(format!("{}/health", self.base))
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        Ok(response.status().is_success())
    }

    pub async fn subscribe_table(
        &self,
        table: &str,
        branch: Option<&str>,
        from: Option<u64>,
        from_sequence: Option<u64>,
    ) -> Result<RealtimeSubscription, ClientError> {
        let mut params = vec![(String::from("table"), table.to_string())];
        if let Some(branch) = branch {
            params.push((String::from("branch"), branch.to_string()));
        }
        if let Some(from) = from {
            params.push((String::from("from"), from.to_string()));
        }
        if let Some(sequence) = from_sequence {
            params.push((String::from("from_sequence"), sequence.to_string()));
        }
        self.subscribe("/v1/stream", params).await
    }

    /// Open a table stream that automatically resumes after a disconnect.
    pub async fn subscribe_table_resumable(
        &self,
        table: &str,
        branch: Option<&str>,
        from: Option<u64>,
        from_sequence: Option<u64>,
    ) -> Result<ResumableRealtimeSubscription, ClientError> {
        let stream = self.subscribe_table(table, branch, from, from_sequence).await?;
        Ok(ResumableRealtimeSubscription {
            client: self.clone(),
            table: table.to_string(),
            branch: branch.map(str::to_string),
            from,
            cursor: from_sequence,
            stream: Some(stream),
            reconnect_delay: ResumableRealtimeSubscription::INITIAL_RECONNECT_DELAY,
            closed: false,
        })
    }

    pub async fn subscribe_broadcast(
        &self,
        channel: &str,
    ) -> Result<RealtimeSubscription, ClientError> {
        self.subscribe(
            &format!("/v1/broadcast/{}", utf8_percent_encode(channel, NON_ALPHANUMERIC)),
            Vec::new(),
        )
        .await
    }

    pub async fn subscribe_presence(
        &self,
        channel: &str,
    ) -> Result<RealtimeSubscription, ClientError> {
        self.subscribe(
            &format!("/v1/presence/{}/stream", utf8_percent_encode(channel, NON_ALPHANUMERIC)),
            Vec::new(),
        )
        .await
    }

    pub async fn subscribe_durable_topic(
        &self,
        partition: &str,
        from: Option<u64>,
    ) -> Result<RealtimeSubscription, ClientError> {
        let mut params = Vec::new();
        if let Some(from) = from {
            params.push((String::from("from"), from.to_string()));
        }
        self.subscribe(
            &format!(
                "/v1/topics/{}/stream",
                utf8_percent_encode(partition, NON_ALPHANUMERIC)
            ),
            params,
        )
        .await
    }

    pub async fn subscribe_durable_topic_resumable(
        &self,
        partition: &str,
        from: Option<u64>,
    ) -> Result<ResumableDurableTopicSubscription, ClientError> {
        let stream = self.subscribe_durable_topic(partition, from).await?;
        Ok(ResumableDurableTopicSubscription {
            client: self.clone(),
            partition: partition.to_string(),
            cursor: from,
            stream: Some(stream),
            reconnect_delay: ResumableDurableTopicSubscription::INITIAL_RECONNECT_DELAY,
            closed: false,
        })
    }

    pub async fn subscribe_query(
        &self,
        table: &str,
        branch: Option<&str>,
        limit: Option<usize>,
    ) -> Result<RealtimeSubscription, ClientError> {
        let mut params = vec![(String::from("table"), table.to_string())];
        if let Some(branch) = branch {
            params.push((String::from("branch"), branch.to_string()));
        }
        if let Some(limit) = limit {
            params.push((String::from("limit"), limit.to_string()));
        }
        self.subscribe("/v1/query-stream", params).await
    }

    /// Open a Supabase-compatible channel and send its phx_join frame.
    pub async fn subscribe_supabase_channel(
        &self,
        channel: &str,
        options: SupabaseChannelOptions,
    ) -> Result<SupabaseChannel, ClientError> {
        let mut params = vec![(String::from("vsn"), String::from("1.0.0"))];
        if !self.api_key.is_empty() {
            params.push((String::from("apikey"), self.api_key.clone()));
        }
        let mut stream = self.subscribe("/realtime/v1/websocket", params).await?;
        let topic = if channel.starts_with("realtime:") {
            channel.to_string()
        } else {
            format!("realtime:{channel}")
        };
        let join_ref = String::from("1");
        let mut config = serde_json::json!({
            "broadcast": {
                "ack": options.broadcast_ack,
                "self": options.broadcast_self,
            },
            "postgres_changes": options.postgres_changes,
        });
        if let Some(key) = options.presence_key {
            config["presence"] = serde_json::json!({ "enabled": true, "key": key });
        }
        stream
            .send_json(serde_json::json!({
                "topic": topic,
                "event": "phx_join",
                "payload": { "config": config },
                "ref": join_ref,
                "join_ref": join_ref,
            }))
            .await?;
        Ok(SupabaseChannel { stream, topic, join_ref, next_ref: 2 })
    }

    pub async fn kv_get(&self, table: &str, key: &str) -> Result<Vec<u8>, ClientError> {
        let response = self
            .auth(self.http.get(format!("{}/v1/kv/{table}/{key}", self.base)))
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        let status = response.status().as_u16();
        let bytes = response.bytes().await.map_err(|e| ClientError::Http(e.to_string()))?;
        if !(200..300).contains(&status) {
            return Err(ClientError::Status(status, String::from_utf8_lossy(&bytes).to_string()));
        }
        Ok(bytes.to_vec())
    }

    pub async fn kv_put(&self, table: &str, key: &str, value: Vec<u8>) -> Result<u64, ClientError> {
        let response = self
            .auth(self.http.put(format!("{}/v1/kv/{table}/{key}", self.base)).body(value))
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        match Self::check(response).await? {
            serde_json::Value::Object(mut map) => map
                .remove("commit")
                .and_then(|v| v.as_u64())
                .ok_or(ClientError::Status(200, String::from("commit"))),
            _ => Err(ClientError::Status(200, String::from("commit"))),
        }
    }

    pub async fn sql(&self, sql: &str) -> Result<serde_json::Value, ClientError> {
        let response = self
            .auth(self.http.post(format!("{}/v1/sql", self.base)))
            .json(&serde_json::json!({ "sql": sql }))
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        Self::check(response).await
    }

    pub async fn sql_copy(&self, table: &str, rows: Vec<CopyRow>) -> Result<u64, ClientError> {
        let response = self
            .auth(self.http.post(format!("{}/v1/sql/copy", self.base)))
            .json(&serde_json::json!({ "table": table, "rows": rows }))
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        match Self::check(response).await? {
            serde_json::Value::Object(mut map) => map
                .remove("rows")
                .and_then(|v| v.as_u64())
                .ok_or(ClientError::Status(200, String::from("rows"))),
            _ => Err(ClientError::Status(200, String::from("rows"))),
        }
    }

    pub async fn explain(&self, sql: &str) -> Result<String, ClientError> {
        let response = self
            .auth(self.http.post(format!("{}/v1/sql/explain", self.base)))
            .json(&serde_json::json!({ "sql": sql }))
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        match Self::check(response).await? {
            serde_json::Value::Object(mut map) => map
                .remove("plan")
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .ok_or(ClientError::Status(200, String::from("plan"))),
            _ => Err(ClientError::Status(200, String::from("plan"))),
        }
    }

    pub async fn rest_list(
        &self,
        table: &str,
        query: &str,
    ) -> Result<serde_json::Value, ClientError> {
        let url = if query.is_empty() {
            format!("{}/rest/v1/{table}", self.base)
        } else {
            format!("{}/rest/v1/{table}?{query}", self.base)
        };
        let response = self
            .auth(self.http.get(url))
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        Self::check(response).await
    }

    pub async fn rest_insert(
        &self,
        table: &str,
        row: &serde_json::Value,
    ) -> Result<serde_json::Value, ClientError> {
        let response = self
            .auth(self.http.post(format!("{}/rest/v1/{table}", self.base)))
            .json(row)
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        Self::check(response).await
    }

    pub async fn rest_upsert(
        &self,
        table: &str,
        row: &serde_json::Value,
    ) -> Result<serde_json::Value, ClientError> {
        self.rest_insert(table, row).await
    }

    pub async fn rest_update(
        &self,
        table: &str,
        query: &str,
        changes: &serde_json::Value,
    ) -> Result<serde_json::Value, ClientError> {
        let suffix = if query.is_empty() {
            String::new()
        } else {
            format!("?{}", query.trim_start_matches('?'))
        };
        let response = self
            .auth(self.http.patch(format!("{}/rest/v1/{table}{suffix}", self.base)))
            .json(changes)
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        Self::check(response).await
    }

    pub async fn rest_delete_where(
        &self,
        table: &str,
        query: &str,
    ) -> Result<serde_json::Value, ClientError> {
        let suffix = if query.is_empty() {
            String::new()
        } else {
            format!("?{}", query.trim_start_matches('?'))
        };
        let response = self
            .auth(self.http.delete(format!("{}/rest/v1/{table}{suffix}", self.base)))
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        Self::check(response).await
    }

    pub async fn graphql(&self, query: &str) -> Result<serde_json::Value, ClientError> {
        let response = self
            .auth(self.http.post(format!("{}/graphql", self.base)))
            .json(&serde_json::json!({ "query": query }))
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        Self::check(response).await
    }

    pub async fn metering(&self) -> Result<serde_json::Value, ClientError> {
        let response = self
            .auth(self.http.get(format!("{}/v1/metering", self.base)))
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        Self::check(response).await
    }

    pub async fn slow_log(
        &self,
        limit: Option<usize>,
        table: Option<&str>,
    ) -> Result<serde_json::Value, ClientError> {
        let mut url = reqwest::Url::parse(&format!("{}/v1/observe/slow", self.base))
            .map_err(|e| ClientError::Http(e.to_string()))?;
        if let Some(limit) = limit {
            url.query_pairs_mut().append_pair("limit", &limit.to_string());
        }
        if let Some(table) = table {
            url.query_pairs_mut().append_pair("table", table);
        }
        let response =
            self.auth(self.http.get(url)).send().await.map_err(|e| ClientError::Http(e.to_string()))?;
        Self::check(response).await
    }

    pub async fn traces(
        &self,
        limit: Option<usize>,
        name: Option<&str>,
        table: Option<&str>,
    ) -> Result<serde_json::Value, ClientError> {
        let mut url = reqwest::Url::parse(&format!("{}/v1/traces", self.base))
            .map_err(|e| ClientError::Http(e.to_string()))?;
        if let Some(limit) = limit {
            url.query_pairs_mut().append_pair("limit", &limit.to_string());
        }
        if let Some(name) = name {
            url.query_pairs_mut().append_pair("name", name);
        }
        if let Some(table) = table {
            url.query_pairs_mut().append_pair("table", table);
        }
        let response =
            self.auth(self.http.get(url)).send().await.map_err(|e| ClientError::Http(e.to_string()))?;
        Self::check(response).await
    }

    async fn post_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, ClientError> {
        let response = self
            .auth(self.http.post(format!("{}{}", self.base, path)))
            .json(&body)
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        Self::check(response).await
    }

    async fn post_path(&self, path: &str) -> Result<serde_json::Value, ClientError> {
        let response = self
            .auth(self.http.post(format!("{}{}", self.base, path)))
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        Self::check(response).await
    }

    async fn delete_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, ClientError> {
        let response = self
            .auth(self.http.delete(format!("{}{}", self.base, path)))
            .json(&body)
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        Self::check(response).await
    }

    async fn get_path(&self, path: &str) -> Result<serde_json::Value, ClientError> {
        let response = self
            .auth(self.http.get(format!("{}{}", self.base, path)))
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        Self::check(response).await
    }

    pub async fn qos(&self) -> Result<serde_json::Value, ClientError> {
        self.get_path("/v1/qos").await
    }

    pub async fn qos_set_tier(
        &self,
        tenant: &str,
        tier: &str,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/qos/tier", serde_json::json!({ "tenant": tenant, "tier": tier })).await
    }

    pub async fn auth_register(
        &self,
        id: &str,
        password: &str,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/auth/register", serde_json::json!({ "id": id, "password": password }))
            .await
    }

    pub async fn auth_verify(
        &self,
        id: &str,
        password: &str,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/auth/verify", serde_json::json!({ "id": id, "password": password }))
            .await
    }

    pub async fn auth_token(
        &self,
        id: &str,
        password: &str,
        code: Option<&str>,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/auth/token", serde_json::json!({ "id": id, "password": password, "code": code }))
            .await
    }

    pub async fn auth_revoke(&self, key: &str) -> Result<serde_json::Value, ClientError> {
        self.delete_json("/v1/auth/keys", serde_json::json!({ "key": key })).await
    }

    pub async fn otp_setup(&self, id: &str) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/auth/otp/setup", serde_json::json!({ "id": id })).await
    }

    pub async fn otp_verify(&self, id: &str, code: &str) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/auth/otp/verify", serde_json::json!({ "id": id, "code": code })).await
    }

    pub async fn passkey_challenge(&self, user: &str) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/auth/passkey/challenge", serde_json::json!({ "user": user })).await
    }

    pub async fn passkey_register(
        &self,
        user: &str,
        credential_id: &str,
        public_key: &str,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/auth/passkey/register",
            serde_json::json!({ "user": user, "credential_id": credential_id, "public_key": public_key }),
        )
        .await
    }

    pub async fn passkey_verify(
        &self,
        user: &str,
        credential_id: &str,
        authenticator_data: &str,
        client_data_json: &str,
        signature: &str,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/auth/passkey/verify",
            serde_json::json!({
                "user": user,
                "credential_id": credential_id,
                "authenticator_data": authenticator_data,
                "client_data_json": client_data_json,
                "signature": signature,
            }),
        )
        .await
    }

    pub async fn mask_set(
        &self,
        table: &str,
        fields: Vec<String>,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/auth/mask", serde_json::json!({ "table": table, "fields": fields }))
            .await
    }

    pub async fn presence_join(
        &self,
        channel: &str,
        member: &str,
    ) -> Result<serde_json::Value, ClientError> {
        self.presence_join_with_state(channel, member, serde_json::Value::Null, None).await
    }

    pub async fn presence_join_with_state(
        &self,
        channel: &str,
        member: &str,
        state: serde_json::Value,
        ttl_secs: Option<u64>,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/presence/join",
            serde_json::json!({
                "channel": channel,
                "member": member,
                "state": state,
                "ttl_secs": ttl_secs,
            }),
        )
        .await
    }

    pub async fn presence_leave(
        &self,
        channel: &str,
        member: &str,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/presence/leave",
            serde_json::json!({ "channel": channel, "member": member }),
        )
        .await
    }

    pub async fn presence_list(&self, channel: &str) -> Result<serde_json::Value, ClientError> {
        self.get_path(&format!(
            "/v1/presence/{}",
            utf8_percent_encode(channel, NON_ALPHANUMERIC)
        ))
        .await
    }

    pub async fn broadcast(
        &self,
        channel: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/broadcast",
            serde_json::json!({ "channel": channel, "payload": payload }),
        )
        .await
    }

    pub async fn topic_append(
        &self,
        partition: &str,
        key: &str,
        value: &str,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/topics/append",
            serde_json::json!({ "partition": partition, "key": key, "value": value }),
        )
        .await
    }

    pub async fn topic_read(
        &self,
        partition: &str,
        from: u64,
    ) -> Result<serde_json::Value, ClientError> {
        self.get_path(&format!("/v1/topics/read?partition={partition}&from={from}")).await
    }

    pub async fn branch_list(&self) -> Result<serde_json::Value, ClientError> {
        self.get_path("/v1/branches").await
    }

    pub async fn branch_reset(
        &self,
        id: &str,
        base_commit_ts: u64,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            &format!("/v1/branches/{id}/reset"),
            serde_json::json!({ "base_commit_ts": base_commit_ts }),
        )
        .await
    }

    pub async fn branch_promote(&self, id: &str) -> Result<serde_json::Value, ClientError> {
        self.post_json(&format!("/v1/branches/{id}/promote"), serde_json::Value::Null).await
    }

    pub async fn branch_diff(
        &self,
        id: &str,
        against: &str,
    ) -> Result<serde_json::Value, ClientError> {
        self.get_path(&format!("/v1/branches/{id}/diff?against={against}")).await
    }

    pub async fn billing_summary(&self) -> Result<serde_json::Value, ClientError> {
        self.get_path("/v1/billing/summary").await
    }

    pub async fn index_stats(&self) -> Result<serde_json::Value, ClientError> {
        self.get_path("/v1/index/stats").await
    }

    pub async fn billing_invoice(&self, tenant: &str) -> Result<serde_json::Value, ClientError> {
        let path = if tenant.is_empty() {
            String::from("/v1/billing/invoice")
        } else {
            format!("/v1/billing/invoice?tenant={tenant}")
        };
        self.get_path(&path).await
    }

    pub async fn regions(&self) -> Result<serde_json::Value, ClientError> {
        self.get_path("/v1/regions").await
    }

    pub async fn migrate_supabase(&self, dump: &str) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/migrate/supabase", serde_json::json!({ "dump": dump })).await
    }

    pub async fn backup_verify(&self, backup_id: &str) -> Result<serde_json::Value, ClientError> {
        self.get_path(&format!("/v1/backups/verify?backup_id={backup_id}")).await
    }

    pub async fn backup_drill(&self) -> Result<serde_json::Value, ClientError> {
        self.get_path("/v1/backups/drill").await
    }

    pub async fn backup_copy(&self, backup_id: &str) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/backups/copy", serde_json::json!({ "backup_id": backup_id })).await
    }

    pub async fn checkpoint(
        &self,
        manifest_id: Option<&str>,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/backups/checkpoint", serde_json::json!({ "manifest_id": manifest_id }))
            .await
    }

    pub async fn snapshot(&self) -> Result<serde_json::Value, ClientError> {
        self.post_path("/v1/snapshots").await
    }

    pub async fn latest_checkpoint(&self) -> Result<serde_json::Value, ClientError> {
        self.get_path("/v1/backups/latest").await
    }

    pub async fn pitr(&self, target: u64) -> Result<serde_json::Value, ClientError> {
        self.get_path(&format!("/v1/backups/pitr?target={target}")).await
    }

    pub async fn restore(&self, target: u64) -> Result<serde_json::Value, ClientError> {
        self.post_path(&format!("/v1/backups/restore?target={target}")).await
    }

    pub async fn archive(&self, backup_id: Option<&str>) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/backups/archive", serde_json::json!({ "backup_id": backup_id })).await
    }

    pub async fn archives(&self) -> Result<serde_json::Value, ClientError> {
        self.get_path("/v1/backups/archives").await
    }

    pub async fn shard_layout(&self) -> Result<serde_json::Value, ClientError> {
        self.get_path("/v1/shards").await
    }

    pub async fn shard_move(
        &self,
        table: &str,
        target: Option<u64>,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/shards/move", serde_json::json!({ "table": table, "target": target }))
            .await
    }

    pub async fn ranges(&self, key: Option<&str>) -> Result<serde_json::Value, ClientError> {
        match key {
            Some(key) => {
                let mut url = reqwest::Url::parse(&format!("{}/v1/ranges", self.base))
                    .map_err(|e| ClientError::Http(e.to_string()))?;
                url.query_pairs_mut().append_pair("key", key);
                let response = self
                    .auth(self.http.get(url))
                    .send()
                    .await
                    .map_err(|e| ClientError::Http(e.to_string()))?;
                Self::check(response).await
            }
            None => self.get_path("/v1/ranges").await,
        }
    }

    pub async fn range_split(
        &self,
        id: &str,
        mid: &str,
        left_id: &str,
        right_id: &str,
        expected_epoch: u64,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/ranges/split",
            serde_json::json!({
                "id": id,
                "mid": mid,
                "left_id": left_id,
                "right_id": right_id,
                "expected_epoch": expected_epoch,
            }),
        )
        .await
    }

    pub async fn range_merge(
        &self,
        left_id: &str,
        right_id: &str,
        merged_id: &str,
        expected_left_epoch: u64,
        expected_right_epoch: u64,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/ranges/merge",
            serde_json::json!({
                "left_id": left_id,
                "right_id": right_id,
                "merged_id": merged_id,
                "expected_left_epoch": expected_left_epoch,
                "expected_right_epoch": expected_right_epoch,
            }),
        )
        .await
    }

    pub async fn range_autosplit(
        &self,
        min_writes: Option<u64>,
    ) -> Result<serde_json::Value, ClientError> {
        match min_writes {
            Some(min) => self.post_path(&format!("/v1/ranges/autosplit?min_writes={min}")).await,
            None => self.post_path("/v1/ranges/autosplit").await,
        }
    }

    pub async fn range_loads(&self) -> Result<serde_json::Value, ClientError> {
        self.get_path("/v1/ranges/loads").await
    }

    pub async fn migrate_neon(&self, branches: &str) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/migrate/neon", serde_json::json!({ "branches": branches })).await
    }

    pub async fn migrate_apply(
        &self,
        id: &str,
        sql: &str,
        author: Option<&str>,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/migrate/apply",
            serde_json::json!({ "id": id, "sql": sql, "author": author }),
        )
        .await
    }

    pub async fn migrate_ledger(&self) -> Result<serde_json::Value, ClientError> {
        self.get_path("/v1/migrate/ledger").await
    }

    pub async fn vector_ann_search(
        &self,
        table: &str,
        vector: Vec<f32>,
        top_k: usize,
        ef: usize,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/vector/ann-search",
            serde_json::json!({ "table": table, "vector": vector, "top_k": top_k, "ef": ef }),
        )
        .await
    }

    pub async fn oidc_login(
        &self,
        redirect_uri: &str,
        state: Option<&str>,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/auth/oidc/login",
            serde_json::json!({ "redirect_uri": redirect_uri, "state": state }),
        )
        .await
    }

    pub async fn oidc_token(&self, id_token: &str) -> Result<serde_json::Value, ClientError> {
        self.post_json("/v1/auth/oidc/token", serde_json::json!({ "id_token": id_token })).await
    }

    pub async fn vector_upsert(
        &self,
        table: &str,
        id: &str,
        vector: Vec<f32>,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/vector/upsert",
            serde_json::json!({ "table": table, "id": id, "vector": vector }),
        )
        .await
    }

    pub async fn vector_search(
        &self,
        table: &str,
        vector: Vec<f32>,
        top_k: usize,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/vector/search",
            serde_json::json!({ "table": table, "vector": vector, "top_k": top_k }),
        )
        .await
    }

    pub async fn text_index(
        &self,
        table: &str,
        id: &str,
        text: &str,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/text/index",
            serde_json::json!({ "table": table, "id": id, "text": text }),
        )
        .await
    }

    pub async fn text_search(
        &self,
        table: &str,
        query: &str,
        top_k: usize,
    ) -> Result<serde_json::Value, ClientError> {
        self.post_json(
            "/v1/text/search",
            serde_json::json!({ "table": table, "query": query, "top_k": top_k }),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Debug, Default, Clone)]
    struct Seen {
        method: String,
        path: String,
        body: String,
    }

    async fn stub() -> (String, Arc<Mutex<Seen>>) {
        let seen = Arc::new(Mutex::new(Seen::default()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = seen.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else { return };
                let state = state.clone();
                tokio::spawn(async move {
                    let mut raw = Vec::new();
                    let mut chunk = vec![0u8; 4096];
                    loop {
                        match socket.read(&mut chunk).await {
                            Ok(0) => break,
                            Ok(read) => {
                                raw.extend_from_slice(&chunk[..read]);
                                if raw.windows(4).any(|w| w == b"\r\n\r\n") {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    let text = String::from_utf8_lossy(&raw).into_owned();
                    let mut lines = text.lines();
                    let head = lines.next().unwrap_or("").to_string();
                    let mut parts = head.split_whitespace();
                    let method = parts.next().unwrap_or("").to_string();
                    let path = parts.next().unwrap_or("").to_string();
                    let mut length = 0usize;
                    for line in text.lines().skip(1).take_while(|line| !line.is_empty()) {
                        if let Some((name, value)) = line.split_once(':') {
                            if name.trim().eq_ignore_ascii_case("content-length") {
                                length = value.trim().parse().unwrap_or(0);
                            }
                        }
                    }
                    let header_end =
                        raw.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4).unwrap_or(0);
                    let mut body = raw.get(header_end..).map(|s| s.to_vec()).unwrap_or_default();
                    while body.len() < length {
                        match socket.read(&mut chunk).await {
                            Ok(0) => break,
                            Ok(read) => body.extend_from_slice(&chunk[..read]),
                            Err(_) => break,
                        }
                    }
                    if let Ok(mut guard) = state.lock() {
                        *guard = Seen {
                            method,
                            path,
                            body: String::from_utf8_lossy(&body).into_owned(),
                        };
                    }
                    let reply = b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 11\r\nconnection: close\r\n\r\n{\"ok\":true}";
                    let _ = socket.write_all(reply).await;
                });
            }
        });
        (base, seen)
    }

    fn seen(state: &Arc<Mutex<Seen>>) -> Seen {
        state.lock().unwrap().clone()
    }

    #[test]
    fn websocket_urls_use_ws_and_encode_replay_params() {
        let client = RymeClient::new(String::from("https://db.example.test"), String::from("key/1"));
        let url = client
            .websocket_url(
                "/v1/stream",
                &[
                    (String::from("table"), String::from("room messages")),
                    (String::from("from_sequence"), String::from("7")),
                ],
            )
            .unwrap();
        assert_eq!(url.scheme(), "wss");
        assert_eq!(url.as_str(), "wss://db.example.test/v1/stream?table=room+messages&from_sequence=7&api_key=key%2F1");
    }

    #[test]
    fn resumable_subscription_keeps_the_highest_sequence() {
        let mut subscription = ResumableRealtimeSubscription {
            client: RymeClient::new(String::from("http://db.example.test"), String::new()),
            table: String::from("docs"),
            branch: None,
            from: None,
            cursor: Some(4),
            stream: None,
            reconnect_delay: ResumableRealtimeSubscription::INITIAL_RECONNECT_DELAY,
            closed: false,
        };
        subscription.update_cursor(r#"{"sequence":9}"#);
        subscription.update_cursor(r#"{"sequence":3}"#);
        assert_eq!(subscription.cursor, Some(9));
    }

    #[tokio::test]
    async fn backup_lifecycle_paths_and_bodies() {
        let (base, state) = stub().await;
        let client = RymeClient::new(base, String::from("k"));
        client.checkpoint(None).await.unwrap();
        assert_eq!(seen(&state).path, "/v1/backups/checkpoint");
        assert_eq!(seen(&state).body, "{\"manifest_id\":null}");
        client.checkpoint(Some("m1")).await.unwrap();
        assert_eq!(seen(&state).body, "{\"manifest_id\":\"m1\"}");
        client.snapshot().await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("POST", "/v1/snapshots"));
        client.latest_checkpoint().await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("GET", "/v1/backups/latest"));
        client.pitr(1735689600).await.unwrap();
        assert_eq!(seen(&state).path, "/v1/backups/pitr?target=1735689600");
        client.restore(1735689600).await.unwrap();
        let got = seen(&state);
        assert_eq!(
            (got.method.as_str(), got.path.as_str()),
            ("POST", "/v1/backups/restore?target=1735689600")
        );
        client.archive(None).await.unwrap();
        assert_eq!(seen(&state).body, "{\"backup_id\":null}");
        client.archives().await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("GET", "/v1/backups/archives"));
        client.backup_verify("nightly-042").await.unwrap();
        assert_eq!(seen(&state).path, "/v1/backups/verify?backup_id=nightly-042");
        client.backup_drill().await.unwrap();
        assert_eq!(seen(&state).path, "/v1/backups/drill");
        client.backup_copy("nightly-042").await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("POST", "/v1/backups/copy"));
        assert_eq!(got.body, "{\"backup_id\":\"nightly-042\"}");
    }

    #[tokio::test]
    async fn rest_crud_paths_and_bodies() {
        let (base, state) = stub().await;
        let client = RymeClient::new(base, String::from("k"));
        client
            .rest_insert("people", &serde_json::json!([{ "id": "p1" }]))
            .await
            .unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("POST", "/rest/v1/people"));
        assert_eq!(got.body, r#"[{"id":"p1"}]"#);
        client
            .rest_upsert("people", &serde_json::json!({ "id": "p1" }))
            .await
            .unwrap();
        assert_eq!(seen(&state).method, "POST");
        client
            .rest_update("people", "id=eq.p1", &serde_json::json!({ "name": "Grace" }))
            .await
            .unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("PATCH", "/rest/v1/people?id=eq.p1"));
        client.rest_delete_where("people", "id=eq.p1").await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("DELETE", "/rest/v1/people?id=eq.p1"));
    }

    #[tokio::test]
    async fn presence_controls_send_state_ttl_and_encoded_channels() {
        let (base, state) = stub().await;
        let client = RymeClient::new(base, String::from("k"));
        client
            .presence_join_with_state(
                "room/one",
                "ada",
                serde_json::json!({ "typing": true }),
                Some(30),
            )
            .await
            .unwrap();
        let got = seen(&state);
        assert_eq!(got.path, "/v1/presence/join");
        assert_eq!(
            got.body,
            r#"{"channel":"room/one","member":"ada","state":{"typing":true},"ttl_secs":30}"#
        );
        client.presence_leave("room/one", "ada").await.unwrap();
        assert_eq!(seen(&state).path, "/v1/presence/leave");
        client.presence_list("room/one").await.unwrap();
        assert_eq!(seen(&state).path, "/v1/presence/room%2Fone");
    }

    #[tokio::test]
    async fn slow_log_paths() {
        let (base, state) = stub().await;
        let client = RymeClient::new(base, String::from("k"));
        client.slow_log(None, None).await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("GET", "/v1/observe/slow"));
        client.slow_log(Some(5), None).await.unwrap();
        assert_eq!(seen(&state).path, "/v1/observe/slow?limit=5");
        client.slow_log(Some(5), Some("docs")).await.unwrap();
        assert_eq!(seen(&state).path, "/v1/observe/slow?limit=5&table=docs");
    }

    #[tokio::test]
    async fn traces_paths() {
        let (base, state) = stub().await;
        let client = RymeClient::new(base, String::from("k"));
        client.traces(None, None, None).await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("GET", "/v1/traces"));
        client.traces(Some(5), None, None).await.unwrap();
        assert_eq!(seen(&state).path, "/v1/traces?limit=5");
        client.traces(Some(5), Some("kv_put"), Some("docs")).await.unwrap();
        assert_eq!(seen(&state).path, "/v1/traces?limit=5&name=kv_put&table=docs");
    }

    #[tokio::test]
    async fn auth_token_paths_and_bodies() {
        let (base, state) = stub().await;
        let client = RymeClient::new(base, String::from("k"));
        client.auth_token("ada", "correct-horse", None).await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("POST", "/v1/auth/token"));
        assert_eq!(got.body, "{\"code\":null,\"id\":\"ada\",\"password\":\"correct-horse\"}");
        client.auth_token("ada", "correct-horse", Some("123456")).await.unwrap();
        let got = seen(&state);
        assert_eq!(got.body, "{\"code\":\"123456\",\"id\":\"ada\",\"password\":\"correct-horse\"}");
        client.auth_revoke("ryme_deadbeef").await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("DELETE", "/v1/auth/keys"));
        assert_eq!(got.body, "{\"key\":\"ryme_deadbeef\"}");
        client.passkey_register("ada", "cred-9", "cHVi").await.unwrap();
        let got = seen(&state);
        assert_eq!(
            (got.method.as_str(), got.path.as_str()),
            ("POST", "/v1/auth/passkey/register")
        );
        assert_eq!(
            got.body,
            "{\"credential_id\":\"cred-9\",\"public_key\":\"cHVi\",\"user\":\"ada\"}"
        );
        client
            .passkey_verify("ada", "cred-9", "YXV0aA", "Y2xpZW50", "c2ln")
            .await
            .unwrap();
        let got = seen(&state);
        assert_eq!(got.path.as_str(), "/v1/auth/passkey/verify");
        client.passkey_register("ada", "cred-9", "cHVi").await.unwrap();
        let got = seen(&state);
        assert_eq!(
            (got.method.as_str(), got.path.as_str()),
            ("POST", "/v1/auth/passkey/register")
        );
        client
            .passkey_verify("ada", "cred-9", "YXV0aA", "Y2xpZW50", "c2ln")
            .await
            .unwrap();
        let got = seen(&state);
        assert_eq!(got.path.as_str(), "/v1/auth/passkey/verify");
    }

    #[tokio::test]
    async fn shard_and_range_paths_and_bodies() {
        let (base, state) = stub().await;
        let client = RymeClient::new(base, String::from("k"));
        client.shard_layout().await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("GET", "/v1/shards"));
        client.shard_move("docs", None).await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("POST", "/v1/shards/move"));
        assert_eq!(got.body, "{\"table\":\"docs\",\"target\":null}");
        client.shard_move("docs", Some(2)).await.unwrap();
        assert_eq!(seen(&state).body, "{\"table\":\"docs\",\"target\":2}");
        client.ranges(None).await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("GET", "/v1/ranges"));
        client.ranges(Some("m")).await.unwrap();
        assert_eq!(seen(&state).path, "/v1/ranges?key=m");
        client.range_split("range-0", "m", "range-a", "range-b", 0).await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("POST", "/v1/ranges/split"));
        assert_eq!(
            got.body,
            "{\"expected_epoch\":0,\"id\":\"range-0\",\"left_id\":\"range-a\",\"mid\":\"m\",\"right_id\":\"range-b\"}"
        );
        client.range_merge("range-a", "range-b", "range-c", 1, 1).await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("POST", "/v1/ranges/merge"));
        assert_eq!(
            got.body,
            "{\"expected_left_epoch\":1,\"expected_right_epoch\":1,\"left_id\":\"range-a\",\"merged_id\":\"range-c\",\"right_id\":\"range-b\"}"
        );
        client.range_autosplit(None).await.unwrap();
        let got = seen(&state);
        assert_eq!(
            (got.method.as_str(), got.path.as_str()),
            ("POST", "/v1/ranges/autosplit")
        );
        client.range_autosplit(Some(10)).await.unwrap();
        let got = seen(&state);
        assert_eq!(got.path.as_str(), "/v1/ranges/autosplit?min_writes=10");
        client.range_loads().await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("GET", "/v1/ranges/loads"));
        client.migrate_apply("m1", "INSERT INTO docs KEY 'k1' VALUE 'v1'", None).await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("POST", "/v1/migrate/apply"));
        assert_eq!(
            got.body,
            "{\"author\":null,\"id\":\"m1\",\"sql\":\"INSERT INTO docs KEY 'k1' VALUE 'v1'\"}"
        );
        client.migrate_ledger().await.unwrap();
        let got = seen(&state);
        assert_eq!((got.method.as_str(), got.path.as_str()), ("GET", "/v1/migrate/ledger"));
    }
}
