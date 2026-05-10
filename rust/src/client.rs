// StreamqClient — main entry point for the streamq Rust SDK.
//
// Uses reqwest for all HTTP calls. A single reqwest::Client is reused
// across all requests for connection pooling, creating a new Client per
// request would defeat HTTP keep-alive and be significantly slower.
//
// All methods are async and return Result<T, Error>. The ? operator works
// throughout because we implement From<reqwest::Error> etc. in error.rs.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use reqwest::{Client, StatusCode};
use serde::Serialize;

use crate::{
    consumer::{Consumer, ConsumerOptions},
    error::{Error, Result},
    models::{BrokerEnvelope, BrokerError, PublishResult, TopicInfo},
};

/// Client for the streamq broker.
///
/// Cheap to clone, the underlying HTTP connection pool is shared.
///
/// # Example
///
/// ```no_run
/// # use streamq::StreamqClient;
/// # #[tokio::main]
/// # async fn main() -> streamq::Result<()> {
/// let client = StreamqClient::new("http://localhost:8080")?;
///
/// client.create_topic("payments").await?;
///
/// let result = client.publish("payments", b"{\"amount\": 100}").await?;
/// println!("published at offset {}", result.offset);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct StreamqClient {
    base_url: String,
    http: Client,
}

impl StreamqClient {
    /// Create a new client connected to the broker at `base_url`.
    ///
    /// Uses `rustls` for TLS, no OpenSSL dependency.
    ///
    /// # Errors
    /// Returns an error if `base_url` is empty or if the HTTP client
    /// fails to initialise (extremely unlikely).
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let base_url = base_url.into();
        if base_url.is_empty() {
            return Err(Error::Other("base_url cannot be empty".to_string()));
        }

        let http = Client::builder()
            // Use rustls so callers don't need OpenSSL installed.
            .use_rustls_tls()
            // Connection pooling is on by default, we keep it.
            .build()?;

        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
        })
    }

    /// Create a new topic on the broker.
    ///
    /// # Errors
    /// - [`Error::Conflict`] if the topic already exists.
    /// - [`Error::Api`] for any other broker error.
    pub async fn create_topic(&self, name: &str) -> Result<()> {
        #[derive(Serialize)]
        struct Body<'a> { name: &'a str }

        self.request_no_body(
            reqwest::Method::POST,
            "/topics",
            Some(&Body { name }),
            StatusCode::CREATED,
        )
        .await
    }

    /// Delete a topic and disconnect all its subscribers.
    ///
    /// # Errors
    /// - [`Error::NotFound`] if the topic does not exist.
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        self.request_no_body::<()>(
            reqwest::Method::DELETE,
            &format!("/topics/{name}"),
            None,
            StatusCode::OK,
        )
        .await
    }

    /// Return stats for all topics. Returns an empty Vec if none exist.
    pub async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        let envelope: BrokerEnvelope<Vec<TopicInfo>> = self
            .request(reqwest::Method::GET, "/topics", None::<&()>, StatusCode::OK)
            .await?;
        Ok(envelope.data.unwrap_or_default())
    }

    /// Return stats for a single topic.
    ///
    /// # Errors
    /// - [`Error::NotFound`] if the topic does not exist.
    pub async fn get_topic(&self, name: &str) -> Result<TopicInfo> {
        let envelope: BrokerEnvelope<TopicInfo> = self
            .request(
                reqwest::Method::GET,
                &format!("/topics/{name}"),
                None::<&()>,
                StatusCode::OK,
            )
            .await?;
        envelope
            .data
            .ok_or_else(|| Error::Other("broker returned no data".to_string()))
    }

    /// Publish a message to the named topic.
    ///
    /// `payload` is raw bytes, base64-encoded automatically for transport.
    ///
    /// Returns a [`PublishResult`] with the assigned offset and server timestamp.
    ///
    /// # Errors
    /// - [`Error::NotFound`] if the topic does not exist.
    pub async fn publish(&self, topic: &str, payload: &[u8]) -> Result<PublishResult> {
        #[derive(Serialize)]
        struct Body { payload: String }

        let encoded = BASE64.encode(payload);

        let envelope: BrokerEnvelope<PublishResult> = self
            .request(
                reqwest::Method::POST,
                &format!("/topics/{topic}/publish"),
                Some(&Body { payload: encoded }),
                StatusCode::ACCEPTED,
            )
            .await?;

        envelope
            .data
            .ok_or_else(|| Error::Other("broker returned no data".to_string()))
    }

    /// Create a [`Consumer`] for the given topic.
    ///
    /// Returns immediately — the connection is established in a background
    /// tokio task when the first `next()` call is made.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use streamq::{StreamqClient, ConsumerOptions};
    /// # #[tokio::main]
    /// # async fn main() -> streamq::Result<()> {
    /// let client = StreamqClient::new("http://localhost:8080")?;
    /// let mut consumer = client.subscribe("payments", ConsumerOptions {
    ///     group: "billing".to_string(),
    ///     from_offset: 0,
    ///     ..Default::default()
    /// });
    ///
    /// while let Some(msg) = consumer.next().await? {
    ///     println!("{:?}", msg.payload);
    ///     consumer.ack(msg.offset);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscribe(&self, topic: &str, opts: ConsumerOptions) -> Consumer {
        Consumer::new(self.base_url.clone(), topic.to_string(), opts)
    }

    /// Make an HTTP request and deserialise the JSON envelope response.
    async fn request<T, B>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&B>,
        expected: StatusCode,
    ) -> Result<BrokerEnvelope<T>>
    where
        T: serde::de::DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.http.request(method, &url);

        if let Some(b) = body {
            req = req.json(b);
        }

        let resp = req.send().await?;
        let status = resp.status();

        if status != expected {
            // Try to extract the broker's error message.
            let err: BrokerError = resp.json().await.unwrap_or(BrokerError { message: None });
            let message = err.message.unwrap_or_else(|| format!("HTTP {status}"));

            return Err(match status {
                StatusCode::NOT_FOUND => Error::NotFound(message),
                StatusCode::CONFLICT => Error::Conflict(message),
                s => Error::Api {
                    status: s.as_u16(),
                    message,
                },
            });
        }

        Ok(resp.json().await?)
    }

    /// Like `request` but discards the response body (for methods that
    /// return no meaningful data, like DELETE).
    async fn request_no_body<B>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&B>,
        expected: StatusCode,
    ) -> Result<()>
    where
        B: Serialize + ?Sized,
    {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.http.request(method, &url);

        if let Some(b) = body {
            req = req.json(b);
        }

        let resp = req.send().await?;
        let status = resp.status();

        if status != expected {
            let err: BrokerError = resp.json().await.unwrap_or(BrokerError { message: None });
            let message = err.message.unwrap_or_else(|| format!("HTTP {status}"));

            return Err(match status {
                StatusCode::NOT_FOUND => Error::NotFound(message),
                StatusCode::CONFLICT => Error::Conflict(message),
                s => Error::Api {
                    status: s.as_u16(),
                    message,
                },
            });
        }

        Ok(())
    }
}
