// Consumer — an async Stream of Messages from a streamq topic.
//
// Design — tokio::sync::mpsc channel + background task:
// The consumer spawns a background tokio task that manages the connection
// (WebSocket or SSE) and sends messages into an mpsc channel. The public
// API exposes a `next()` method that receives from that channel, and a
// `Stream` implementation for use with stream combinators.
//
// Why mpsc channel instead of implementing Stream directly on the connection?
// Implementing Stream directly on a WebSocket would require the consumer
// struct to hold the connection and be !Send in some configurations.
// The channel approach decouples the connection lifecycle from the consumer
// handle — the caller holds a lightweight struct with a receiver, and the
// background task owns the connection. This is the idiomatic tokio pattern
// for wrapping async I/O as a stream.
//
// Reconnect:
// The background task reconnects automatically on connection failure.
// Each reconnect attempt waits reconnect_delay before retrying.
// If max_reconnect_attempts is exceeded, the channel is closed with an
// error sentinel and next() returns the error.
//
// At-least-once delivery (WebSocket + group):
// Call consumer.ack(offset) after processing a message. The broker only
// advances the group's committed offset on explicit ack. On reconnect,
// the broker redelivers from the last committed offset.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use url::Url;

use crate::{
    error::{Error, Result},
    models::{AckFrame, Message},
};

// Internal channel messages, either a real Message or a terminal error.
enum ChannelItem {
    Message(Message),
    Error(Error),
}

/// Options for creating a consumer.
#[derive(Debug, Clone)]
pub struct ConsumerOptions {
    /// Consumer group ID. Empty = broadcast (receive all messages).
    pub group: String,

    /// Starting offset.
    /// -1 = tail (new messages only, default).
    ///  0 = full history replay.
    ///  N = resume from offset N.
    pub from_offset: i64,

    /// Streaming transport.
    pub protocol: Protocol,

    /// Time to wait between reconnect attempts.
    pub reconnect_delay: Duration,

    /// Max reconnect attempts. 0 = unlimited.
    pub max_reconnect_attempts: usize,

    /// Internal channel buffer size.
    pub buffer_size: usize,
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        Self {
            group: String::new(),
            from_offset: -1,
            protocol: Protocol::WebSocket,
            reconnect_delay: Duration::from_secs(2),
            max_reconnect_attempts: 0,
            buffer_size: 64,
        }
    }
}

/// Streaming transport protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    WebSocket,
    Sse,
}

/// Async consumer for a streamq topic.
///
/// Obtain via [`crate::StreamqClient::subscribe`].
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
///     println!("offset {}: {:?}", msg.offset, msg.payload);
///     consumer.ack(msg.offset);
/// }
/// # Ok(())
/// # }
/// ```
pub struct Consumer {
    rx: mpsc::Receiver<ChannelItem>,
    ack_tx: mpsc::Sender<i64>,
}

impl Consumer {
    /// Create and start a consumer. Called internally by StreamqClient.
    pub(crate) fn new(base_url: String, topic: String, opts: ConsumerOptions) -> Self {
        let (tx, rx) = mpsc::channel::<ChannelItem>(opts.buffer_size);
        let (ack_tx, ack_rx) = mpsc::channel::<i64>(opts.buffer_size);

        // Spawn the background task that manages the connection.
        // The task owns tx and ack_rx; the caller owns rx and ack_tx.
        tokio::spawn(run_consumer(base_url, topic, opts, tx, ack_rx));

        Consumer { rx, ack_tx }
    }

    /// Receive the next message from the topic.
    ///
    /// Returns `Ok(Some(msg))` for each message.
    /// Returns `Ok(None)` when the consumer is closed cleanly.
    /// Returns `Err(e)` if the connection fails permanently.
    pub async fn next(&mut self) -> Result<Option<Message>> {
        match self.rx.recv().await {
            Some(ChannelItem::Message(msg)) => Ok(Some(msg)),
            Some(ChannelItem::Error(e)) => Err(e),
            None => Ok(None), // channel closed — clean shutdown
        }
    }

    /// Acknowledge a message offset.
    ///
    /// Sends `{"ack": offset}` to the broker. Only meaningful for
    /// WebSocket consumers in a consumer group — provides at-least-once
    /// delivery guarantees.
    ///
    /// No-op for SSE consumers or ungrouped consumers.
    /// Non-blocking — the ack is queued and sent asynchronously.
    pub fn ack(&self, offset: i64) {
        // try_send is non-blocking. If the channel is full or closed,
        // we drop the ack silently, the worst case is redelivery on reconnect.
        let _ = self.ack_tx.try_send(offset);
    }

    /// Close the consumer.
    ///
    /// Dropping the Consumer also closes it — this method exists for
    /// explicit control over the shutdown timing.
    pub fn close(self) {
        // Dropping self closes both rx and ack_tx, which signals the
        // background task to stop on its next iteration.
        drop(self);
    }
}

/// Main background task — manages reconnects and delegates to protocol handlers.
async fn run_consumer(
    base_url: String,
    topic: String,
    opts: ConsumerOptions,
    tx: mpsc::Sender<ChannelItem>,
    mut ack_rx: mpsc::Receiver<i64>,
) {
    let mut attempts: usize = 0;

    loop {
        let result = match opts.protocol {
            Protocol::WebSocket => connect_ws(&base_url, &topic, &opts, &tx, &mut ack_rx).await,
            Protocol::Sse => connect_sse(&base_url, &topic, &opts, &tx).await,
        };

        match result {
            Ok(()) => {
                // Clean exit, tx was dropped (consumer closed) or
                // broker closed the connection without error.
                return;
            }
            Err(e) => {
                attempts += 1;

                // Check if the consumer handle has been dropped.
                // If tx.send fails, the receiver is gone, stop.
                if tx.is_closed() {
                    return;
                }

                if opts.max_reconnect_attempts > 0 && attempts >= opts.max_reconnect_attempts {
                    let _ = tx
                        .send(ChannelItem::Error(Error::MaxReconnects {
                            attempts,
                            message: e.to_string(),
                        }))
                        .await;
                    return;
                }

                tokio::time::sleep(opts.reconnect_delay).await;
            }
        }
    }
}

async fn connect_ws(
    base_url: &str,
    topic: &str,
    opts: &ConsumerOptions,
    tx: &mpsc::Sender<ChannelItem>,
    ack_rx: &mut mpsc::Receiver<i64>,
) -> Result<()> {
    let url = build_url(base_url, topic, "ws", opts)?;

    let (ws_stream, _) = connect_async(url.as_str()).await?;
    let (mut write, mut read) = ws_stream.split();

    // Process incoming frames and outgoing acks concurrently.
    // We use tokio::select! to interleave reads from the broker with
    // acks from the caller, both happen on the same connection.
    loop {
        tokio::select! {
            // Incoming message from broker
            msg = read.next() => {
                match msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        match serde_json::from_str::<Message>(&text) {
                            Ok(msg) => {
                                if tx.send(ChannelItem::Message(msg)).await.is_err() {
                                    // Receiver dropped, consumer was closed.
                                    return Ok(());
                                }
                            }
                            Err(e) => {
                                // Malformed frame, log and continue.
                                eprintln!("streamq: malformed ws frame: {e}");
                            }
                        }
                    }
                    Some(Ok(WsMessage::Close(_))) | None => {
                        // Broker closed the connection cleanly.
                        return Ok(());
                    }
                    Some(Err(e)) => {
                        return Err(Error::WebSocket(e));
                    }
                    _ => {} // Ping/Pong/Binary, ignore
                }
            }

            // Outgoing ack from caller
            offset = ack_rx.recv() => {
                match offset {
                    Some(offset) if !opts.group.is_empty() => {
                        let frame = AckFrame { ack: offset };
                        let text = serde_json::to_string(&frame)?;
                        if let Err(e) = write.send(WsMessage::Text(text)).await {
                            return Err(Error::WebSocket(e));
                        }
                    }
                    None => {
                        // ack_tx dropped, consumer was closed.
                        return Ok(());
                    }
                    _ => {} // no group, ack is a no-op
                }
            }
        }
    }
}

async fn connect_sse(
    base_url: &str,
    topic: &str,
    opts: &ConsumerOptions,
    tx: &mpsc::Sender<ChannelItem>,
) -> Result<()> {
    let url = build_url(base_url, topic, "sse", opts)?;

    // Use reqwest to open a streaming GET request.
    // We don't use a full SSE client library to keep dependencies minimal
    // SSE is simple enough to parse manually (lines ending with \n\n).
    let client = reqwest::Client::new();
    let resp = client
        .get(url.as_str())
        .header("Accept", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(Error::Api {
            status: resp.status().as_u16(),
            message: format!("SSE connect failed: {}", resp.status()),
        });
    }

    // Stream the response body line by line.
    // SSE format:
    //   data: <json>\n
    //   \n               ← event boundary (blank line)
    let mut stream = resp.bytes_stream();
    let mut buffer = String::new();
    let mut data_line = String::new();

    use futures_util::StreamExt;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        // Process complete lines from the buffer.
        while let Some(pos) = buffer.find('\n') {
            let line = buffer[..pos].trim_end_matches('\r').to_string();
            buffer.drain(..=pos);

            if let Some(data) = line.strip_prefix("data: ") {
                data_line = data.to_string();
            } else if line.is_empty() && !data_line.is_empty() {
                // Blank line = end of event.
                match serde_json::from_str::<Message>(&data_line) {
                    Ok(msg) => {
                        if tx.send(ChannelItem::Message(msg)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        eprintln!("streamq: malformed sse event: {e}");
                    }
                }
                data_line.clear();
            }
        }
    }

    Ok(())
}

fn build_url(base_url: &str, topic: &str, protocol: &str, opts: &ConsumerOptions) -> Result<Url> {
    // Replace http(s) with ws(s) for WebSocket connections.
    let base = if protocol == "ws" {
        base_url
            .replace("https://", "wss://")
            .replace("http://", "ws://")
    } else {
        base_url.to_string()
    };

    let path = format!(
        "{}/topics/{}/subscribe/{}",
        base.trim_end_matches('/'),
        topic,
        protocol
    );
    let mut url = Url::parse(&path)?;

    if !opts.group.is_empty() {
        url.query_pairs_mut().append_pair("group", &opts.group);
    }
    if opts.from_offset >= 0 {
        url.query_pairs_mut()
            .append_pair("from_offset", &opts.from_offset.to_string());
    }

    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_opts() -> ConsumerOptions {
        ConsumerOptions::default()
    }

    #[test]
    fn build_url_ws_with_group_and_offset() {
        let opts = ConsumerOptions {
            group: "billing".to_string(),
            from_offset: 0,
            ..default_opts()
        };
        let url = build_url("http://localhost:8080", "payments", "ws", &opts).unwrap();
        assert!(url.as_str().starts_with("ws://"));
        assert!(url.as_str().contains("/topics/payments/subscribe/ws"));
        assert!(url.as_str().contains("group=billing"));
        assert!(url.as_str().contains("from_offset=0"));
    }

    #[test]
    fn build_url_sse_no_group_tail() {
        let opts = ConsumerOptions {
            from_offset: -1, // tail, should not appear
            ..default_opts()
        };
        let url = build_url("http://localhost:8080", "events", "sse", &opts).unwrap();
        assert!(url.as_str().starts_with("http://"));
        assert!(url.as_str().contains("/topics/events/subscribe/sse"));
        assert!(!url.as_str().contains("group"));
        assert!(!url.as_str().contains("from_offset"));
    }

    #[test]
    fn build_url_https_to_wss() {
        let url = build_url("https://broker.example.com", "t", "ws", &default_opts()).unwrap();
        assert!(url.as_str().starts_with("wss://"));
    }

    #[test]
    fn build_url_from_offset_zero_included() {
        let opts = ConsumerOptions {
            from_offset: 0,
            ..default_opts()
        };
        let url = build_url("http://localhost:8080", "t", "ws", &opts).unwrap();
        assert!(url.as_str().contains("from_offset=0"));
    }
}
