// streamq-rs — Official Rust SDK for the streamq message broker.
//
// # Quick start
//
// ```no_run
// use streamq::{StreamqClient, ConsumerOptions};
//
// #[tokio::main]
// async fn main() -> streamq::Result<()> {
//     let client = StreamqClient::new("http://localhost:8080")?;
//
//     client.create_topic("payments").await?;
//
//     let result = client.publish("payments", b"{\"amount\": 100}").await?;
//     println!("published at offset {}", result.offset);
//
//     let mut consumer = client.subscribe("payments", ConsumerOptions {
//         group: "billing".to_string(),
//         from_offset: 0,
//         ..Default::default()
//     });
//
//     while let Some(msg) = consumer.next().await? {
//         let text = String::from_utf8_lossy(&msg.payload);
//         println!("offset {}: {text}", msg.offset);
//         consumer.ack(msg.offset);
//     }
//
//     Ok(())
// }
// ```

mod client;
mod consumer;
mod error;
mod models;

pub use client::StreamqClient;
pub use consumer::{Consumer, ConsumerOptions, Protocol};
pub use error::{Error, Result};
pub use models::{Message, PublishResult, TopicInfo};
