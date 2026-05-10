// Integration tests for StreamqClient using wiremock.
// wiremock spins up a real HTTP server in-process, no real broker needed.
// Tests verify the full request/response cycle including JSON parsing,
// status code mapping, and error type dispatch.

use wiremock::{
    matchers::{body_json_string, method, path},
    Mock, MockServer, ResponseTemplate,
};

use streamq::{Error, StreamqClient};

fn success(data: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(serde_json::json!({
        "status": "success",
        "data": data
    }))
}

fn success_status(status: u16, data: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(status).set_body_json(serde_json::json!({
        "status": "success",
        "data": data
    }))
}

fn error_resp(status: u16, message: &str) -> ResponseTemplate {
    ResponseTemplate::new(status).set_body_json(serde_json::json!({
        "status": "error",
        "message": message
    }))
}

#[tokio::test]
async fn create_topic_success() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/topics"))
        .respond_with(success_status(
            201,
            serde_json::json!({ "name": "payments" }),
        ))
        .mount(&server)
        .await;

    let client = StreamqClient::new(server.uri()).unwrap();
    assert!(client.create_topic("payments").await.is_ok());
}

#[tokio::test]
async fn create_topic_conflict() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/topics"))
        .respond_with(error_resp(409, "topic already exists"))
        .mount(&server)
        .await;

    let client = StreamqClient::new(server.uri()).unwrap();
    let err = client.create_topic("payments").await.unwrap_err();

    assert!(matches!(err, Error::Conflict(_)));
    if let Error::Conflict(msg) = err {
        assert!(msg.contains("already exists"));
    }
}

#[tokio::test]
async fn delete_topic_success() {
    let server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path("/topics/payments"))
        .respond_with(success(serde_json::Value::Null))
        .mount(&server)
        .await;

    let client = StreamqClient::new(server.uri()).unwrap();
    assert!(client.delete_topic("payments").await.is_ok());
}

#[tokio::test]
async fn delete_topic_not_found() {
    let server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path("/topics/missing"))
        .respond_with(error_resp(404, "topic not found"))
        .mount(&server)
        .await;

    let client = StreamqClient::new(server.uri()).unwrap();
    let err = client.delete_topic("missing").await.unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
}

#[tokio::test]
async fn list_topics_returns_parsed_vec() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/topics"))
        .respond_with(success(serde_json::json!([
            { "name": "payments", "message_count": 10, "subscriber_count": 2, "latest_offset": 9 },
            { "name": "events",   "message_count":  5, "subscriber_count": 0, "latest_offset": 4 },
        ])))
        .mount(&server)
        .await;

    let client = StreamqClient::new(server.uri()).unwrap();
    let topics = client.list_topics().await.unwrap();

    assert_eq!(topics.len(), 2);
    assert_eq!(topics[0].name, "payments");
    assert_eq!(topics[0].message_count, 10);
    assert_eq!(topics[1].name, "events");
}

#[tokio::test]
async fn list_topics_empty_returns_empty_vec() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/topics"))
        .respond_with(success(serde_json::json!([])))
        .mount(&server)
        .await;

    let client = StreamqClient::new(server.uri()).unwrap();
    let topics = client.list_topics().await.unwrap();
    assert!(topics.is_empty());
}

#[tokio::test]
async fn get_topic_returns_info() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/topics/payments"))
        .respond_with(success(serde_json::json!({
            "name": "payments",
            "message_count": 42,
            "subscriber_count": 1,
            "latest_offset": 41
        })))
        .mount(&server)
        .await;

    let client = StreamqClient::new(server.uri()).unwrap();
    let info = client.get_topic("payments").await.unwrap();

    assert_eq!(info.name, "payments");
    assert_eq!(info.latest_offset, 41);
}

#[tokio::test]
async fn get_topic_not_found() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/topics/missing"))
        .respond_with(error_resp(404, "topic not found"))
        .mount(&server)
        .await;

    let client = StreamqClient::new(server.uri()).unwrap();
    assert!(matches!(
        client.get_topic("missing").await.unwrap_err(),
        Error::NotFound(_)
    ));
}

#[tokio::test]
async fn publish_returns_offset() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/topics/payments/publish"))
        .respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({
            "status": "success",
            "data": {
                "offset": 7,
                "topic": "payments",
                "timestamp": "2025-01-01T00:00:00Z"
            }
        })))
        .mount(&server)
        .await;

    let client = StreamqClient::new(server.uri()).unwrap();
    let result = client
        .publish("payments", b"{\"amount\":100}")
        .await
        .unwrap();

    assert_eq!(result.offset, 7);
    assert_eq!(result.topic, "payments");
}

#[tokio::test]
async fn publish_base64_encodes_payload() {
    let server = MockServer::start().await;

    // Verify the request body contains a base64-encoded payload.
    // "hello" base64-encodes to "aGVsbG8="
    Mock::given(method("POST"))
        .and(path("/topics/payments/publish"))
        .and(body_json_string(r#"{"payload":"aGVsbG8="}"#))
        .respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({
            "status": "success",
            "data": { "offset": 0, "topic": "payments", "timestamp": "2025-01-01T00:00:00Z" }
        })))
        .mount(&server)
        .await;

    let client = StreamqClient::new(server.uri()).unwrap();
    client.publish("payments", b"hello").await.unwrap();
}

#[tokio::test]
async fn publish_not_found() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/topics/missing/publish"))
        .respond_with(error_resp(404, "topic not found"))
        .mount(&server)
        .await;

    let client = StreamqClient::new(server.uri()).unwrap();
    assert!(matches!(
        client.publish("missing", b"data").await.unwrap_err(),
        Error::NotFound(_)
    ));
}

#[test]
fn error_display_not_found() {
    let e = Error::NotFound("topic not found".to_string());
    assert!(e.to_string().contains("not found"));
}

#[test]
fn error_display_conflict() {
    let e = Error::Conflict("already exists".to_string());
    assert!(e.to_string().contains("conflict"));
}

#[test]
fn error_display_api() {
    let e = Error::Api {
        status: 500,
        message: "internal error".to_string(),
    };
    assert!(e.to_string().contains("500"));
}

#[test]
fn error_display_max_reconnects() {
    let e = Error::MaxReconnects {
        attempts: 5,
        message: "refused".to_string(),
    };
    assert!(e.to_string().contains("5"));
}
