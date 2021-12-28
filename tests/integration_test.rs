// Docker containers must be started during the integration tests
use chrono::TimeZone as _;

#[derive(serde::Serialize)]
struct Message {
    foo: &'static str,
}

#[tokio::test]
async fn it_works_with_fluentd() {
    let transport = tokio_fluent_logger::TcpTransport::new("localhost", 24224);
    let logger =
        tokio_fluent_logger::Fluent::new(transport, tokio_fluent_logger::Config::default())
            .await
            .unwrap();
    let t = chrono::Utc.timestamp(1640707646, 942201600);

    logger
        .post_with_time("test.default", t, Message { foo: "bar" })
        .await
        .unwrap();
}

#[tokio::test]
async fn it_works_with_fluentd_sub_second() {
    let transport = tokio_fluent_logger::TcpTransport::new("localhost", 24224);
    let logger = tokio_fluent_logger::Fluent::new(
        transport,
        tokio_fluent_logger::Config::builder()
            .sub_second_precision(true)
            .build(),
    )
    .await
    .unwrap();
    let t = chrono::Utc.timestamp(1640707646, 942201600);

    logger
        .post_with_time("test.sub_second", t, Message { foo: "bar" })
        .await
        .unwrap();
}

#[tokio::test]
async fn it_works_with_fluentd_ack() {
    let transport = tokio_fluent_logger::TcpTransport::new("localhost", 24224);
    let logger = tokio_fluent_logger::Fluent::new(
        transport,
        tokio_fluent_logger::Config::builder()
            .request_ack(true)
            .build(),
    )
    .await
    .unwrap();
    let t = chrono::Utc.timestamp(1640707646, 942201600);

    logger
        .post_with_time("test.ack", t, Message { foo: "bar" })
        .await
        .unwrap();
}

#[tokio::test]
async fn it_works_with_fluent_bit() {
    let transport = tokio_fluent_logger::TcpTransport::new("localhost", 24225);
    let logger =
        tokio_fluent_logger::Fluent::new(transport, tokio_fluent_logger::Config::default())
            .await
            .unwrap();
    let t = chrono::Utc.timestamp(1640707646, 942201600);

    logger
        .post_with_time("test.default", t, Message { foo: "bar" })
        .await
        .unwrap();
}

#[tokio::test]
async fn it_works_with_fluent_bit_sub_second() {
    let transport = tokio_fluent_logger::TcpTransport::new("localhost", 24225);
    let logger = tokio_fluent_logger::Fluent::new(
        transport,
        tokio_fluent_logger::Config::builder()
            .sub_second_precision(true)
            .build(),
    )
    .await
    .unwrap();
    let t = chrono::Utc.timestamp(1640707646, 942201600);

    logger
        .post_with_time("test.sub_second", t, Message { foo: "bar" })
        .await
        .unwrap();
}

#[tokio::test]
async fn it_works_with_fluent_bit_ack() {
    let transport = tokio_fluent_logger::TcpTransport::new("localhost", 24225);
    let logger = tokio_fluent_logger::Fluent::new(
        transport,
        tokio_fluent_logger::Config::builder()
            .request_ack(true)
            .build(),
    )
    .await
    .unwrap();
    let t = chrono::Utc.timestamp(1640707646, 942201600);

    logger
        .post_with_time("test.ack", t, Message { foo: "bar" })
        .await
        .unwrap();
}
