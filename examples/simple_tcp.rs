#[derive(Debug, serde::Serialize)]
struct Message {
    foo: &'static str,
    hoge: &'static str,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = tokio_fluent_logger::TcpTransport::new("localhost", 24225);
    let logger = tokio_fluent_logger::Fluent::new(
        transport,
        tokio_fluent_logger::Config::builder()
            .sub_second_precision(true)
            .build(),
    )
    .await?;
    for _ in 0..1000000 {
        logger
            .post(
                "test.rust",
                Message {
                    foo: "bar",
                    hoge: "fuga",
                },
            )
            .await?;
    }
    Ok(())
}
