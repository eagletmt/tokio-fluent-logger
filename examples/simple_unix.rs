#[derive(Debug, serde::Serialize)]
struct Message {
    foo: &'static str,
    hoge: &'static str,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let logger = tokio_fluent_logger::Fluent::<tokio_fluent_logger::UnixTransport>::new(
        tokio_fluent_logger::Config::builder()
            .fluent_socket_path("./fluentd.sock")
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
