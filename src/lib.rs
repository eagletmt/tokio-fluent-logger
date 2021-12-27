#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Config {
    sub_second_precision: bool,
    max_retry: u32,
    write_timeout: Option<std::time::Duration>,
    request_ack: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sub_second_precision: false,
            max_retry: 13,
            write_timeout: None,
            request_ack: false,
        }
    }
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder {
            config: Config::default(),
        }
    }
}

pub struct ConfigBuilder {
    config: Config,
}

impl ConfigBuilder {
    pub fn sub_second_precision(mut self, value: bool) -> Self {
        self.config.sub_second_precision = value;
        self
    }

    pub fn write_timeout(mut self, value: std::time::Duration) -> Self {
        self.config.write_timeout = Some(value);
        self
    }

    pub fn request_ack(mut self, value: bool) -> Self {
        self.config.request_ack = value;
        self
    }

    pub fn build(self) -> Config {
        self.config
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialize(#[from] rmp_serde::encode::Error),
    #[error("failed to write after {0} attempts")]
    WriteMaxRetryExceeded(u32),
}

#[derive(Debug, serde::Deserialize)]
pub struct AckResponse {
    pub ack: String,
}

#[async_trait::async_trait]
pub trait TransportStream: Sized {
    type Stream;

    async fn connect(&self) -> std::io::Result<Self::Stream>;
    async fn write_buf<B>(&self, stream: &Self::Stream, buf: B) -> std::io::Result<()>
    where
        B: bytes::Buf + Send;
    async fn read_ack(&self, stream: &Self::Stream) -> std::io::Result<AckResponse>;
}

pub struct TcpTransport {
    host: String,
    port: u16,
}
impl TcpTransport {
    pub fn new<S>(host: S, port: u16) -> Self
    where
        S: Into<String>,
    {
        Self {
            host: host.into(),
            port,
        }
    }
}
#[async_trait::async_trait]
impl TransportStream for TcpTransport {
    type Stream = tokio::net::TcpStream;

    async fn connect(&self) -> std::io::Result<Self::Stream> {
        Ok(Self::Stream::connect((self.host.as_str(), self.port)).await?)
    }

    async fn write_buf<B>(&self, stream: &Self::Stream, mut buf: B) -> std::io::Result<()>
    where
        B: bytes::Buf + Send,
    {
        while buf.has_remaining() {
            stream.writable().await?;
            match stream.try_write(buf.chunk()) {
                Ok(n) => buf.advance(n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    async fn read_ack(&self, stream: &Self::Stream) -> std::io::Result<AckResponse> {
        let mut buf = bytes::BytesMut::new();

        loop {
            stream.readable().await?;
            match stream.try_read_buf(&mut buf) {
                Ok(_) => match rmp_serde::from_slice::<AckResponse>(&buf) {
                    Ok(ack) => return Ok(ack),
                    Err(e) => {
                        tracing::debug!(%e, ?buf, "rmp_serde failed, retrying");
                    }
                },
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(unix)]
pub struct UnixTransport {
    path: std::path::PathBuf,
}
#[cfg(unix)]
impl UnixTransport {
    pub fn new<P>(path: P) -> Self
    where
        P: Into<std::path::PathBuf>,
    {
        Self { path: path.into() }
    }
}
#[cfg(unix)]
#[async_trait::async_trait]
impl TransportStream for UnixTransport {
    type Stream = tokio::net::UnixStream;

    async fn connect(&self) -> std::io::Result<Self::Stream> {
        Ok(Self::Stream::connect(&self.path).await?)
    }

    async fn write_buf<B>(&self, stream: &Self::Stream, mut buf: B) -> std::io::Result<()>
    where
        B: bytes::Buf + Send,
    {
        while buf.has_remaining() {
            stream.writable().await?;
            match stream.try_write(buf.chunk()) {
                Ok(n) => buf.advance(n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    async fn read_ack(&self, stream: &Self::Stream) -> std::io::Result<AckResponse> {
        let mut buf = bytes::BytesMut::new();

        loop {
            stream.readable().await?;
            match stream.try_read_buf(&mut buf) {
                Ok(_) => match rmp_serde::from_slice::<AckResponse>(&buf) {
                    Ok(ack) => return Ok(ack),
                    Err(e) => {
                        tracing::debug!(%e, ?buf, "rmp_serde failed, retrying");
                    }
                },
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e),
            }
        }
    }
}

#[derive(Clone)]
struct SerializedMessage {
    message: bytes::Bytes,
    ack: Option<String>,
}

#[derive(Debug)]
pub struct Fluent<T>
where
    T: TransportStream,
{
    transport: T,
    stream: T::Stream,
    config: Config,
}

impl<T> Fluent<T>
where
    T: TransportStream,
{
    pub async fn new(transport: T, config: Config) -> std::io::Result<Self> {
        Ok(Self {
            stream: transport.connect().await?,
            transport,
            config,
        })
    }

    pub fn stream(&self) -> &T::Stream {
        &self.stream
    }

    pub async fn post<U>(&self, tag: &str, message: U) -> Result<(), Error>
    where
        U: serde::Serialize,
    {
        self.post_with_time(tag, chrono::Utc::now(), message).await
    }

    pub async fn post_with_time<U, Tz>(
        &self,
        tag: &str,
        time: chrono::DateTime<Tz>,
        message: U,
    ) -> Result<(), Error>
    where
        U: serde::Serialize,
        Tz: chrono::TimeZone,
    {
        let message = self.encode_data(tag, time, message)?;
        self.post_raw_data(message).await?;
        Ok(())
    }

    fn encode_data<U, Tz>(
        &self,
        tag: &str,
        time: chrono::DateTime<Tz>,
        record: U,
    ) -> Result<SerializedMessage, rmp_serde::encode::Error>
    where
        U: serde::Serialize,
        Tz: chrono::TimeZone,
    {
        use bytes::BufMut as _;
        use serde::Serialize as _;

        let mut writer = bytes::BytesMut::new().writer();
        let mut serializer = rmp_serde::Serializer::new(&mut writer).with_struct_map();

        let ack = if self.config.request_ack {
            let ack = base64::encode(uuid::Uuid::new_v4().as_bytes());
            let option = std::collections::HashMap::from([("chunk", ack.clone())]);
            if self.config.sub_second_precision {
                (tag, EventTime(time), record, option).serialize(&mut serializer)?;
            } else {
                (tag, time.timestamp(), record, option).serialize(&mut serializer)?;
            }
            Some(ack)
        } else {
            if self.config.sub_second_precision {
                (tag, EventTime(time), record).serialize(&mut serializer)?;
            } else {
                (tag, time.timestamp(), record).serialize(&mut serializer)?;
            }
            None
        };

        Ok(SerializedMessage {
            message: writer.into_inner().freeze(),
            ack,
        })
    }

    async fn post_raw_data(&self, message: SerializedMessage) -> Result<(), Error> {
        self.write_with_retry(message).await
    }

    async fn write_with_retry(&self, message: SerializedMessage) -> Result<(), Error> {
        for _ in 0..self.config.max_retry {
            if self.write(&message).await.is_ok() {
                return Ok(());
            }
        }
        Err(Error::WriteMaxRetryExceeded(self.config.max_retry))
    }

    async fn write(&self, message: &SerializedMessage) -> std::io::Result<()> {
        if let Some(d) = self.config.write_timeout {
            tokio::time::timeout(
                d,
                self.transport
                    .write_buf(&self.stream, message.message.clone()),
            )
            .await??;
        } else {
            self.transport
                .write_buf(&self.stream, message.message.clone())
                .await?;
        }

        if let Some(ref sent_ack) = message.ack {
            let received_ack = self.transport.read_ack(&self.stream).await?;
            if &received_ack.ack != sent_ack {
                tracing::error!(
                    "received ack '{}' doesn't match expected ack '{}'",
                    received_ack.ack,
                    sent_ack
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "fluentd ack failure",
                ));
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
struct EventTime<Tz>(chrono::DateTime<Tz>)
where
    Tz: chrono::TimeZone;

impl<Tz> serde::Serialize for EventTime<Tz>
where
    Tz: chrono::TimeZone,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use bytes::BufMut as _;

        // https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#eventtime-ext-format
        const EVENT_TIME_TYPE: i8 = 0x00;
        let mut buf = bytes::BytesMut::new();
        buf.put_u32(self.0.timestamp().try_into().expect("EventTime extension format can handle timestamp values only in unsigned 32-bit integer: https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#eventtime-ext-format"));
        // Coercing timestamp_subsec_nanos() to u32 should be safe because timestamp_subsec_nanos()
        // < 10^9
        buf.put_u32(self.0.timestamp_subsec_nanos() as u32);
        serializer
            .serialize_newtype_struct(rmp_serde::MSGPACK_EXT_STRUCT_NAME, &(EVENT_TIME_TYPE, buf))
    }
}

#[cfg(test)]
mod tests {
    struct TestTransport {}

    #[derive(Default)]
    struct TestStream {
        queue: std::sync::Mutex<std::cell::RefCell<Vec<bytes::Bytes>>>,
        last_ack: std::sync::Mutex<std::cell::RefCell<String>>,
        ack_counter: std::sync::Mutex<std::cell::RefCell<usize>>,
    }

    #[async_trait::async_trait]
    impl super::TransportStream for TestTransport {
        type Stream = TestStream;

        async fn connect(&self) -> std::io::Result<Self::Stream> {
            Ok(Self::Stream::default())
        }

        async fn write_buf<B>(&self, stream: &Self::Stream, mut buf: B) -> std::io::Result<()>
        where
            B: bytes::Buf + Send,
        {
            let b = buf.copy_to_bytes(buf.remaining());

            let r: Result<
                (
                    &str,
                    u64,
                    TestMessage,
                    std::collections::HashMap<&str, String>,
                ),
                _,
            > = rmp_serde::from_read_ref(&b);
            if let Ok((_, _, _, mut option)) = r {
                let last_ack = stream.last_ack.lock().unwrap();
                *last_ack.borrow_mut() = option.remove("chunk").unwrap();
            }

            let queue = stream.queue.lock().unwrap();
            queue.borrow_mut().push(b);
            Ok(())
        }

        async fn read_ack(&self, stream: &Self::Stream) -> std::io::Result<super::AckResponse> {
            let ack_counter = stream.ack_counter.lock().unwrap();
            *ack_counter.borrow_mut() += 1;
            let last_ack = stream.last_ack.lock().unwrap();
            let last_ack = last_ack.borrow();
            Ok(super::AckResponse {
                ack: last_ack.to_owned(),
            })
        }
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct TestMessage<'a> {
        message: &'a str,
    }

    use chrono::TimeZone as _;

    #[tokio::test]
    async fn it_sends_a_message() {
        let logger = super::Fluent::new(TestTransport {}, super::Config::default())
            .await
            .unwrap();
        let time = chrono::Utc.timestamp(1640612102, 750781000);
        logger
            .post_with_time("tag.name", time, TestMessage { message: "bar" })
            .await
            .unwrap();
        let queue = logger.stream().queue.lock().unwrap();
        let queue = queue.borrow();
        assert_eq!(queue.len(), 1);
        // 0x93 == 0x90 + 3: fixarray with N=3
        // 0xa8 == 0xa0 + 8: fixstr with N=8
        // 0xce: uint 32
        // 0x61c9c106 == 1640612102
        // 0x81 == 0x80 + 1: fixmap with N=1
        // 0xa7 == 0xa0 + 7: fixstr with N=7
        // 0xa3 == 0xa0 + 3: fixstr with N=3
        assert_eq!(
            queue[0].as_ref(),
            b"\x93\xa8tag.name\xce\x61\xc9\xc1\x06\x81\xa7message\xa3bar"
        );
        let ack_counter = logger.stream().ack_counter.lock().unwrap();
        assert_eq!(*ack_counter.borrow(), 0);
    }

    #[tokio::test]
    async fn it_sends_a_message_with_sub_second() {
        let logger = super::Fluent::new(
            TestTransport {},
            super::Config::builder().sub_second_precision(true).build(),
        )
        .await
        .unwrap();
        let time = chrono::Utc.timestamp(1640612102, 750781000);
        logger
            .post_with_time("tag.name", time, TestMessage { message: "bar" })
            .await
            .unwrap();
        let queue = logger.stream().queue.lock().unwrap();
        let queue = queue.borrow();
        assert_eq!(queue.len(), 1);
        // 0x93 == 0x90 + 3: fixarray with N=3
        // 0xa8 == 0xa0 + 8: fixstr with N=8
        // 0xd7: fixext 8
        // 0x00:   type == 0
        // 0x61c9c106 == 1640612102
        // 0x2cc00248 == 750781000
        // 0x81 == 0x80 + 1: fixmap with N=1
        // 0xa7 == 0xa0 + 7: fixstr with N=7
        // 0xa3 == 0xa0 + 3: fixstr with N=3
        assert_eq!(
            queue[0].as_ref(),
            b"\x93\xa8tag.name\xd7\x00\x61\xc9\xc1\x06\x2c\xc0\x02\x48\x81\xa7message\xa3bar"
        );
        let ack_counter = logger.stream().ack_counter.lock().unwrap();
        assert_eq!(*ack_counter.borrow(), 0);
    }

    #[tokio::test]
    async fn it_sends_a_message_requiring_ack() {
        let logger = super::Fluent::new(
            TestTransport {},
            super::Config::builder().request_ack(true).build(),
        )
        .await
        .unwrap();
        let time = chrono::Utc.timestamp(1640612102, 750781000);
        logger
            .post_with_time("tag.name", time, TestMessage { message: "bar" })
            .await
            .unwrap();
        let queue = logger.stream().queue.lock().unwrap();
        let queue = queue.borrow();
        assert_eq!(queue.len(), 1);
        // 0x94 == 0x90 + 4: fixarray with N=4
        // 0xa8 == 0xa0 + 8: fixstr with N=8
        // 0xce: uint 32
        // 0x61c9c106 == 1640612102
        // 0x81 == 0x80 + 1: fixmap with N=1
        // 0xa7 == 0xa0 + 7: fixstr with N=7
        // 0xa3 == 0xa0 + 3: fixstr with N=3
        // 0x81 == 0x80 + 1: fixmap with N=1
        // 0xa5 == 0xa0 + 5: fixstr with N=5
        // 0xb8 == 0xa0 + 24: fixstr with N=24
        let ack_counter = logger.stream().ack_counter.lock().unwrap();
        assert_eq!(*ack_counter.borrow(), 1);
        let last_ack = logger.stream().last_ack.lock().unwrap();
        let mut expected =
            b"\x94\xa8tag.name\xce\x61\xc9\xc1\x06\x81\xa7message\xa3bar\x81\xa5chunk\xb8".to_vec();
        expected.extend_from_slice(last_ack.borrow().as_bytes());
        assert_eq!(queue[0], expected);
    }
}
