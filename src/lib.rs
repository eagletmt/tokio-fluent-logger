#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Config {
    fluent_host: String,
    fluent_port: u16,
    sub_second_precision: bool,
    max_retry: u32,
    write_timeout: Option<std::time::Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            fluent_host: "127.0.0.1".to_owned(),
            fluent_port: 24224,
            sub_second_precision: false,
            max_retry: 13,
            write_timeout: None,
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

#[derive(Debug)]
pub struct Fluent {
    // TODO: Support UDP and UNIX domain socket
    stream: tokio::net::TcpStream,
    config: Config,
}

impl Fluent {
    pub async fn new(config: Config) -> std::io::Result<Self> {
        Ok(Self {
            stream: connect(&config).await?,
            config,
        })
    }

    pub async fn post<T>(&self, tag: &str, message: T) -> Result<(), Error>
    where
        T: serde::Serialize,
    {
        self.post_with_time(tag, chrono::Utc::now(), message).await
    }

    pub async fn post_with_time<T, Tz>(
        &self,
        tag: &str,
        time: chrono::DateTime<Tz>,
        message: T,
    ) -> Result<(), Error>
    where
        T: serde::Serialize,
        Tz: chrono::TimeZone,
    {
        let message = self.encode_data(tag, time, message)?;
        self.post_raw_data(message).await?;
        Ok(())
    }

    fn encode_data<T, Tz>(
        &self,
        tag: &str,
        time: chrono::DateTime<Tz>,
        record: T,
    ) -> Result<bytes::Bytes, rmp_serde::encode::Error>
    where
        T: serde::Serialize,
        Tz: chrono::TimeZone,
    {
        use bytes::BufMut as _;
        use serde::Serialize as _;

        // TODO: Support request_ack

        let mut writer = bytes::BytesMut::new().writer();
        let mut serializer = rmp_serde::Serializer::new(&mut writer).with_struct_map();
        if self.config.sub_second_precision {
            (tag, EventTime(time), record).serialize(&mut serializer)?;
        } else {
            (tag, time.timestamp(), record).serialize(&mut serializer)?;
        }
        Ok(writer.into_inner().freeze())
    }

    async fn post_raw_data(&self, message: bytes::Bytes) -> Result<(), Error> {
        self.write_with_retry(message).await
    }

    async fn write_with_retry(&self, message: bytes::Bytes) -> Result<(), Error> {
        for _ in 0..self.config.max_retry {
            if self.write(message.clone()).await? {
                return Ok(());
            }
        }
        Err(Error::WriteMaxRetryExceeded(self.config.max_retry))
    }

    async fn write(&self, message: bytes::Bytes) -> std::io::Result<bool> {
        if let Some(d) = self.config.write_timeout {
            tokio::time::timeout(d, self.write_all(message)).await?
        } else {
            self.write_all(message).await
        }
    }

    async fn write_all(&self, mut message: bytes::Bytes) -> std::io::Result<bool> {
        use bytes::Buf as _;

        while message.has_remaining() {
            self.stream.writable().await?;
            match self.stream.try_write(&message) {
                Ok(n) => message.advance(n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(true)
    }
}

async fn connect(config: &Config) -> std::io::Result<tokio::net::TcpStream> {
    tokio::net::TcpStream::connect((config.fluent_host.as_str(), config.fluent_port)).await
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
