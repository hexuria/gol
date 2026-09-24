use protocol::RunId;
use redis::Commands;

pub struct RedisRunQueue {
    url: String,
    key: String,
}

impl RedisRunQueue {
    pub fn open(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            key: "gol:runs".to_string(),
        }
    }

    pub fn with_key(url: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            key: key.into(),
        }
    }

    pub fn push(&self, id: RunId) -> Result<(), String> {
        let mut connection = self.connection()?;
        connection
            .lpush::<_, _, ()>(&self.key, id.to_string())
            .map_err(|error| error.to_string())
    }

    pub fn pop(&self) -> Result<Option<RunId>, String> {
        let mut connection = self.connection()?;
        let value: Option<String> = connection
            .rpop(&self.key, None::<std::num::NonZeroUsize>)
            .map_err(|error| error.to_string())?;
        value
            .map(|text| text.parse().map_err(|error: uuid::Error| error.to_string()))
            .transpose()
    }

    fn connection(&self) -> Result<redis::Connection, String> {
        redis::Client::open(self.url.as_str())
            .map_err(|error| error.to_string())?
            .get_connection()
            .map_err(|error| error.to_string())
    }
}
