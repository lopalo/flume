use async_std::net::{IpAddr, Ipv4Addr, SocketAddr};
use dotenv;
#[cfg(feature = "sqlite")]
use sqlx::sqlite::SqliteConnectOptions;
use std::env::{self, VarError};
use std::process;
use std::str::FromStr;
use std::time::Duration;
use log::LevelFilter;

pub enum QueueHubType {
    InMemory,
    #[cfg(feature = "sqlite")]
    Sqlite,
}

impl FromStr for QueueHubType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s.to_lowercase()[..] {
            "in-memory" => Ok(Self::InMemory),
            #[cfg(feature = "sqlite")]
            "sqlite" => Ok(Self::Sqlite),
            _ => Err(()),
        }
    }
}

pub struct Config {
    pub log_level: LevelFilter,
    pub max_queue_size: usize,
    pub garbage_collection_period: Duration,
    pub queue_hub_type: QueueHubType,
    pub http_sock_address: SocketAddr,
    #[cfg(feature = "sqlite")]
    pub database_url: SqliteConnectOptions,
}

pub fn read_config() -> Config {
    dotenv::from_filename(".env").ok();

    let log_level = parse_env_var(
        "LOG_LEVEL",
        "one of: off, error, warn, info, debug, trace",
        LevelFilter::Info,
    );

    let max_queue_size =
        parse_env_var("MAX_QUEUE_SIZE", "a positive number", 10_000);

    let garbage_collection_period = Duration::from_millis(parse_env_var(
        "GARBAGE_COLLECTION_PERIOD_MS",
        "a positive number in milliseconds",
        30_000,
    ));

    let mut hub_type_options = vec!["in-memory"];
    if cfg!(feature = "sqlite") {
        hub_type_options.push("sqlite")
    }
    let queue_hub_type = parse_env_var(
        "QUEUE_HUB_TYPE",
        &format!("one of: {}", hub_type_options.join(", ")),
        QueueHubType::InMemory,
    );

    let http_sock_address = parse_env_var(
        "HTTP_SOCKET_ADDRESS",
        "a valid socket address: <ip address>:<port>",
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8822),
    );

    #[cfg(feature = "sqlite")]
    let database_url = parse_env_var(
        "DATABASE_URL",
        "a valid path to an SQLite database: sqlite://<path>",
        SqliteConnectOptions::new().filename("sqlite://db/queue_hub.db"),
    );

    Config {
        log_level,
        max_queue_size,
        garbage_collection_period,
        queue_hub_type,
        http_sock_address,
        #[cfg(feature = "sqlite")]
        database_url,
    }
}

fn parse_env_var<T>(var_name: &str, val_description: &str, default_val: T) -> T
where
    T: FromStr,
    T::Err: std::fmt::Debug,
{
    let exit = || {
        eprintln!("\"{}\" must be {}", var_name, val_description);
        process::exit(1)
    };
    match env::var(var_name) {
        Ok(val_str) => val_str.parse().unwrap_or_else(|_| exit()),
        Err(VarError::NotUnicode(_)) => exit(),
        Err(VarError::NotPresent) => default_val,
    }
}
