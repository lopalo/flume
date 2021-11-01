use async_std::net::{IpAddr, Ipv4Addr, SocketAddr};
use dotenv;
use std::env::{self, VarError};
use std::process;
use std::str::FromStr;
use tide::log::LevelFilter;

pub enum QueueHubType {
    InMemory,
}

impl FromStr for QueueHubType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s.to_lowercase()[..] {
            "in-memory" => Ok(Self::InMemory),
            _ => Err(()),
        }
    }
}

pub struct Config {
    pub log_level: LevelFilter,
    pub max_queue_size: usize,
    pub queue_hub_type: QueueHubType,
    pub http_sock_address: SocketAddr,
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

    let queue_hub_type = parse_env_var(
        "QUEUE_HUB_TYPE",
        "one of: in-memory",
        QueueHubType::InMemory,
    );

    let http_sock_address = parse_env_var(
        "HTTP_SOCKET_ADDRESS",
        "a valid socket address: <ip address>:<port>",
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8822),
    );

    Config {
        log_level,
        max_queue_size,
        queue_hub_type,
        http_sock_address,
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
