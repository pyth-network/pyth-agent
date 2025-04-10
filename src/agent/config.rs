use {
    super::{
        metrics,
        pyth,
        services,
        solana::network,
        state,
    },
    anyhow::Result,
    config as config_rs,
    config_rs::{
        Environment,
        File,
    },
    serde::Deserialize,
    std::{
        path::Path,
        time::Duration,
    },
};

/// Configuration for all components of the Agent
#[derive(Deserialize, Debug)]
pub struct Config {
    #[serde(default)]
    pub channel_capacities:    ChannelCapacities,
    pub primary_network:       network::Config,
    pub secondary_network:     Option<network::Config>,
    #[serde(default)]
    #[serde(rename = "pythd_adapter")]
    pub state:                 state::Config,
    #[serde(default)]
    pub pythd_api_server:      pyth::rpc::Config,
    #[serde(default)]
    pub metrics_server:        metrics::Config,
    #[serde(default)]
    pub remote_keypair_loader: services::keypairs::Config,
    pub opentelemetry:         Option<OpenTelemetryConfig>,
    pub pyth_lazer:            Option<services::lazer_exporter::Config>,
}

impl Config {
    pub fn new(config_file: impl AsRef<Path>) -> Result<Self> {
        // Build a new configuration object, allowing the default values to be
        // overridden by those in the config_file or "AGENT_"-prefixed environment
        // variables.
        config_rs::Config::builder()
            .add_source(File::from(config_file.as_ref()))
            .add_source(Environment::with_prefix("agent"))
            .build()?
            .try_deserialize()
            .map_err(|e| e.into())
    }
}

/// Capacities of the channels top-level components use to communicate
#[derive(Deserialize, Debug)]
pub struct ChannelCapacities {
    /// Capacity of the channel used to broadcast shutdown events to all components
    pub shutdown:                 usize,
    /// Capacity of the channel used to send updates from the primary Oracle to the Global Store
    pub primary_oracle_updates:   usize,
    /// Capacity of the channel used to send updates from the secondary Oracle to the Global Store
    pub secondary_oracle_updates: usize,
    /// Capacity of the channel the Pythd API Adapter uses to send lookup requests to the Global Store
    pub global_store_lookup:      usize,
    /// Capacity of the channel the Pythd API Adapter uses to communicate with the Local Store
    pub local_store_lookup:       usize,
    /// Capacity of the channel on which the Local Store receives messages
    pub local_store:              usize,
    /// Capacity of the channel on which the Pythd API Adapter receives messages
    pub pythd_adapter:            usize,
    /// Capacity of the slog logging channel. Adjust this value if you see complaints about channel capacity from slog
    pub logger_buffer:            usize,
}

impl Default for ChannelCapacities {
    fn default() -> Self {
        Self {
            shutdown:                 10000,
            primary_oracle_updates:   10000,
            secondary_oracle_updates: 10000,
            global_store_lookup:      10000,
            local_store_lookup:       10000,
            local_store:              10000,
            pythd_adapter:            10000,
            logger_buffer:            10000,
        }
    }
}


#[derive(Deserialize, Debug)]
pub struct OpenTelemetryConfig {
    #[serde(with = "humantime_serde")]
    pub exporter_timeout_duration: Duration,
    pub exporter_endpoint:         String,
}
