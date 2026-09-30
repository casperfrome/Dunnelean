use clap::{Parser, Subcommand};
use dunnelean::{
    api,
    config::{RunSpec, ServerConfig},
    engine::{self, Engine},
    store::Store,
};
#[derive(Parser)]
#[command(
    name = "dunnelean",
    version,
    about = "Arrow-native MySQL ↔ Doris batch synchronization"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(long, default_value = "dunnelean.toml")]
        config: String,
    },
    Validate {
        #[arg(long)]
        job: String,
    },
    Schema,
    Openapi,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "dunnelean=info".into()),
        )
        .init();
    match Cli::parse().command {
        Command::Schema => println!("{}", serde_json::to_string_pretty(&api::connector_info())?),
        Command::Openapi => println!("{}", serde_json::to_string_pretty(&api::openapi())?),
        Command::Validate { job } => {
            let spec: RunSpec = serde_json::from_slice(&std::fs::read(job)?)?;
            println!(
                "{}",
                engine::validation_json(&engine::validate(&spec).await?)
            );
        }
        Command::Serve { config } => {
            let config: ServerConfig = toml::from_str(&std::fs::read_to_string(config)?)?;
            let store = Store::open(&config.state_path)?;
            let engine = Engine::new(store, &config)?;
            let listener = tokio::net::TcpListener::bind(&config.listen).await?;
            tracing::info!(address=%listener.local_addr()?,"Dunnelean is ready");
            let app = api::router(engine.clone());
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = tokio::signal::ctrl_c().await;
                    engine.shutdown(config.shutdown_timeout_ms).await;
                })
                .await?;
        }
    }
    Ok(())
}
