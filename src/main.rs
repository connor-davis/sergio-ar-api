use std::net::SocketAddr;

use anyhow::Error;
use axum::{
    extract::DefaultBodyLimit,
    http::{
        header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE},
        HeaderValue, Method,
    },
};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::{config::Config, router::create_router};

mod config;
mod router;
mod routes;

#[derive(Clone)]
pub struct AppState {
    pub db: Pool<Postgres>,
    pub env: Config,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!(
        r#"
 _                            _  __   ___       __ _                       
| |   ___ _ _  _____ __ _____| |/ _| / __| ___ / _| |___ __ ____ _ _ _ ___ 
| |__/ _ \ ' \/ -_) V  V / _ \ |  _| \__ \/ _ \  _|  _\ V  V / _` | '_/ -_)
|____\___/_||_\___|\_/\_/\___/_|_|   |___/\___/_|  \__|\_/\_/\__,_|_| \___|
                                                                                   

Automatic Reports Consolidation API © 2023
        "#
    );

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "api=debug,tower_http=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Config::init();

    let pool = match PgPoolOptions::new()
        .max_connections(32)
        .connect(&config.database_url)
        .await
    {
        Ok(pool) => {
            println!("✅ Connection to the database is successful!");
            pool
        }
        Err(err) => {
            println!("🔥 Failed to connect to the database: {:?}", err);
            std::process::exit(1);
        }
    };

    let migration_result = sqlx::migrate!().run(&pool).await;

    match migration_result {
        Ok(_) => {
            println!("✅ Database migration successful!");

            let app_state = AppState {
                db: pool.clone(),
                env: config.clone(),
            };

            let app = create_router(app_state.clone()).await;

            let cors = CorsLayer::new()
                .allow_origin("http://localhost:3001".parse::<HeaderValue>()?)
                .allow_origin("http://localhost:5173".parse::<HeaderValue>()?)
                .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
                .allow_credentials(true)
                .allow_headers([AUTHORIZATION, ACCEPT, CONTENT_TYPE]);

            let app = app
                .layer(DefaultBodyLimit::max(100_000_000))
                .layer(cors)
                .layer(ServiceBuilder::new().layer(TraceLayer::new_for_http()));

            let address = SocketAddr::from(([0, 0, 0, 0], 3000));

            let listener = TcpListener::bind(&address).await?;

            println!("🚀 Listening on http://{}", address);

            axum::serve(listener, app.into_make_service()).await?;

            Ok(())
        }
        Err(err) => {
            println!("🔥 Database migration failed: {:?}", err);

            Ok(())
        }
    }
}
