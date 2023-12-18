use std::env;

use dotenv::dotenv;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
}

impl Config {
    pub fn init() -> Config {
        let environment_result = dotenv();

        if environment_result.is_err() {
            println!("🔥 Failed to load .env file.");
            std::process::exit(1);
        }

        let database_url =
            env::var("DATABASE_URL").expect("Failed to find DATABASE_URL environment variable.");

        Config { database_url }
    }
}
