#![allow(unused)]

use std::fs::read_to_string;

use anyhow::Error;

pub struct InvoicingParser {}

impl InvoicingParser {
    pub async fn parse_invoicing_file(file_path: &str) -> Result<String, Error> {
        // Open the csv file.
        tracing::info!("❕ Opening file {}", file_path);
        tracing::info!("❕ Reading file to string.");
        let lines = read_to_string(file_path)?;

        println!("{}", lines);

        tracing::info!("❕ Done");

        tracing::info!("❕ Replacing tab stops.");
        let lines = lines.replace("\t", ",");

        tracing::info!("{:?}", lines);

        Ok(lines)
    }
}
