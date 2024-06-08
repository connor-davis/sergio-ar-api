use std::{
    fs::File,
    io::{Read, Write},
};

use anyhow::Error;

pub struct InvoicingParser {}

impl InvoicingParser {
    pub async fn parse_invoicing_file(file_path: &str) -> Result<String, Error> {
        // Open the csv file.
        tracing::info!("❕ Opening file {}", file_path);
        let mut file = File::open(file_path)?;
        let mut lines = String::new();

        tracing::info!("❕ Reading file to string.");
        file.read_to_string(&mut lines)?;
        file.flush()?;
        tracing::info!("❕ Done");

        tracing::info!("❕ Replacing tab stops.");
        let lines = lines.replace("\t", ",");

        tracing::info!("{:?}", lines);

        Ok(lines)
    }
}
