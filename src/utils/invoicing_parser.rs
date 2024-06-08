use std::{
    fs::File,
    io::{BufReader, Read, Write},
};

use anyhow::Error;

pub struct InvoicingParser {}

impl InvoicingParser {
    pub async fn parse_invoicing_file(file_path: &str) -> Result<String, Error> {
        // Open the csv file.
        let mut file = File::open(file_path)?;
        let mut lines = String::new();

        file.read_to_string(&mut lines)?;
        file.flush()?;

        let lines = lines.replace("\t", ",");

        println!("{:?}", lines);

        Ok(lines)
    }
}
