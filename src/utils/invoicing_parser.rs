use std::{
    fs::File,
    io::{BufReader, Read},
};

use anyhow::Error;

pub struct InvoicingParser {}

impl InvoicingParser {
    pub async fn parse_invoicing_file(file_path: &str) -> Result<String, Error> {
        // Open the csv file.
        let file = File::open(file_path)?;

        let reader = BufReader::new(file);

        let mut lines: String = String::new();

        for byte in reader.bytes() {
            match byte {
                Ok(byte) => {
                    if byte == 0 {
                        continue;
                    }

                    let byte = byte as char;

                    if byte == '\t' {
                        lines.push(',');
                    } else {
                        lines.push(byte);
                    }
                }
                Err(err) => {
                    println!("🔥 Failed to read byte: {:?}", err);
                }
            }
        }

        Ok(lines)
    }
}
