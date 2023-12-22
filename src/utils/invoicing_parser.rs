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

        // for line in reader.lines() {
        //     match line {
        //         Ok(byte_line) => match String::from_utf8(byte_line.into_bytes()) {
        //             Ok(line) => {
        //                 let line_without_0s = line.replace("\0", "");

        //                 if line_without_0s.contains("\t") {
        //                     let line_without_tabs = line_without_0s.replace("\t", ",");
        //                     lines.push_str(&format!("{}\n", line_without_tabs));
        //                 } else {
        //                     lines.push_str(&format!("{}\n", line_without_0s));
        //                 }
        //             }
        //             Err(err) => {
        //                 println!("🔥 Failed to convert line to utf8: {:?}", err);
        //             }
        //         },
        //         Err(err) => {
        //             println!("🔥 Failed to read line: {:?}", err);
        //         }
        //     }
        // }

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
