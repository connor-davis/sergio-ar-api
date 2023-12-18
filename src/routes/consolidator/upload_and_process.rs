use anyhow::Error;
use axum::{
    extract::{Multipart, Query},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use calamine::{open_workbook, Reader, Xlsx};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{
    fs::{create_dir, try_exists, File},
    io::AsyncWriteExt,
};

#[derive(Deserialize)]
pub struct UploadAndProcessQuery {
    pub date: String,
}

pub async fn upload_and_process(
    Query(query): Query<UploadAndProcessQuery>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    let upload_result = store_files(&mut multipart, &query.date).await;

    match upload_result {
        Ok(_) => {
            println!("✅ Upload successful!");

            let process_result = consolidate_files(&query.date).await;

            match process_result {
                Ok(_) => {
                    println!("✅ Consolidation successful!");

                    Ok(Json(json!({
                        "status": StatusCode::OK.as_u16(),
                        "message": "Upload and process successful!",
                    })))
                }
                Err(_) => {
                    println!("🔥 Consolidation failed!");

                    return Ok(Json(json!({
                        "status": StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                        "message": "Consolidation failed!",
                    })));
                }
            }
        }
        Err(_) => {
            println!("🔥 Upload failed!");

            Ok(Json(json!({
                "status": StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                "message": "Upload and process failed!",
            })))
        }
    }
}

async fn store_files(multipart: &mut Multipart, date: &str) -> Result<(), Error> {
    let temp_directory_exists = try_exists("temp").await;

    match temp_directory_exists {
        Ok(directory) => {
            if !directory {
                println!("❕ Temp directory not found. Creating temp directory.");

                let create_dir_result = create_dir("temp").await;

                match create_dir_result {
                    Ok(_) => {
                        println!("✅ Temp directory created.")
                    }
                    Err(_) => {
                        println!("🔥 Failed to create the temp directory.")
                    }
                }
            }
        }
        Err(_) => {
            println!("🔥 Unknown error when checking if the temp directory exists.")
        }
    }

    let directory_path = format!("temp/{}", date);
    let directory_exists = try_exists(&directory_path).await;

    match directory_exists {
        Ok(directory) => {
            if !directory {
                println!("directory not found. creating");

                let create_dir_result = create_dir(&directory_path).await;

                match create_dir_result {
                    Ok(_) => {
                        println!("directory created");
                    }
                    Err(_) => {
                        println!("failed to create directory");
                    }
                }
            }
        }
        Err(_) => {
            println!("unknown error when checking directory exists")
        }
    }

    while let Some(field) = multipart.next_field().await.unwrap() {
        let name = field.name().unwrap().to_string();
        let data = field.bytes().await.unwrap();

        let file_result = File::create(format!("{}/{}", &directory_path, &name)).await;

        match file_result {
            Ok(mut file) => {
                let write_result = file.write_all(&data).await;

                match write_result {
                    Ok(_) => {
                        println!("✅ File data written to temporary file: {}", &name);
                    }
                    Err(_) => {
                        println!("🔥 Failed to write file data to temporary file.");
                    }
                }
            }
            Err(_) => {
                println!("🔥 Failed to upload and temporarily store file.");
            }
        }
    }

    Ok(())
}

async fn consolidate_files(date: &str) -> Result<(), Error> {
    let first_dialogue_file_path = format!("temp/{}/{}", date, "dialogue-1.xlsx");
    let second_dialogue_file_path = format!("temp/{}/{}", date, "dialogue-2.xlsx");
    let invoicing_file_path = format!("temp/{}/{}", date, "invoicing-report.csv");

    let mut first_dialogue_workbook: Xlsx<_> =
        open_workbook(first_dialogue_file_path).expect("Cannot open first dialogue file.");

    let first_sheet = first_dialogue_workbook
        .worksheet_range(first_dialogue_workbook.sheet_names()[0].as_str())
        .expect("Cannot open first dialogue sheet.");

    let mut second_dialogue_workbook: Xlsx<_> =
        open_workbook(second_dialogue_file_path).expect("Cannot open second dialogue file.");

    let second_sheet = second_dialogue_workbook
        .worksheet_range(second_dialogue_workbook.sheet_names()[0].as_str())
        .expect("Cannot open second dialogue sheet.");

    println!("✅ Successfully opened all dialogue files.");

    let invoicing_sheet = csv::Reader::from_path(invoicing_file_path)
        .expect("Cannot open invoicing file.")
        .into_records()
        .map(|record| record.expect("Cannot read invoicing file."))
        .collect::<Vec<_>>();

    println!("✅ Successfully opened invoicing file.");

    println!("❕ Consolidating files...");

    for (index, row) in first_sheet.rows().enumerate() {
        let row = row.iter().map(|cell| cell.to_string()).collect::<Vec<_>>();

        println!("❕ Consolidating row {:?}", row);
    }

    Ok(())
}
