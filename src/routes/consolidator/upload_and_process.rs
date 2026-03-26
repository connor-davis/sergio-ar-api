use std::{collections::HashMap, fs, io::Write};

use anyhow::Error;
use axum::{
    extract::{Multipart, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use calamine::{open_workbook, Reader, Xlsx};
use chrono::{Datelike, NaiveDateTime};
use csv::{ReaderBuilder, StringRecord};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    fs::{create_dir, try_exists},
    spawn,
};

use crate::AppState;

#[derive(Deserialize)]
pub struct UploadAndProcessQuery {
    pub date: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DialogueRow {
    pub shift_group: String,
    pub shift: String,
    pub teacher_name: String,
    pub start_date: String,
    pub end_date: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DialogueConsolidatedRow {
    pub shift_group: String,
    pub shift: String,
    pub shift_type: String,
    pub teacher_name: String,
    pub start_date: String,
    pub end_date: String,
}

struct DialogueCsvColumns {
    start: usize,
    finish: usize,
    shift: usize,
    shift_group: usize,
    teacher_name: usize,
}

// #[derive(Debug, Clone, Deserialize, Serialize)]
// pub struct InternalPickup {
//     pub shift: String,
//     pub initial_teacher: String,
//     pub new_teacher: String,
//     pub shift_group: String,
// }

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InvoicingRow {
    pub teacher_name: String,
    pub eligible: bool,
    pub activity_start: NaiveDateTime,
    pub activity_end: NaiveDateTime,
    pub shift: String,
}

struct InvoicingCsvColumns {
    teacher_name: usize,
    eligible: usize,
    activity_start: usize,
    activity_end: usize,
    shift: Vec<usize>,
}

fn parse_process_date(process_date: &str) -> Result<NaiveDateTime, Error> {
    let process_date_split = process_date.split("-").collect::<Vec<_>>();
    let process_date_day = process_date_split[2];
    let process_date_month = process_date_split[1];
    let process_date_year = process_date_split[0];

    NaiveDateTime::parse_from_str(
        &format!("{}/{}/{} 00:00:00", process_date_day, process_date_month, process_date_year),
        "%d/%m/%Y %H:%M:%S",
    )
    .map_err(Error::from)
}

fn parse_dialogue_datetime(value: &str) -> Result<NaiveDateTime, Error> {
    let value = value.trim().trim_matches('"');
    let supported_formats = [
        "%m/%d/%Y %I:%M %p",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ];

    for format in supported_formats {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(value, format) {
            return Ok(parsed);
        }
    }

    Err(anyhow::anyhow!(
        "Unsupported dialogue datetime format: {}",
        value
    ))
}

fn is_same_day(left: NaiveDateTime, right: NaiveDateTime) -> bool {
    left.day() == right.day() && left.month() == right.month() && left.year() == right.year()
}

fn parse_invoicing_datetime(value: &str) -> Result<NaiveDateTime, Error> {
    let value = value.trim().trim_matches('"');
    let supported_formats = [
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M %p",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ];

    for format in supported_formats {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(value, format) {
            return Ok(parsed);
        }
    }

    Err(anyhow::anyhow!(
        "Unsupported invoicing datetime format: {}",
        value
    ))
}

fn parse_eligible_status(value: &str) -> bool {
    normalize_csv_header(value) == "eligible"
}

fn normalize_csv_header(header: &str) -> String {
    header
        .trim()
        .trim_start_matches('\u{feff}')
        .to_ascii_lowercase()
}

fn preprocess_malformed_csv(contents: &str) -> String {
    // Simple approach: remove all quotes from the CSV content
    // This handles both malformed CSV with extra quotes and normal quoted CSV
    // uniformly by stripping all quote characters.
    contents.replace('"', "")
}

fn find_header_index(headers: &StringRecord, aliases: &[&str]) -> Option<usize> {
    headers.iter().position(|header| {
        let normalized_header = normalize_csv_header(header);

        aliases
            .iter()
            .any(|alias| normalized_header == normalize_csv_header(alias))
    })
}

fn build_dialogue_csv_columns(headers: &StringRecord) -> Result<DialogueCsvColumns, Error> {
    let start = find_header_index(headers, &["Start", "Activity Start"])
        .ok_or_else(|| anyhow::anyhow!("Dialogue CSV is missing a Start column"))?;
    let finish = find_header_index(headers, &["Finish", "End", "Activity End"])
        .ok_or_else(|| anyhow::anyhow!("Dialogue CSV is missing a Finish column"))?;
    let shift = find_header_index(headers, &["Shift: Shift Number", "Shift Number", "Shift"])
        .ok_or_else(|| anyhow::anyhow!("Dialogue CSV is missing a Shift column"))?;
    let shift_group = find_header_index(headers, &["Resource: Shift Group", "Shift Group"])
        .ok_or_else(|| anyhow::anyhow!("Dialogue CSV is missing a Shift Group column"))?;
    let teacher_name =
        find_header_index(headers, &["Resource: Resource Name", "Resource Name", "Teacher Name"])
            .ok_or_else(|| anyhow::anyhow!("Dialogue CSV is missing a Teacher Name column"))?;

    Ok(DialogueCsvColumns {
        start,
        finish,
        shift,
        shift_group,
        teacher_name,
    })
}

fn build_invoicing_csv_columns(headers: &StringRecord) -> Result<InvoicingCsvColumns, Error> {
    let teacher_name = find_header_index(headers, &["Teacher_Name", "Teacher Name"])
        .ok_or_else(|| anyhow::anyhow!("Invoicing CSV is missing a Teacher Name column"))?;
    let eligible = find_header_index(headers, &["Eligible_Status", "Eligible Status", "Eligible"])
        .ok_or_else(|| anyhow::anyhow!("Invoicing CSV is missing an Eligible column"))?;
    let activity_start = find_header_index(
        headers,
        &["Activity_Start_Time", "Activity Start Time", "Activity Start"],
    )
    .ok_or_else(|| anyhow::anyhow!("Invoicing CSV is missing an Activity Start column"))?;
    let activity_end = find_header_index(
        headers,
        &["Activity_End_Time", "Activity End Time", "Activity End"],
    )
    .ok_or_else(|| anyhow::anyhow!("Invoicing CSV is missing an Activity End column"))?;

    let shift = ["shift_name_tsm", "Shift_Name", "Shift Name", "Shift"]
        .iter()
        .filter_map(|alias| find_header_index(headers, &[*alias]))
        .collect::<Vec<_>>();

    if shift.is_empty() {
        return Err(anyhow::anyhow!("Invoicing CSV is missing a Shift column"));
    }

    Ok(InvoicingCsvColumns {
        teacher_name,
        eligible,
        activity_start,
        activity_end,
        shift,
    })
}

fn first_non_empty_field(record: &StringRecord, indexes: &[usize]) -> String {
    indexes
        .iter()
        .filter_map(|index| record.get(*index))
        .map(str::trim)
        .find(|value| !value.is_empty())
        .unwrap_or_default()
        .to_string()
}

fn load_dialogue_rows_from_csv(
    file_path: &str,
    process_date: NaiveDateTime,
) -> Result<Vec<DialogueRow>, Error> {
    let file_contents = fs::read(file_path)?;
    let file_contents = String::from_utf8_lossy(&file_contents);

    // Preprocess to remove all quotes
    let file_contents = preprocess_malformed_csv(&file_contents);

    // Always use comma as delimiter
    let delimiter = b',';

    tracing::info!(
        "📄 Parsing dialogue CSV {} using delimiter {:?}",
        file_path,
        delimiter as char
    );

    let mut reader = ReaderBuilder::new()
        .trim(csv::Trim::All)
        .delimiter(delimiter)
        .flexible(true)
        .from_reader(file_contents.as_bytes());

    let headers = reader.headers()?.clone();
    let columns = build_dialogue_csv_columns(&headers)?;

    let mut rows = Vec::new();

    for (index, record) in reader.records().enumerate() {
        let record = match record {
            Ok(record) => record,
            Err(error) => {
                tracing::warn!(
                    "Skipping malformed dialogue CSV row {} in {}: {:?}",
                    index + 2,
                    file_path,
                    error
                );
                continue;
            }
        };

        let start = record.get(columns.start).unwrap_or("").trim();
        let finish = record.get(columns.finish).unwrap_or("").trim();
        let shift = record.get(columns.shift).unwrap_or("").trim();
        let shift_group = record.get(columns.shift_group).unwrap_or("").trim();
        let teacher_name = record.get(columns.teacher_name).unwrap_or("").trim();

        if start.is_empty()
            || finish.is_empty()
            || shift.is_empty()
            || shift_group.is_empty()
            || teacher_name.is_empty()
        {
            tracing::warn!(
                "Skipping dialogue CSV row {} in {} due to missing required columns",
                index + 2,
                file_path
            );
            continue;
        }

        let start_date_res = parse_dialogue_datetime(start);
        let end_date_res = parse_dialogue_datetime(finish);

        let (start_date, end_date) = match (start_date_res, end_date_res) {
            (Ok(start_date), Ok(end_date)) => (start_date, end_date),
            (Err(start_error), Err(end_error)) => {
                tracing::warn!(
                    "Skipping dialogue CSV row {} in {} due to invalid start and finish datetimes: {:?}, {:?}",
                    index + 2,
                    file_path,
                    start_error,
                    end_error
                );
                continue;
            }
            (Err(error), _) => {
                tracing::warn!(
                    "Skipping dialogue CSV row {} in {} due to invalid start datetime: {:?}",
                    index + 2,
                    file_path,
                    error
                );
                continue;
            }
            (_, Err(error)) => {
                tracing::warn!(
                    "Skipping dialogue CSV row {} in {} due to invalid finish datetime: {:?}",
                    index + 2,
                    file_path,
                    error
                );
                continue;
            }
        };

        if is_same_day(start_date, process_date) || is_same_day(end_date, process_date) {
            rows.push(DialogueRow {
                shift_group: shift_group.to_string(),
                shift: shift.to_string(),
                teacher_name: teacher_name.to_string(),
                start_date: start.to_string(),
                end_date: finish.to_string(),
            });
        }
    }

    Ok(rows)
}

fn load_dialogue_rows_from_xlsx(
    file_path: &str,
    process_date: NaiveDateTime,
) -> Result<Vec<DialogueRow>, Error> {
    let mut workbook: Xlsx<_> =
        open_workbook(file_path).map_err(|e| anyhow::anyhow!("Cannot open xlsx file: {}", e))?;

    let sheet = workbook
        .worksheet_range(workbook.sheet_names()[0].as_str())
        .map_err(|e| anyhow::anyhow!("Cannot open xlsx sheet: {}", e))?;

    let mut rows = Vec::new();

    let mut shift_group_temp = String::new();
    let mut shift_temp = String::new();
    let mut teacher_name_temp = String::new();
    let mut start_date_temp = String::new();
    let mut end_date_temp = String::new();

    let file_rows = sheet.rows().enumerate().collect::<Vec<_>>();
    let file_rows = file_rows.iter().skip(10).collect::<Vec<_>>();

    let mut current_row = 0;
    let total_rows = file_rows.len();

    for (_, row) in file_rows {
        current_row += 1;

        if total_rows > 4 && current_row == total_rows - 4 {
            break;
        }

        let row = row.iter().map(|cell| cell.to_string()).collect::<Vec<_>>();

        let shift_group = row.get(0).map(|s| s.as_str()).unwrap_or("");
        if !shift_group.is_empty() {
            shift_group_temp = shift_group.to_string();
        }

        let shift = row.get(6).map(|s| s.as_str()).unwrap_or("");
        if !shift.is_empty() {
            shift_temp = shift.to_string();
        }

        let teacher_name = row.get(1).map(|s| s.as_str()).unwrap_or("");
        if !teacher_name.is_empty() {
            teacher_name_temp = teacher_name.to_string();
        }

        let start_date = row.get(4).map(|s| s.as_str()).unwrap_or("");
        if !start_date.is_empty() {
            start_date_temp = start_date.to_string();
        }

        let end_date = row.get(5).map(|s| s.as_str()).unwrap_or("");
        if !end_date.is_empty() {
            end_date_temp = end_date.to_string();
        }

        if teacher_name_temp.is_empty() || start_date_temp.is_empty() || end_date_temp.is_empty() {
            continue;
        }

        let start_date_res = parse_dialogue_datetime(&start_date_temp);
        let end_date_res = parse_dialogue_datetime(&end_date_temp);

        if let (Ok(start_date), Ok(end_date)) = (start_date_res, end_date_res) {
            if is_same_day(start_date, process_date) || is_same_day(end_date, process_date) {
                rows.push(DialogueRow {
                    shift_group: shift_group_temp.clone(),
                    shift: shift_temp.clone(),
                    teacher_name: teacher_name_temp.clone(),
                    start_date: start_date_temp.clone(),
                    end_date: end_date_temp.clone(),
                });
            }
        }
    }

    Ok(rows)
}

fn load_dialogue_rows(
    base_path: &str,
    slot: u8,
    process_date: NaiveDateTime,
) -> Result<Vec<DialogueRow>, Error> {
    let csv_path = format!("{}/dialogue-{}.csv", base_path, slot);
    let xlsx_path = format!("{}/dialogue-{}.xlsx", base_path, slot);

    if std::path::Path::new(&csv_path).exists() {
        tracing::info!("📄 Loading dialogue-{} from CSV", slot);
        load_dialogue_rows_from_csv(&csv_path, process_date)
    } else if std::path::Path::new(&xlsx_path).exists() {
        tracing::info!("📄 Loading dialogue-{} from XLSX", slot);
        load_dialogue_rows_from_xlsx(&xlsx_path, process_date)
    } else {
        Err(anyhow::anyhow!(
            "dialogue-{} file not found (tried .csv and .xlsx)",
            slot
        ))
    }
}

pub async fn upload_and_process(
    Query(query): Query<UploadAndProcessQuery>,
    State(app_state): State<AppState>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    store_files(&mut multipart, &query.date)
        .await
        .map_err(|_error| {
            tracing::error!("🔥 Upload failed!");

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    "message": "Upload failed. Please contact the developer.",
                })),
            )
        })?;

    tracing::info!("✅ Upload successful!");

    spawn(async move {
        if let Err(error) = consolidate_files(app_state, query.date).await {
            tracing::error!("🔥 Consolidation failed: {:?}", error);
        }
    });

    Ok("Your files are being processed. Please check back periodically to see the processed data.")
}

async fn store_files(multipart: &mut Multipart, date: &str) -> Result<(), Error> {
    let temp_directory_exists = try_exists("temp").await;

    match temp_directory_exists {
        Ok(directory) => {
            if !directory {
                tracing::info!("❕ Temp directory not found. Creating temp directory.");

                let create_dir_result = create_dir("temp").await;

                match create_dir_result {
                    Ok(_) => {
                        tracing::info!("✅ Temp directory created.")
                    }
                    Err(_) => {
                        tracing::error!("🔥 Failed to create the temp directory.")
                    }
                }
            }
        }
        Err(_) => {
            tracing::error!("🔥 Unknown error when checking if the temp directory exists.")
        }
    }

    let directory_path = format!("temp/{}", date);
    let directory_exists = try_exists(&directory_path).await;

    match directory_exists {
        Ok(exists) => {
            if exists {
                tracing::info!("❕ Directory exists. Cleaning.");
                if let Err(e) = tokio::fs::remove_dir_all(&directory_path).await {
                    tracing::error!("🔥 Failed to clean directory: {}", e);
                }
            }
            
            tracing::info!("❕ Creating directory.");
            let create_dir_result = create_dir(&directory_path).await;

            match create_dir_result {
                Ok(_) => {
                    tracing::info!("✅ Directory created.");
                }
                Err(_) => {
                    tracing::error!("🔥 Failed to create directory.");
                }
            }
        }
        Err(_) => {
            tracing::error!("🔥 Unknown error when checking directory exists.")
        }
    }

    while let Some(field) = multipart.next_field().await.unwrap() {
        let name = field.name().unwrap().to_string();
        let original_filename = field.file_name().unwrap_or(&name).to_string();

        let extension = std::path::Path::new(&original_filename)
            .extension()
            .and_then(std::ffi::OsStr::to_str)
            .unwrap_or("")
            .to_lowercase();

        let name_lower = name.to_lowercase();
        let file_name = if !extension.is_empty() && !name_lower.ends_with(&format!(".{}", extension)) {
            format!("{}.{}", name_lower, extension)
        } else {
            name_lower
        };

        let data = field.bytes().await.unwrap();

        let file_path = format!("{}/{}", &directory_path, &file_name);
        let file_exists = try_exists(&file_path).await?;

        let mut file = if file_exists {
            std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&file_path)?
        } else {
            std::fs::File::create(&file_path)?
        };

        let write_result = file.write_all(&data);

        file.flush()?;

        match write_result {
            Ok(_) => {
                tracing::info!("✅ File data written to temporary file: {}", &file_name);
            }
            Err(_) => {
                tracing::error!("🔥 Failed to write file data to temporary file.");
            }
        }
    }

    Ok(())
}

async fn consolidate_files(
    app_state: AppState,
    process_date: String,
) -> Result<impl IntoResponse, Error> {
    let invoicing_file_path = format!("temp/{}/{}", process_date, "invoicing-report.csv");
    let dialogue_base_path = format!("temp/{}", process_date);

    let process_date = parse_process_date(&process_date)?;

    tracing::info!("✅ Successfully opened all dialogue files.");

    let invoicing_file_contents = fs::read(&invoicing_file_path)?;
    let invoicing_file_contents = String::from_utf8_lossy(&invoicing_file_contents);

    // Preprocess to remove all quotes
    let invoicing_file_contents = preprocess_malformed_csv(&invoicing_file_contents);

    // Always use comma as delimiter
    let invoicing_delimiter = b',';

    tracing::info!(
        "📄 Parsing invoicing CSV {} using delimiter {:?}",
        invoicing_file_path,
        invoicing_delimiter as char
    );

    let mut invoicing_reader = ReaderBuilder::new()
        .trim(csv::Trim::All)
        .delimiter(invoicing_delimiter)
        .flexible(true)
        .from_reader(invoicing_file_contents.as_bytes());

    let invoicing_headers = invoicing_reader.headers()?.clone();
    let invoicing_columns = build_invoicing_csv_columns(&invoicing_headers)?;

    tracing::info!("✅ Successfully opened invoicing file.");

    tracing::info!("❕ Consolidating files...");

    // Consolidate first dialogue file
    tracing::info!("❕ Mapping first dialogue file...");
    let first_dialogue_rows = load_dialogue_rows(&dialogue_base_path, 1, process_date)?;

    tracing::info!("✅ Successfully mapped first dialogue file.");

    // Consolidate second dialogue file
    tracing::info!("❕ Mapping second dialogue file...");
    let second_dialogue_rows = load_dialogue_rows(&dialogue_base_path, 2, process_date)?;

    tracing::info!("✅ Successfully mapped second dialogue file.");

    // Consolidate invoicing file
    tracing::info!("❕ Mapping invoicing file...");

    let mut invoicing_rows: Vec<InvoicingRow> = Vec::new();

    for (index, row) in invoicing_reader.records().enumerate() {
        let row = match row {
            Ok(row) => row,
            Err(error) => {
                tracing::warn!(
                    "Skipping malformed invoicing CSV row {} in {}: {:?}",
                    index + 2,
                    invoicing_file_path,
                    error
                );
                continue;
            }
        };

        let teacher_name = row
            .get(invoicing_columns.teacher_name)
            .unwrap_or("")
            .trim()
            .to_string();
        let eligible = parse_eligible_status(row.get(invoicing_columns.eligible).unwrap_or(""));
        let activity_start = row
            .get(invoicing_columns.activity_start)
            .unwrap_or("")
            .trim();
        let activity_end = row
            .get(invoicing_columns.activity_end)
            .unwrap_or("")
            .trim();
        let shift = first_non_empty_field(&row, &invoicing_columns.shift);

        if teacher_name.is_empty() || activity_start.is_empty() || activity_end.is_empty() {
            tracing::warn!(
                "Skipping invoicing CSV row {} in {} due to missing required columns",
                index + 2,
                invoicing_file_path
            );
            continue;
        }

        let activity_start_date = match parse_invoicing_datetime(activity_start) {
            Ok(parsed) => parsed,
            Err(error) => {
                tracing::warn!(
                    "Skipping invoicing CSV row {} in {} due to invalid activity start datetime: {:?}",
                    index + 2,
                    invoicing_file_path,
                    error
                );
                continue;
            }
        };

        let activity_end_date = match parse_invoicing_datetime(activity_end) {
            Ok(parsed) => parsed,
            Err(error) => {
                tracing::warn!(
                    "Skipping invoicing CSV row {} in {} due to invalid activity end datetime: {:?}",
                    index + 2,
                    invoicing_file_path,
                    error
                );
                continue;
            }
        };

        let invoicing_row = InvoicingRow {
            teacher_name,
            eligible,
            activity_start: activity_start_date,
            activity_end: activity_end_date,
            shift,
        };

        let invoicing_row_date = invoicing_row.activity_start;

        //  Check that the day, month and year are the same as the process date
        if invoicing_row_date.day() == process_date.day()
            && invoicing_row_date.month() == process_date.month()
            && invoicing_row_date.year() == process_date.year()
        {
            invoicing_rows.push(invoicing_row);
        }
    }

    tracing::info!("✅ Successfully mapped invoicing file.");

    tracing::info!(
        "❕ Storing invoice {:?} rows to the database.",
        invoicing_rows.len()
    );

    let mut inserted_invoices = 0;
    let mut skipped_invoices = 0;
    let mut updated_invoices = 0;

    for invoicing_row in invoicing_rows {
        let InvoicingRow {
            teacher_name,
            eligible,
            activity_start,
            activity_end,
            shift,
        } = invoicing_row;

        // See if the invoice row already exists
        let existing_invoice_row = sqlx::query_scalar::<_, i32>(
            r#"
                SELECT
                    id
                FROM invoices
                WHERE teacher_name = $1 AND shift = $2 AND activity_start = $3 AND activity_end = $4
            "#
        )
        .bind(&teacher_name)
        .bind(&shift)
        .bind(activity_start)
        .bind(activity_end)
        .fetch_optional(&app_state.db)
        .await
        .map_err(|error| {
            tracing::error!("Error fetching existing invoice row: {:?}", error);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    "message": "Error fetching existing invoice row. Please contact the developer.",
                })),
            )
        });

        match existing_invoice_row {
            Ok(existing_invoice_row) => match existing_invoice_row {
                Some(existing_invoice_id) => {
                    let update_result = sqlx::query(
                            r#"
                                UPDATE invoices
                                SET
                                    eligible = $1
                                WHERE id = $2
                            "#
                        )
                        .bind(eligible)
                        .bind(existing_invoice_id)
                        .execute(&app_state.db)
                        .await
                        .map_err(|error| {
                            tracing::error!("Error updating invoice row: {:?}", error);
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({
                                    "status": StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                                    "message": "Error updating invoice row. Please contact the developer.",
                                })),
                            )
                        });

                    match update_result {
                        Ok(_) => {
                            updated_invoices += 1;
                        }
                        Err(_) => {
                            tracing::error!("🔥 Error updating invoice row.");

                            skipped_invoices += 1;
                        }
                    }
                }
                None => {
                    let insert_result = sqlx::query(
                            r#"
                                INSERT INTO invoices (
                                    teacher_name,
                                    eligible,
                                    activity_start,
                                    activity_end,
                                    shift
                                )
                                VALUES (
                                    $1,
                                    $2,
                                    $3,
                                    $4,
                                    $5
                                )
                            "#
                        )
                        .bind(&teacher_name)
                        .bind(eligible)
                        .bind(activity_start)
                        .bind(activity_end)
                        .bind(&shift)
                        .execute(&app_state.db)
                        .await
                        .map_err(|error| {
                            tracing::error!("Error inserting invoice row: {:?}", error);
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({
                                    "status": StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                                    "message": "Error inserting invoice row. Please contact the developer.",
                                })),
                            )
                        });

                    match insert_result {
                        Ok(_) => {
                            inserted_invoices += 1;
                        }
                        Err(_) => {
                            tracing::error!("🔥 Error inserting invoice row.");

                            skipped_invoices += 1;
                        }
                    }
                }
            },
            Err(_) => {
                tracing::info!("🔥 Error fetching existing invoice row.");

                skipped_invoices += 1;
            }
        }
    }

    tracing::info!("❕ Consolidating dialogues...");

    tracing::info!("❕ First dialogue rows: {}", first_dialogue_rows.len());
    tracing::info!("❕ Second dialogue rows: {}", second_dialogue_rows.len());

    let mut consolidated_rows: Vec<DialogueConsolidatedRow> = Vec::new();

    let first_shifts = first_dialogue_rows
        .iter()
        .map(|row| row.shift.clone())
        .collect::<Vec<String>>();
    let second_shifts = second_dialogue_rows
        .iter()
        .map(|row| row.shift.clone())
        .collect::<Vec<String>>();

    // let taught_shifts = first_shifts
    //     .iter()
    //     .filter(|shift| {
    //         let is_in_second_shifts = second_shifts.contains(shift);

    //         let shift_teacher = first_dialogue_rows
    //             .iter()
    //             .find(|row| row.shift == **shift)
    //             .unwrap()
    //             .teacher_name
    //             .clone();

    //         let second_shift_teacher = second_dialogue_rows
    //             .iter()
    //             .find(|row| row.shift == **shift)
    //             .unwrap()
    //             .teacher_name
    //             .clone();

    //         let is_same_teacher = shift_teacher == second_shift_teacher;

    //         is_in_second_shifts && is_same_teacher
    //     })
    //     .collect::<Vec<&String>>();

    // let dropped_shifts = first_shifts
    //     .iter()
    //     .filter(|shift| !second_shifts.contains(shift))
    //     .collect::<Vec<&String>>();

    // let mut internal_pickups: Vec<InternalPickup> = vec![];

    // for first_dialogue_row in first_dialogue_rows {
    //     let is_in_second_shifts = second_shifts.contains(&first_dialogue_row.shift);

    //     if is_in_second_shifts {
    //         let second_dialogue_row = second_dialogue_rows
    //             .iter()
    //             .find(|row| row.shift == first_dialogue_row.shift)
    //             .unwrap();

    //         let shift_teacher = first_dialogue_row.teacher_name.clone();
    //         let second_shift_teacher = second_dialogue_row.teacher_name.clone();

    //         if shift_teacher != second_shift_teacher {
    //             let internal_pickup = InternalPickup {
    //                 shift: first_dialogue_row.shift.clone(),
    //                 initial_teacher: shift_teacher,
    //                 new_teacher: second_shift_teacher,
    //                 shift_group: first_dialogue_row.shift_group.clone(),
    //             };

    //             internal_pickups.push(internal_pickup);
    //         }
    //     }
    // }

    let dropped_shifts = first_shifts
        .iter()
        .filter(|shift| !second_shifts.contains(shift))
        .collect::<Vec<&String>>();
    let pick_up_shifts = second_shifts
        .iter()
        .filter(|shift| !first_shifts.contains(shift))
        .collect::<Vec<&String>>();

    let undecided_first_dialog_rows = first_dialogue_rows
        .iter()
        .filter(|row| {
            !dropped_shifts.contains(&&row.shift) && !pick_up_shifts.contains(&&row.shift)
        })
        .collect::<Vec<&DialogueRow>>();

    let undecided_second_dialog_rows = second_dialogue_rows
        .iter()
        .filter(|row| {
            !dropped_shifts.contains(&&row.shift) && !pick_up_shifts.contains(&&row.shift)
        })
        .collect::<Vec<&DialogueRow>>();

    let previous_shift_teachers = undecided_first_dialog_rows
        .iter()
        .map(|row| {
            (
                row.shift.clone(),
                format!("{}:{}", row.teacher_name, row.shift_group),
            )
        })
        .collect::<HashMap<String, String>>();

    let mut internal_pick_up_shifts: Vec<String> = vec![];
    let mut dropped_and_picked_up_shifts: Vec<String> = vec![];

    for second_dialogue_row in undecided_second_dialog_rows {
        let previous_shift_assignment = previous_shift_teachers.get(&second_dialogue_row.shift);

        match previous_shift_assignment {
            Some(previous_shift_assignment) => {
                let shift_assignment_split =
                    previous_shift_assignment.split(":").collect::<Vec<_>>();

                let shift_teacher = shift_assignment_split[0];
                let shift_group = shift_assignment_split[1];

                if shift_teacher != second_dialogue_row.teacher_name
                    && shift_group == second_dialogue_row.shift_group
                {
                    internal_pick_up_shifts.push(second_dialogue_row.shift.clone());
                }

                if shift_teacher != second_dialogue_row.teacher_name
                    && shift_group != second_dialogue_row.shift_group
                {
                    dropped_and_picked_up_shifts.push(second_dialogue_row.shift.clone());
                }
            }
            None => {}
        }
    }

    for current_dialogue_row in first_dialogue_rows {
        let is_dropped = dropped_shifts.contains(&&current_dialogue_row.shift);
       
        let mut consolidated_row = DialogueConsolidatedRow {
            shift_group: current_dialogue_row.shift_group.clone(),
            shift: current_dialogue_row.shift.clone(),
            shift_type: "-".to_string(),
            teacher_name: current_dialogue_row.teacher_name.clone(),
            start_date: current_dialogue_row.start_date.clone(),
            end_date: current_dialogue_row.end_date.clone(),
        };

        match is_dropped {
            true => {
                consolidated_row.shift_type = "Dropped".to_string();

                consolidated_rows.push(consolidated_row);
            }
            false => {}
        }
    }

    for current_dialogue_row in second_dialogue_rows {
        let is_pick_up = pick_up_shifts.contains(&&current_dialogue_row.shift);
        let is_internal_pick_up = internal_pick_up_shifts.contains(&&current_dialogue_row.shift);
        let is_dropped_and_picked_up =
            dropped_and_picked_up_shifts.contains(&&current_dialogue_row.shift);

        let mut consolidated_row = DialogueConsolidatedRow {
            shift_group: current_dialogue_row.shift_group.clone(),
            shift: current_dialogue_row.shift.clone(),
            shift_type: "-".to_string(),
            teacher_name: current_dialogue_row.teacher_name.clone(),
            start_date: current_dialogue_row.start_date.clone(),
            end_date: current_dialogue_row.end_date.clone(),
        };

        match is_pick_up {
            true => {
                consolidated_row.shift_type = "Pickup".to_string();

                consolidated_rows.push(consolidated_row);

                continue;
            }
            false => {}
        }

        match is_internal_pick_up {
            true => {
                consolidated_row.shift_type = "Internal Pickup".to_string();

                consolidated_rows.push(consolidated_row);

                continue;
            }
            false => {}
        }

        match is_dropped_and_picked_up {
            true => {
                consolidated_row.shift_type = "Dropped & Picked Up".to_string();

                consolidated_rows.push(consolidated_row);

                continue;
            }
            false => {}
        }

        consolidated_rows.push(consolidated_row);
    }

    // let mut first_dialogue_rows_split: HashMap<String, Vec<DialogueRow>> = HashMap::new();

    // for row in first_dialogue_rows {
    //     let shift_group = row.shift_group.clone();

    //     match first_dialogue_rows_split.get_mut(&shift_group) {
    //         Some(shift_group_rows) => {
    //             shift_group_rows.push(row);
    //         }
    //         None => {
    //             let mut shift_group_rows: Vec<DialogueRow> = Vec::new();

    //             shift_group_rows.push(row);

    //             first_dialogue_rows_split.insert(shift_group, shift_group_rows);
    //         }
    //     }
    // }

    // let mut second_dialogue_rows_split: HashMap<String, Vec<DialogueRow>> = HashMap::new();

    // for row in second_dialogue_rows {
    //     let shift_group = row.shift_group.clone();

    //     match second_dialogue_rows_split.get_mut(&shift_group) {
    //         Some(shift_group_rows) => {
    //             shift_group_rows.push(row);
    //         }
    //         None => {
    //             let mut shift_group_rows: Vec<DialogueRow> = Vec::new();

    //             shift_group_rows.push(row);

    //             second_dialogue_rows_split.insert(shift_group, shift_group_rows);
    //         }
    //     }
    // }

    // for (shift_group, first_dialogue_rows) in first_dialogue_rows_split {
    //     let second_dialogue_rows = second_dialogue_rows_split.get(&shift_group);

    //     match second_dialogue_rows {
    //         Some(second_dialogue_rows) => {
    //             let first_shifts = first_dialogue_rows
    //                 .iter()
    //                 .map(|row| row.shift.clone())
    //                 .collect::<Vec<String>>();
    //             let second_shifts = second_dialogue_rows
    //                 .iter()
    //                 .map(|row| row.shift.clone())
    //                 .collect::<Vec<String>>();

    //             let lost_shifts = first_shifts
    //                 .iter()
    //                 .filter(|shift| !second_shifts.contains(shift))
    //                 .collect::<Vec<&String>>();
    //             let new_shifts = second_shifts
    //                 .iter()
    //                 .filter(|shift| !first_shifts.contains(shift))
    //                 .collect::<Vec<&String>>();

    //             let previous_shift_teachers = first_dialogue_rows
    //                 .iter()
    //                 .map(|row| (row.shift.clone(), row.teacher_name.clone()))
    //                 .collect::<HashMap<String, String>>();

    //             let picked_up_shifts = second_dialogue_rows
    //                 .iter()
    //                 .filter(|row| {
    //                     let previous_shift_teacher = previous_shift_teachers.get(&row.shift);

    //                     match previous_shift_teacher {
    //                         Some(previous_shift_teacher) => {
    //                             if previous_shift_teacher != &row.teacher_name {
    //                                 true
    //                             } else {
    //                                 false
    //                             }
    //                         }
    //                         None => false,
    //                     }
    //                 })
    //                 .map(|row| row.shift.clone())
    //                 .collect::<Vec<String>>();

    //             let lost_but_picked_up_shifts = first_dialogue_rows
    //                 .iter()
    //                 .filter(|row| {
    //                     let second_shift = second_shifts.contains(&row.shift);

    //                     if second_shift {
    //                         let second_dialogue_row = second_dialogue_rows
    //                             .iter()
    //                             .find(|second_dialogue_row| second_dialogue_row.shift == row.shift);

    //                         match second_dialogue_row {
    //                             Some(second_dialogue_row) => {
    //                                 if second_dialogue_row.teacher_name != row.teacher_name {
    //                                     true
    //                                 } else {
    //                                     false
    //                                 }
    //                             }
    //                             None => false,
    //                         }
    //                     } else {
    //                         false
    //                     }
    //                 })
    //                 .map(|row| row.shift.clone())
    //                 .collect::<Vec<String>>();

    //             for current_shift in second_dialogue_rows
    //                 .iter()
    //                 .filter(|row| new_shifts.contains(&&row.shift))
    //                 .collect::<Vec<&DialogueRow>>()
    //             {
    //                 let consolidated_row = DialogueConsolidatedRow {
    //                     shift_group: shift_group.clone(),
    //                     shift: current_shift.shift.clone(),
    //                     shift_type: "Pickup".to_string(),
    //                     teacher_name: current_shift.teacher_name.clone(),
    //                     start_date: current_shift.start_date.clone(),
    //                     end_date: current_shift.end_date.clone(),
    //                 };

    //                 consolidated_rows.push(consolidated_row);
    //             }

    //             for current_shift in second_dialogue_rows
    //                 .iter()
    //                 .filter(|row| picked_up_shifts.contains(&&row.shift))
    //                 .collect::<Vec<&DialogueRow>>()
    //             {
    //                 let consolidated_row = DialogueConsolidatedRow {
    //                     shift_group: shift_group.clone(),
    //                     shift: current_shift.shift.clone(),
    //                     shift_type: "Internal Pickup".to_string(),
    //                     teacher_name: current_shift.teacher_name.clone(),
    //                     start_date: current_shift.start_date.clone(),
    //                     end_date: current_shift.end_date.clone(),
    //                 };

    //                 consolidated_rows.push(consolidated_row);
    //             }

    //             for current_shift in first_dialogue_rows
    //                 .iter()
    //                 .filter(|row| lost_shifts.contains(&&row.shift))
    //                 .collect::<Vec<&DialogueRow>>()
    //             {
    //                 let consolidated_row = DialogueConsolidatedRow {
    //                     shift_group: shift_group.clone(),
    //                     shift: current_shift.shift.clone(),
    //                     shift_type: "Dropped".to_string(),
    //                     teacher_name: current_shift.teacher_name.clone(),
    //                     start_date: current_shift.start_date.clone(),
    //                     end_date: current_shift.end_date.clone(),
    //                 };

    //                 consolidated_rows.push(consolidated_row);
    //             }

    //             for current_shift in first_dialogue_rows
    //                 .iter()
    //                 .filter(|row| lost_but_picked_up_shifts.contains(&&row.shift))
    //                 .collect::<Vec<&DialogueRow>>()
    //             {
    //                 let consolidated_row = DialogueConsolidatedRow {
    //                     shift_group: shift_group.clone(),
    //                     shift: current_shift.shift.clone(),
    //                     shift_type: "Dropped & Picked Up".to_string(),
    //                     teacher_name: current_shift.teacher_name.clone(),
    //                     start_date: current_shift.start_date.clone(),
    //                     end_date: current_shift.end_date.clone(),
    //                 };

    //                 consolidated_rows.push(consolidated_row);
    //             }

    //             for current_shift in second_dialogue_rows
    //                 .iter()
    //                 .filter(|row| {
    //                     !picked_up_shifts.contains(&&row.shift)
    //                         && !new_shifts.contains(&&row.shift)
    //                         && !lost_shifts.contains(&&row.shift)
    //                         && !lost_but_picked_up_shifts.contains(&&row.shift)
    //                 })
    //                 .collect::<Vec<&DialogueRow>>()
    //             {
    //                 let consolidated_row = DialogueConsolidatedRow {
    //                     shift_group: shift_group.clone(),
    //                     shift: current_shift.shift.clone(),
    //                     shift_type: "-".to_string(),
    //                     teacher_name: current_shift.teacher_name.clone(),
    //                     start_date: current_shift.start_date.clone(),
    //                     end_date: current_shift.end_date.clone(),
    //                 };

    //                 consolidated_rows.push(consolidated_row);
    //             }
    //         }
    //         None => {}
    //     }
    // }

    tracing::info!("❕ Consolidated rows: {}", consolidated_rows.len());

    tracing::info!("✅ Successfully consolidated dialogues.");

    consolidated_rows.sort_by(|a, b| {
        match (
            parse_dialogue_datetime(&a.start_date),
            parse_dialogue_datetime(&b.start_date),
        ) {
            (Ok(a_date), Ok(b_date)) => a_date.cmp(&b_date),
            _ => a.start_date.cmp(&b.start_date),
        }
    });

    consolidated_rows.sort_by(|a, b| a.teacher_name.cmp(&b.teacher_name));
    consolidated_rows.sort_by(|a, b| a.shift_group.cmp(&b.shift_group));

    // Use the invoicing rows to determine which consolidated rows are eligible
    let consolidated_rows: Vec<&DialogueConsolidatedRow> = consolidated_rows
        .iter()
        .filter(|row| {
            match parse_dialogue_datetime(&row.start_date) {
                Ok(row_start_date) => {
                    row_start_date.day() == process_date.day()
                        && row_start_date.month() == process_date.month()
                        && row_start_date.year() == process_date.year()
                }
                Err(error) => {
                    tracing::warn!(
                        "Skipping consolidated row for teacher {} due to invalid start datetime {:?}: {:?}",
                        row.teacher_name,
                        row.start_date,
                        error
                    );
                    false
                }
            }
        })
        .collect::<Vec<&DialogueConsolidatedRow>>();

    tracing::info!("❕ Consolidated rows: {}", consolidated_rows.len());

    tracing::info!("❕ Storing consolidated rows to the database...");

    let mut new_teachers = 0;
    let mut skipped_teachers = 0;
    let mut new_shifts = 0;
    let mut skipped_shifts = 0;

    for consolidated_row in consolidated_rows {
        let teacher_name = consolidated_row.teacher_name.clone();
        let shift_group = consolidated_row.shift_group.clone();
        let shift = consolidated_row.shift.clone();
        let shift_type = consolidated_row.shift_type.clone();
        let start_date = parse_dialogue_datetime(&consolidated_row.start_date).map_err(|error| {
            tracing::error!(
                "🔥 Failed to parse consolidated row start datetime {:?}: {:?}",
                consolidated_row.start_date,
                error
            );

            Error::msg("Failed to parse consolidated row start datetime.")
        })?;
        let end_date = parse_dialogue_datetime(&consolidated_row.end_date).map_err(|error| {
            tracing::error!(
                "🔥 Failed to parse consolidated row end datetime {:?}: {:?}",
                consolidated_row.end_date,
                error
            );

            Error::msg("Failed to parse consolidated row end datetime.")
        })?;

        let teacher_found = sqlx::query_scalar::<_, i32>("SELECT id FROM teachers WHERE name = $1")
            .bind(&teacher_name)
            .fetch_optional(&app_state.db)
            .await
            .map_err(|_| {
                tracing::error!("🔥 Failed to fetch teacher from the database.");

                Error::msg("Failed to fetch teacher from the database.")
            })?;

        let teacher_found = match teacher_found {
            Some(teacher_id) => {
                skipped_teachers += 1;

                teacher_id
            }
            None => {
                let insert_teacher_id = sqlx::query_scalar::<_, i32>(
                    "INSERT INTO teachers (name) VALUES ($1) RETURNING id"
                )
                .bind(&teacher_name)
                .fetch_one(&app_state.db)
                .await
                .map_err(|_| {
                    tracing::error!("🔥 Failed to insert teacher into the database.");

                    Error::msg("Failed to insert teacher into the database.")
                })?;

                new_teachers += 1;

                insert_teacher_id
            }
        };

        let schedule_found = sqlx::query_scalar::<_, i32>(
            "SELECT id FROM schedules WHERE teacher_id = $1 AND start_date = $2 AND end_date = $3 AND shift = $4 AND shift_type = $5 AND shift_group = $6"
        )
        .bind(teacher_found)
        .bind(start_date)
        .bind(end_date)
        .bind(&shift)
        .bind(&shift_type)
        .bind(&shift_group)
        .fetch_optional(&app_state.db)
        .await
        .map_err(|_| {
            tracing::error!("🔥 Failed to fetch schedule from the database.");

            Error::msg("Failed to fetch schedule from the database.")
        })?;

        match schedule_found {
            Some(_) => {
                skipped_shifts += 1;
            }
            None => {
                sqlx::query(
                    "INSERT INTO schedules (teacher_id, start_date, end_date, shift, shift_type, shift_group) VALUES ($1, $2, $3, $4, $5, $6)"
                )
                .bind(teacher_found)
                .bind(start_date)
                .bind(end_date)
                .bind(&shift)
                .bind(&shift_type)
                .bind(&shift_group)
                .execute(&app_state.db)
                .await
                .map_err(|_| {
                    tracing::error!("🔥 Failed to insert schedule into the database.");

                    Error::msg("Failed to insert schedule into the database.")
                })?;

                new_shifts += 1;
            }
        }
    }

    tracing::info!("✅ Invoicing consolidation complete.");

    Ok(Json(json!({
        "status": StatusCode::OK.as_u16(),
        "message": "Consolidation successful.",
        "new_teachers": new_teachers,
        "skipped_teachers": skipped_teachers,
        "new_shifts": new_shifts,
        "skipped_shifts": skipped_shifts,
        "inserted_invoices": inserted_invoices,
        "skipped_invoices": skipped_invoices,
        "updated_invoices": updated_invoices,
    })))
}
