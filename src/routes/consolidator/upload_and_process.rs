use std::{collections::HashMap, fs::File, io::Write};

use anyhow::Error;
use axum::{
    extract::{Multipart, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use calamine::{open_workbook, Reader, Xlsx};
use chrono::{Datelike, NaiveDateTime};
use csv::ReaderBuilder;
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InvoicingRow {
    pub teacher_name: String,
    pub eligible: bool,
    pub activity_start: NaiveDateTime,
    pub activity_end: NaiveDateTime,
    pub shift: String,
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

    spawn(consolidate_files(app_state, query.date));

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
        Ok(directory) => {
            if !directory {
                tracing::info!("❕ Directory not found. Creating.");

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
        }
        Err(_) => {
            tracing::error!("🔥 Unknown error when checking directory exists.")
        }
    }

    while let Some(field) = multipart.next_field().await.unwrap() {
        let name = field.name().unwrap().to_string();
        let data = field.bytes().await.unwrap();

        let file_path = format!("{}/{}", &directory_path, &name);
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
                tracing::info!("✅ File data written to temporary file: {}", &name);
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
    let first_dialogue_file_path = format!("temp/{}/{}", process_date, "dialogue-1.xlsx");
    let second_dialogue_file_path = format!("temp/{}/{}", process_date, "dialogue-2.xlsx");
    let invoicing_file_path = format!("temp/{}/{}", process_date, "invoicing-report.csv");

    let process_date_split = process_date.split("-").collect::<Vec<_>>();
    let process_date_day = process_date_split[2];
    let process_date_month = process_date_split[1];
    let process_date_year = process_date_split[0];

    let process_date = format!(
        "{}/{}/{}",
        process_date_day, process_date_month, process_date_year
    );

    let process_date =
        NaiveDateTime::parse_from_str(&format!("{} 00:00:00", process_date), "%d/%m/%Y %H:%M:%S")
            .unwrap();

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

    tracing::info!("✅ Successfully opened all dialogue files.");

    let file = File::open(invoicing_file_path)?;
    let mut parser_csv_reader = csv::ReaderBuilder::new().from_reader(file);

    let mut data = String::new();

    for record in parser_csv_reader.byte_records() {
        let record = record?;

        for piece in record.into_iter() {
            for c in piece.into_iter() {
                let byte = c.to_string().parse::<u8>()?;

                if byte == 0 {
                    continue;
                }

                if byte == b'\n' {
                    continue;
                }

                if byte == b'\t' {
                    data.push_str(",")
                } else {
                    data.push(byte as char);
                }
            }

            data.push_str("\n")
        }
    }

    let data = data.replace("\n\n", "\n");

    let reader = ReaderBuilder::new().from_reader(data.as_bytes());

    let invoicing_sheet = reader
        .into_records()
        .map(|record| record.unwrap_or(csv::StringRecord::new()))
        .filter(|record| record.len() > 0)
        .collect::<Vec<_>>();

    tracing::info!("✅ Successfully opened invoicing file.");

    tracing::info!("❕ Consolidating files...");

    let mut first_dialogue_rows: Vec<DialogueRow> = Vec::new();

    let mut shift_group_temp = String::new();
    let mut shift_temp = String::new();
    let mut teacher_name_temp = String::new();
    let mut start_date_temp = String::new();
    let mut end_date_temp = String::new();

    // Consolidate first dialogue file
    tracing::info!("❕ Mapping first dialogue file...");

    let file_rows = first_sheet.rows().enumerate().collect::<Vec<_>>();
    let file_rows = file_rows.iter().skip(10).collect::<Vec<_>>();

    let mut current_row = 0;
    let total_rows = file_rows.len();

    for (_, row) in file_rows {
        current_row += 1;

        if current_row == total_rows - 3 {
            break;
        }

        let row = row.iter().map(|cell| cell.to_string()).collect::<Vec<_>>();

        let shift_group = &row[0];

        if shift_group.len() > 0 {
            shift_group_temp = shift_group.to_string();
        }

        let shift = &row[6];

        if shift.len() > 0 {
            shift_temp = shift.to_string();
        }

        let teacher_name = &row[1];

        if teacher_name.len() > 0 {
            teacher_name_temp = teacher_name.to_string();
        }

        let start_date = &row[4];

        if start_date.len() > 0 {
            start_date_temp = start_date.to_string();
        }

        let end_date = &row[5];

        if end_date.len() > 0 {
            end_date_temp = end_date.to_string();
        }

        let dialogue_row = DialogueRow {
            shift_group: shift_group_temp.to_string(),
            shift: shift_temp.to_string(),
            teacher_name: teacher_name_temp.to_string(),
            start_date: start_date_temp.to_string(),
            end_date: end_date_temp.to_string(),
        };

        let dialogue_row_start_date = dialogue_row.start_date.split(" ").collect::<Vec<_>>()[0];

        println!("{:?}", dialogue_row_start_date);

        let dialogue_row_start_day = dialogue_row_start_date.split("/").collect::<Vec<_>>()[1];
        let dialogue_row_start_month = dialogue_row_start_date.split("/").collect::<Vec<_>>()[0];
        let dialogue_row_start_year = dialogue_row_start_date.split("/").collect::<Vec<_>>()[2];

        let dialogue_row_start_date = format!(
            "{}/{}/{}",
            dialogue_row_start_day, dialogue_row_start_month, dialogue_row_start_year
        );

        let dialogue_row_start_date = NaiveDateTime::parse_from_str(
            &format!("{} 00:00:00", dialogue_row_start_date),
            "%d/%m/%Y %H:%M:%S",
        )
        .unwrap();

        let dialogue_row_end_date = dialogue_row.end_date.split(" ").collect::<Vec<_>>()[0];
        let dialogue_row_end_day = dialogue_row_end_date.split("/").collect::<Vec<_>>()[1];
        let dialogue_row_end_month = dialogue_row_end_date.split("/").collect::<Vec<_>>()[0];
        let dialogue_row_end_year = dialogue_row_end_date.split("/").collect::<Vec<_>>()[2];

        let dialogue_row_end_date = format!(
            "{}/{}/{}",
            dialogue_row_end_day, dialogue_row_end_month, dialogue_row_end_year
        );

        let dialogue_row_end_date = NaiveDateTime::parse_from_str(
            &format!("{} 00:00:00", dialogue_row_end_date),
            "%d/%m/%Y %H:%M:%S",
        )
        .unwrap();

        // Check if the dialogue row date is equal to the process date or if the day and year are the same but the month is less than the process date month
        if dialogue_row_start_date.day() == process_date.day()
            && dialogue_row_start_date.month() == process_date.month()
            && dialogue_row_start_date.year() == process_date.year()
            || dialogue_row_end_date.day() == process_date.day()
                && dialogue_row_end_date.month() == process_date.month()
                && dialogue_row_end_date.year() == process_date.year()
        {
            first_dialogue_rows.push(dialogue_row);
        }
    }

    tracing::info!("✅ Successfully mapped first dialogue file.");

    // Consolidate second dialogue file
    tracing::info!("❕ Mapping second dialogue file...");

    let mut second_dialogue_rows: Vec<DialogueRow> = Vec::new();

    let file_rows = second_sheet.rows().enumerate().collect::<Vec<_>>();
    let file_rows = file_rows.iter().skip(10).collect::<Vec<_>>();

    let mut current_row = 0;
    let total_rows = file_rows.len();

    for (_, row) in file_rows {
        current_row += 1;

        if current_row == total_rows - 3 {
            break;
        }

        let row = row.iter().map(|cell| cell.to_string()).collect::<Vec<_>>();

        let shift_group = &row[0];

        if shift_group.len() > 0 {
            shift_group_temp = shift_group.to_string();
        }

        let shift = &row[6];

        if shift.len() > 0 {
            shift_temp = shift.to_string();
        }

        let teacher_name = &row[1];

        if teacher_name.len() > 0 {
            teacher_name_temp = teacher_name.to_string();
        }

        let start_date = &row[4];

        if start_date.len() > 0 {
            start_date_temp = start_date.to_string();
        }

        let end_date = &row[5];

        if end_date.len() > 0 {
            end_date_temp = end_date.to_string();
        }

        let dialogue_row = DialogueRow {
            shift_group: shift_group_temp.to_string(),
            shift: shift_temp.to_string(),
            teacher_name: teacher_name_temp.to_string(),
            start_date: start_date_temp.to_string(),
            end_date: end_date_temp.to_string(),
        };

        let dialogue_row_start_date = dialogue_row.start_date.split(" ").collect::<Vec<_>>()[0];
        let dialogue_row_start_day = dialogue_row_start_date.split("/").collect::<Vec<_>>()[1];
        let dialogue_row_start_month = dialogue_row_start_date.split("/").collect::<Vec<_>>()[0];
        let dialogue_row_start_year = dialogue_row_start_date.split("/").collect::<Vec<_>>()[2];

        let dialogue_row_start_date = format!(
            "{}/{}/{}",
            dialogue_row_start_day, dialogue_row_start_month, dialogue_row_start_year
        );

        let dialogue_row_start_date = NaiveDateTime::parse_from_str(
            &format!("{} 00:00:00", dialogue_row_start_date),
            "%d/%m/%Y %H:%M:%S",
        )
        .unwrap();

        let dialogue_row_end_date = dialogue_row.end_date.split(" ").collect::<Vec<_>>()[0];
        let dialogue_row_end_day = dialogue_row_end_date.split("/").collect::<Vec<_>>()[1];
        let dialogue_row_end_month = dialogue_row_end_date.split("/").collect::<Vec<_>>()[0];
        let dialogue_row_end_year = dialogue_row_end_date.split("/").collect::<Vec<_>>()[2];

        let dialogue_row_end_date = format!(
            "{}/{}/{}",
            dialogue_row_end_day, dialogue_row_end_month, dialogue_row_end_year
        );

        let dialogue_row_end_date = NaiveDateTime::parse_from_str(
            &format!("{} 00:00:00", dialogue_row_end_date),
            "%d/%m/%Y %H:%M:%S",
        )
        .unwrap();

        // Check if the dialogue row date is equal to the process date or if the day and year are the same but the month is less than the process date month
        if dialogue_row_start_date.day() == process_date.day()
            && dialogue_row_start_date.month() == process_date.month()
            && dialogue_row_start_date.year() == process_date.year()
            || dialogue_row_end_date.day() == process_date.day()
                && dialogue_row_end_date.month() == process_date.month()
                && dialogue_row_end_date.year() == process_date.year()
        {
            second_dialogue_rows.push(dialogue_row);
        }
    }

    tracing::info!("✅ Successfully mapped second dialogue file.");

    // Consolidate invoicing file
    tracing::info!("❕ Mapping invoicing file...");

    let mut invoicing_rows: Vec<InvoicingRow> = Vec::new();

    for row in invoicing_sheet {
        let teacher_name = row.get(4);
        let eligible = row.get(5);
        let activity_start = row.get(7);
        let activity_end = row.get(8);
        let shift = row.get(9);

        let teacher_name = match teacher_name {
            Some(teacher_name) => teacher_name.to_string(),
            None => String::new(),
        };

        let eligible = match eligible {
            Some(eligible) => {
                if eligible == "Eligible" {
                    true
                } else {
                    false
                }
            }
            None => false,
        };

        let activity_start = match activity_start {
            Some(activity_start) => activity_start.to_string(),
            None => String::new(),
        };

        let activity_end = match activity_end {
            Some(activity_end) => activity_end.to_string(),
            None => String::new(),
        };

        let shift = match shift {
            Some(shift) => shift.to_string(),
            None => String::new(),
        };

        let activity_start_split = activity_start.split(" ").collect::<Vec<_>>();
        let activity_start_date = activity_start_split[0];
        let activity_start_day = activity_start_date.split("/").collect::<Vec<_>>()[1];
        let activity_start_month = activity_start_date.split("/").collect::<Vec<_>>()[0];
        let activity_start_year = activity_start_date.split("/").collect::<Vec<_>>()[2];
        let activity_start_time = activity_start_split[1];
        let activity_start_hour = activity_start_time.split(":").collect::<Vec<_>>()[0];
        let activity_start_minute = activity_start_time.split(":").collect::<Vec<_>>()[1];

        let activity_start_date = format!(
            "{}/{}/{} {}:{}",
            activity_start_day,
            activity_start_month,
            activity_start_year,
            activity_start_hour,
            activity_start_minute
        );

        let activity_start_date =
            NaiveDateTime::parse_from_str(&format!("{}", activity_start_date), "%d/%m/%Y %H:%M")
                .unwrap();

        let activity_end_split = activity_end.split(" ").collect::<Vec<_>>();
        let activity_end_date = activity_end_split[0];
        let activity_end_day = activity_end_date.split("/").collect::<Vec<_>>()[1];
        let activity_end_month = activity_end_date.split("/").collect::<Vec<_>>()[0];
        let activity_end_year = activity_end_date.split("/").collect::<Vec<_>>()[2];
        let activity_end_time = activity_end_split[1];
        let activity_end_hour = activity_end_time.split(":").collect::<Vec<_>>()[0];
        let activity_end_minute = activity_end_time.split(":").collect::<Vec<_>>()[1];

        let activity_end_date = format!(
            "{}/{}/{} {}:{}",
            activity_end_day,
            activity_end_month,
            activity_end_year,
            activity_end_hour,
            activity_end_minute
        );

        let activity_end_date =
            NaiveDateTime::parse_from_str(&format!("{}", activity_end_date), "%d/%m/%Y %H:%M")
                .unwrap();

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
        // See if the invoice row already exists
        let existing_invoice_row = sqlx::query!(
            r#"
                SELECT
                    id
                FROM invoices
                WHERE teacher_name = $1 AND shift = $2 AND activity_start = $3 AND activity_end = $4
            "#,
            invoicing_row.teacher_name,
            invoicing_row.shift,
            invoicing_row.activity_start,
            invoicing_row.activity_end
        )
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
                Some(existing_invoice_row) => {
                    let update_result = sqlx::query!(
                            r#"
                                UPDATE invoices
                                SET
                                    eligible = $1
                                WHERE id = $2
                            "#,
                            invoicing_row.eligible,
                            existing_invoice_row.id
                        )
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
                    let insert_result = sqlx::query!(
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
                            "#,
                            invoicing_row.teacher_name,
                            invoicing_row.eligible,
                            invoicing_row.activity_start,
                            invoicing_row.activity_end,
                            invoicing_row.shift
                        )
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

    // Split the first and second dialogue rows by their shift group
    let mut first_dialogue_rows_split: HashMap<String, Vec<DialogueRow>> = HashMap::new();

    for row in first_dialogue_rows {
        let shift_group = row.shift_group.clone();

        if first_dialogue_rows_split.contains_key(&shift_group) {
            let shift_group_rows = first_dialogue_rows_split.get_mut(&shift_group).unwrap();

            shift_group_rows.push(row);
        } else {
            let mut shift_group_rows: Vec<DialogueRow> = Vec::new();

            shift_group_rows.push(row);

            first_dialogue_rows_split.insert(shift_group, shift_group_rows);
        }
    }

    let mut second_dialogue_rows_split: HashMap<String, Vec<DialogueRow>> = HashMap::new();

    for row in second_dialogue_rows {
        let shift_group = row.shift_group.clone();

        if second_dialogue_rows_split.contains_key(&shift_group) {
            let shift_group_rows = second_dialogue_rows_split.get_mut(&shift_group).unwrap();

            shift_group_rows.push(row);
        } else {
            let mut shift_group_rows: Vec<DialogueRow> = Vec::new();

            shift_group_rows.push(row);

            second_dialogue_rows_split.insert(shift_group, shift_group_rows);
        }
    }

    // Consolidate the first and second dialogue rows by their shift group
    let mut consolidated_rows: Vec<DialogueConsolidatedRow> = Vec::new();

    for (shift_group, first_dialogue_rows) in first_dialogue_rows_split {
        let second_dialogue_rows = second_dialogue_rows_split.get(&shift_group);

        match second_dialogue_rows {
            Some(second_dialogue_rows) => {
                let first_shifts = first_dialogue_rows
                    .iter()
                    .map(|row| row.shift.clone())
                    .collect::<Vec<String>>();
                let second_shifts = second_dialogue_rows
                    .iter()
                    .map(|row| row.shift.clone())
                    .collect::<Vec<String>>();

                let lost_shifts = first_shifts
                    .iter()
                    .filter(|shift| !second_shifts.contains(shift))
                    .collect::<Vec<&String>>();
                let new_shifts = second_shifts
                    .iter()
                    .filter(|shift| !first_shifts.contains(shift))
                    .collect::<Vec<&String>>();

                let previous_shift_teachers = first_dialogue_rows
                    .iter()
                    .map(|row| (row.shift.clone(), row.teacher_name.clone()))
                    .collect::<HashMap<String, String>>();

                let picked_up_shifts = second_dialogue_rows
                    .iter()
                    .filter(|row| {
                        let previous_shift_teacher = previous_shift_teachers.get(&row.shift);

                        match previous_shift_teacher {
                            Some(previous_shift_teacher) => {
                                if previous_shift_teacher != &row.teacher_name {
                                    true
                                } else {
                                    false
                                }
                            }
                            None => false,
                        }
                    })
                    .map(|row| row.shift.clone())
                    .collect::<Vec<String>>();

                let lost_but_picked_up_shifts = first_dialogue_rows
                    .iter()
                    .filter(|row| {
                        let second_shift = second_shifts.contains(&row.shift);

                        if second_shift {
                            let second_dialogue_row = second_dialogue_rows
                                .iter()
                                .find(|second_dialogue_row| second_dialogue_row.shift == row.shift);

                            match second_dialogue_row {
                                Some(second_dialogue_row) => {
                                    if second_dialogue_row.teacher_name != row.teacher_name {
                                        true
                                    } else {
                                        false
                                    }
                                }
                                None => false,
                            }
                        } else {
                            false
                        }
                    })
                    .map(|row| row.shift.clone())
                    .collect::<Vec<String>>();

                for current_shift in second_dialogue_rows
                    .iter()
                    .filter(|row| new_shifts.contains(&&row.shift))
                    .collect::<Vec<&DialogueRow>>()
                {
                    let consolidated_row = DialogueConsolidatedRow {
                        shift_group: shift_group.clone(),
                        shift: current_shift.shift.clone(),
                        shift_type: "Pickup".to_string(),
                        teacher_name: current_shift.teacher_name.clone(),
                        start_date: current_shift.start_date.clone(),
                        end_date: current_shift.end_date.clone(),
                    };

                    consolidated_rows.push(consolidated_row);
                }

                for current_shift in second_dialogue_rows
                    .iter()
                    .filter(|row| picked_up_shifts.contains(&&row.shift))
                    .collect::<Vec<&DialogueRow>>()
                {
                    let consolidated_row = DialogueConsolidatedRow {
                        shift_group: shift_group.clone(),
                        shift: current_shift.shift.clone(),
                        shift_type: "Internal Pickup".to_string(),
                        teacher_name: current_shift.teacher_name.clone(),
                        start_date: current_shift.start_date.clone(),
                        end_date: current_shift.end_date.clone(),
                    };

                    consolidated_rows.push(consolidated_row);
                }

                for current_shift in first_dialogue_rows
                    .iter()
                    .filter(|row| lost_shifts.contains(&&row.shift))
                    .collect::<Vec<&DialogueRow>>()
                {
                    let consolidated_row = DialogueConsolidatedRow {
                        shift_group: shift_group.clone(),
                        shift: current_shift.shift.clone(),
                        shift_type: "Dropped".to_string(),
                        teacher_name: current_shift.teacher_name.clone(),
                        start_date: current_shift.start_date.clone(),
                        end_date: current_shift.end_date.clone(),
                    };

                    consolidated_rows.push(consolidated_row);
                }

                for current_shift in first_dialogue_rows
                    .iter()
                    .filter(|row| lost_but_picked_up_shifts.contains(&&row.shift))
                    .collect::<Vec<&DialogueRow>>()
                {
                    let consolidated_row = DialogueConsolidatedRow {
                        shift_group: shift_group.clone(),
                        shift: current_shift.shift.clone(),
                        shift_type: "Dropped & Picked Up".to_string(),
                        teacher_name: current_shift.teacher_name.clone(),
                        start_date: current_shift.start_date.clone(),
                        end_date: current_shift.end_date.clone(),
                    };

                    consolidated_rows.push(consolidated_row);
                }

                for current_shift in second_dialogue_rows
                    .iter()
                    .filter(|row| {
                        !picked_up_shifts.contains(&&row.shift)
                            && !new_shifts.contains(&&row.shift)
                            && !lost_shifts.contains(&&row.shift)
                            && !lost_but_picked_up_shifts.contains(&&row.shift)
                    })
                    .collect::<Vec<&DialogueRow>>()
                {
                    let consolidated_row = DialogueConsolidatedRow {
                        shift_group: shift_group.clone(),
                        shift: current_shift.shift.clone(),
                        shift_type: "-".to_string(),
                        teacher_name: current_shift.teacher_name.clone(),
                        start_date: current_shift.start_date.clone(),
                        end_date: current_shift.end_date.clone(),
                    };

                    consolidated_rows.push(consolidated_row);
                }
            }
            None => {}
        }
    }

    tracing::info!("❕ Consolidated rows: {}", consolidated_rows.len());

    tracing::info!("✅ Successfully consolidated dialogues.");

    consolidated_rows.sort_by(|a, b| {
        let a_date = a.start_date.split(" ").collect::<Vec<_>>()[0];
        let a_day = a_date.split("/").collect::<Vec<_>>()[1];
        let a_month = a_date.split("/").collect::<Vec<_>>()[0];
        let a_year = a_date.split("/").collect::<Vec<_>>()[2];
        let a_time = a.start_date.split(" ").collect::<Vec<_>>()[1];
        let a_hour = a_time.split(":").collect::<Vec<_>>()[0];
        let a_minute = a_time.split(":").collect::<Vec<_>>()[1];
        let a_time_period = a.start_date.split(" ").collect::<Vec<_>>()[2];

        let b_date = b.start_date.split(" ").collect::<Vec<_>>()[0];
        let b_day = b_date.split("/").collect::<Vec<_>>()[1];
        let b_month = b_date.split("/").collect::<Vec<_>>()[0];
        let b_year = b_date.split("/").collect::<Vec<_>>()[2];
        let b_time = b.start_date.split(" ").collect::<Vec<_>>()[1];
        let b_hour = b_time.split(":").collect::<Vec<_>>()[0];
        let b_minute = b_time.split(":").collect::<Vec<_>>()[1];
        let b_time_period = b.start_date.split(" ").collect::<Vec<_>>()[2];

        let a_date = format!(
            "{}/{}/{} {}:{} {}",
            a_day, a_month, a_year, a_hour, a_minute, a_time_period
        );

        let b_date = format!(
            "{}/{}/{} {}:{} {}",
            b_day, b_month, b_year, b_hour, b_minute, b_time_period
        );

        let a_date = NaiveDateTime::parse_from_str(&a_date, "%d/%m/%Y %I:%M %p").unwrap();

        let b_date = NaiveDateTime::parse_from_str(&b_date, "%d/%m/%Y %I:%M %p").unwrap();

        a_date.cmp(&b_date)
    });

    consolidated_rows.sort_by(|a, b| a.teacher_name.cmp(&b.teacher_name));
    consolidated_rows.sort_by(|a, b| a.shift_group.cmp(&b.shift_group));

    // Use the invoicing rows to determine which consolidated rows are eligible
    let consolidated_rows: Vec<&DialogueConsolidatedRow> = consolidated_rows
        .iter()
        .filter(|row| {
            let row_start_date = row.start_date.split(" ").collect::<Vec<_>>()[0];
            let row_start_day = row_start_date.split("/").collect::<Vec<_>>()[1];
            let row_start_month = row_start_date.split("/").collect::<Vec<_>>()[0];
            let row_start_year = row_start_date.split("/").collect::<Vec<_>>()[2];

            let row_start_date =
                format!("{}/{}/{}", row_start_day, row_start_month, row_start_year);

            let row_start_date = NaiveDateTime::parse_from_str(
                &format!("{} 00:00:00", row_start_date),
                "%d/%m/%Y %H:%M:%S",
            )
            .unwrap();

            row_start_date.day() == process_date.day()
                && row_start_date.month() == process_date.month()
                && row_start_date.year() == process_date.year()
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
        let start_date = consolidated_row.start_date.clone();
        let end_date = consolidated_row.end_date.clone();

        let start_date_split = start_date.split(" ").collect::<Vec<_>>();
        let end_date_split = end_date.split(" ").collect::<Vec<_>>();

        let start_time_period = start_date_split[2];
        let end_time_period = end_date_split[2];

        let start_date = start_date_split[0];
        let end_date = end_date_split[0];

        let start_time = start_date_split[1];
        let end_time = end_date_split[1];

        let start_date_split = start_date.split("/").collect::<Vec<_>>();
        let end_date_split = end_date.split("/").collect::<Vec<_>>();

        let start_day = start_date_split[1];
        let start_month = start_date_split[0];
        let start_year = start_date_split[2];

        let end_day = end_date_split[1];
        let end_month = end_date_split[0];
        let end_year = end_date_split[2];

        let start_time_split = start_time.split(":").collect::<Vec<_>>();
        let end_time_split = end_time.split(":").collect::<Vec<_>>();

        let start_hour = start_time_split[0];
        let start_minute = start_time_split[1];

        let end_hour = end_time_split[0];
        let end_minute = end_time_split[1];

        let start_date = format!(
            "{}/{}/{} {}:{} {}",
            start_day, start_month, start_year, start_hour, start_minute, start_time_period
        );

        let end_date = format!(
            "{}/{}/{} {}:{} {}",
            end_day, end_month, end_year, end_hour, end_minute, end_time_period
        );

        let start_date = NaiveDateTime::parse_from_str(&start_date, "%d/%m/%Y %I:%M %p").unwrap();
        let end_date = NaiveDateTime::parse_from_str(&end_date, "%d/%m/%Y %I:%M %p").unwrap();

        let teacher_found = sqlx::query!("SELECT * FROM teachers WHERE name = $1", teacher_name)
            .fetch_optional(&app_state.db)
            .await
            .map_err(|_| {
                tracing::error!("🔥 Failed to fetch teacher from the database.");

                Error::msg("Failed to fetch teacher from the database.")
            })?;

        let teacher_found = match teacher_found {
            Some(teacher) => {
                skipped_teachers += 1;

                teacher.id
            }
            None => {
                let insert_teacher_result = sqlx::query!(
                    "INSERT INTO teachers (name) VALUES ($1) RETURNING id",
                    teacher_name
                )
                .fetch_one(&app_state.db)
                .await
                .map_err(|_| {
                    tracing::error!("🔥 Failed to insert teacher into the database.");

                    Error::msg("Failed to insert teacher into the database.")
                })?;

                new_teachers += 1;

                insert_teacher_result.id
            }
        };

        let schedule_found = sqlx::query!(
            "SELECT * FROM schedules WHERE teacher_id = $1 AND start_date = $2 AND end_date = $3 AND shift = $4 AND shift_type = $5 AND shift_group = $6",
            teacher_found,
            start_date,
            end_date,
            shift,
            shift_type,
            shift_group
        )
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
                sqlx::query!(
                    "INSERT INTO schedules (teacher_id, start_date, end_date, shift, shift_type, shift_group) VALUES ($1, $2, $3, $4, $5, $6)",
                    teacher_found,
                    start_date,
                    end_date,
                    shift,
                    shift_type,
                    shift_group,
                )
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
