use std::{
    collections::{HashMap, HashSet},
    fs,
    io::Write,
};

use anyhow::Error;
use axum::{
    extract::{Multipart, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use calamine::{open_workbook, Reader, Xlsx};
use chrono::{Datelike, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::{Africa::Johannesburg, America::New_York, Tz};
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
    eligible: Option<usize>,
    activity_start: usize,
    activity_end: usize,
    shift: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DialogueMatchKey {
    shift: String,
    start_date: String,
    end_date: String,
}

fn source_timezone() -> Tz {
    std::env::var("DIALOGUE_SOURCE_TIMEZONE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(New_York)
}

fn app_timezone() -> Tz {
    std::env::var("APP_TIMEZONE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(Johannesburg)
}

fn localize_in_timezone(naive: NaiveDateTime, timezone: Tz) -> NaiveDateTime {
    match timezone.from_local_datetime(&naive) {
        chrono::LocalResult::Single(zoned) => zoned.naive_local(),
        chrono::LocalResult::Ambiguous(earliest, _) => earliest.naive_local(),
        chrono::LocalResult::None => {
            tracing::warn!(
                "Datetime {:?} is invalid in {}; using naive value unchanged",
                naive,
                timezone
            );
            naive
        }
    }
}

/// CSV exports from the US company are wall-clock times in [source_timezone].
/// All comparisons and DB storage use [app_timezone] (default Africa/Johannesburg).
fn convert_source_local_to_app_timezone(naive: NaiveDateTime) -> NaiveDateTime {
    let source = source_timezone();
    let app = app_timezone();
    let source_zoned = source.from_local_datetime(&naive);
    let source_dt = match source_zoned {
        chrono::LocalResult::Single(dt) => dt,
        chrono::LocalResult::Ambiguous(earliest, _) => earliest,
        chrono::LocalResult::None => {
            tracing::warn!(
                "Datetime {:?} is invalid in {}; treating as app-local",
                naive,
                source
            );
            return localize_in_timezone(naive, app);
        }
    };

    source_dt.with_timezone(&app).naive_local()
}

fn parse_process_date(process_date: &str) -> Result<NaiveDateTime, Error> {
    let date = NaiveDate::parse_from_str(process_date, "%Y-%m-%d")?;
    let midnight = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| anyhow::anyhow!("Invalid process date: {}", process_date))?;

    Ok(localize_in_timezone(midnight, app_timezone()))
}

const DIALOGUE_DATETIME_FORMATS: &[&str] = &[
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%d/%m/%Y %I:%M %p",
    "%d/%m/%Y %I:%M:%S %p",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
];

fn push_unique_datetime(candidates: &mut Vec<NaiveDateTime>, parsed: NaiveDateTime) {
    let parsed = convert_source_local_to_app_timezone(parsed);

    if !candidates.iter().any(|existing| *existing == parsed) {
        candidates.push(parsed);
    }
}

fn dialogue_datetime_candidates(value: &str) -> Vec<NaiveDateTime> {
    let value = value.trim().trim_matches('"');
    let mut candidates = Vec::new();

    for format in DIALOGUE_DATETIME_FORMATS {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(value, format) {
            push_unique_datetime(&mut candidates, parsed);
        }
    }

    if let Some(parsed) = parse_dialogue_datetime_flexible(value, false) {
        push_unique_datetime(&mut candidates, parsed);
    }

    if let Some(parsed) = parse_dialogue_datetime_flexible(value, true) {
        push_unique_datetime(&mut candidates, parsed);
    }

    candidates
}

fn parse_dialogue_datetime(value: &str) -> Result<NaiveDateTime, Error> {
    dialogue_datetime_candidates(value)
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("Unsupported dialogue datetime format: {}", value.trim()))
}

fn select_dialogue_start_end(
    start: &str,
    finish: &str,
    process_date: NaiveDateTime,
) -> Option<(NaiveDateTime, NaiveDateTime)> {
    let starts = dialogue_datetime_candidates(start);
    let ends = dialogue_datetime_candidates(finish);

    if starts.is_empty() || ends.is_empty() {
        return None;
    }

    let matching_starts = starts
        .iter()
        .filter(|start_date| is_same_day(**start_date, process_date))
        .copied()
        .collect::<Vec<_>>();
    let matching_ends = ends
        .iter()
        .filter(|end_date| is_same_day(**end_date, process_date))
        .copied()
        .collect::<Vec<_>>();

    if let Some(start_date) = matching_starts.first().copied() {
        let end_date = ends
            .iter()
            .copied()
            .filter(|end_date| *end_date >= start_date)
            .min_by_key(|end_date| *end_date - start_date)
            .or_else(|| ends.first().copied())?;

        return Some((start_date, end_date));
    }

    if let Some(end_date) = matching_ends.first().copied() {
        let start_date = starts
            .iter()
            .copied()
            .filter(|start_date| *start_date <= end_date)
            .max()
            .or_else(|| starts.first().copied())?;

        return Some((start_date, end_date));
    }

    None
}

fn parse_dialogue_datetime_flexible(value: &str, swap_month_and_day: bool) -> Option<NaiveDateTime> {
    let parts: Vec<&str> = value.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }

    let date_parts: Vec<&str> = parts[0].split('/').collect();
    if date_parts.len() != 3 {
        return None;
    }

    let first: u32 = date_parts[0].parse().ok()?;
    let second: u32 = date_parts[1].parse().ok()?;
    let year: i32 = date_parts[2].parse().ok()?;

    let (month, day) = if swap_month_and_day {
        (second, first)
    } else {
        (first, second)
    };

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    let (hour, minute, second) = if parts.len() >= 3 && parts[2].eq_ignore_ascii_case("AM")
        || parts[2].eq_ignore_ascii_case("PM")
    {
        let time_parts: Vec<&str> = parts[1].split(':').collect();
        if time_parts.len() < 2 {
            return None;
        }

        let mut hour: u32 = time_parts[0].parse().ok()?;
        let minute: u32 = time_parts[1].parse().ok()?;
        let second = time_parts
            .get(2)
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);

        if parts[2].eq_ignore_ascii_case("PM") && hour < 12 {
            hour += 12;
        } else if parts[2].eq_ignore_ascii_case("AM") && hour == 12 {
            hour = 0;
        }

        (hour, minute, second)
    } else {
        let time_parts: Vec<&str> = parts[1].split(':').collect();
        if time_parts.len() < 2 {
            return None;
        }

        (
            time_parts[0].parse().ok()?,
            time_parts[1].parse().ok()?,
            time_parts
                .get(2)
                .and_then(|value| value.parse().ok())
                .unwrap_or(0),
        )
    };

    NaiveDateTime::parse_from_str(
        &format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            year, month, day, hour, minute, second
        ),
        "%Y-%m-%d %H:%M:%S",
    )
    .ok()
}

fn format_dialogue_datetime(parsed: NaiveDateTime) -> String {
    parsed.format("%Y-%m-%d %H:%M:%S").to_string()
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
            return Ok(convert_source_local_to_app_timezone(parsed));
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

fn decode_bytes_to_string(bytes: &[u8]) -> String {
    if bytes.starts_with(&[0xFF, 0xFE]) {
        let utf16: Vec<u16> = bytes[2..]
            .chunks_exact(2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]))
            .collect();
        return String::from_utf16_lossy(&utf16);
    }
    if bytes.starts_with(&[0xFE, 0xFF]) {
        let utf16: Vec<u16> = bytes[2..]
            .chunks_exact(2)
            .map(|b| u16::from_be_bytes([b[0], b[1]]))
            .collect();
        return String::from_utf16_lossy(&utf16);
    }
    String::from_utf8_lossy(bytes).into_owned()
}

fn detect_csv_delimiter(contents: &str) -> u8 {
    let sample_line = contents
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or_default();

    let candidates = [b',', b';', b'\t', b'|'];

    candidates
        .into_iter()
        .max_by_key(|candidate| sample_line.matches(*candidate as char).count())
        .unwrap_or(b',')
}

fn normalize_csv_header(header: &str) -> String {
    header
        .trim()
        .trim_start_matches('\u{feff}')
        .to_ascii_lowercase()
}

fn preprocess_malformed_csv(contents: &str) -> String {
    // Excel dialogue exports wrap the entire row in quotes and escape inner quotes
    // as doubled quotes (e.g. "Start,""Finish"",""Shift...""). Stripping all quotes
    // yields a comma-separated row the CSV reader can split reliably.
    contents.replace('"', "")
}

fn is_excel_dialogue_export(contents: &str) -> bool {
    contents.contains("\"\"Finish\"\"")
        || contents.contains("\"\"Start\"\"")
        || contents.contains(",\"\"")
}

fn dialogue_csv_splits_cleanly(contents: &str, delimiter: u8) -> bool {
    let mut reader = ReaderBuilder::new()
        .trim(csv::Trim::All)
        .delimiter(delimiter)
        .flexible(true)
        .from_reader(contents.as_bytes());

    let headers = match reader.headers() {
        Ok(headers) => headers,
        Err(_) => return false,
    };

    if headers.len() < 5 || build_dialogue_csv_columns(headers).is_err() {
        return false;
    }

    let expected_columns = headers.len();

    match reader.records().next() {
        Some(Ok(record)) => record.len() >= expected_columns.saturating_sub(1),
        Some(Err(_)) => false,
        None => true,
    }
}

fn prepare_dialogue_csv_for_parsing(contents: &str) -> String {
    if is_excel_dialogue_export(contents) {
        return preprocess_malformed_csv(contents);
    }

    let delimiter = detect_csv_delimiter(contents);

    if dialogue_csv_splits_cleanly(contents, delimiter) {
        return contents.to_string();
    }

    let preprocessed = preprocess_malformed_csv(contents);

    if dialogue_csv_splits_cleanly(&preprocessed, delimiter) {
        preprocessed
    } else {
        contents.to_string()
    }
}

fn normalize_identifier(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn normalize_shift_identifier(value: &str) -> String {
    let value = normalize_identifier(value);
    let numeric_candidate = value.replace(',', "");

    if let Ok(integer) = numeric_candidate.parse::<i64>() {
        return integer.to_string();
    }

    if let Ok(float) = numeric_candidate.parse::<f64>() {
        if float.fract().abs() < f64::EPSILON {
            return format!("{float:.0}");
        }

        let mut formatted = float.to_string();

        if formatted.contains('.') {
            while formatted.ends_with('0') {
                formatted.pop();
            }

            if formatted.ends_with('.') {
                formatted.pop();
            }
        }

        return formatted;
    }

    value.to_ascii_lowercase()
}

fn canonicalize_dialogue_datetime(value: &str) -> String {
    match parse_dialogue_datetime(value) {
        Ok(parsed) => format_dialogue_datetime(parsed),
        Err(_) => normalize_identifier(value).to_ascii_lowercase(),
    }
}

fn build_dialogue_match_key(row: &DialogueRow) -> DialogueMatchKey {
    DialogueMatchKey {
        shift: normalize_shift_identifier(&row.shift),
        start_date: canonicalize_dialogue_datetime(&row.start_date),
        end_date: canonicalize_dialogue_datetime(&row.end_date),
    }
}

fn consolidate_dialogue_rows(
    first_dialogue_rows: &[DialogueRow],
    second_dialogue_rows: &[DialogueRow],
) -> Vec<DialogueConsolidatedRow> {
    let first_keys = first_dialogue_rows
        .iter()
        .map(build_dialogue_match_key)
        .collect::<Vec<_>>();
    let second_keys = second_dialogue_rows
        .iter()
        .map(build_dialogue_match_key)
        .collect::<Vec<_>>();

    let first_key_set = first_keys.iter().cloned().collect::<HashSet<_>>();
    let second_key_set = second_keys.iter().cloned().collect::<HashSet<_>>();

    let dropped_keys = first_key_set
        .difference(&second_key_set)
        .cloned()
        .collect::<HashSet<_>>();
    let pickup_keys = second_key_set
        .difference(&first_key_set)
        .cloned()
        .collect::<HashSet<_>>();

    let previous_shift_assignments = first_dialogue_rows
        .iter()
        .map(|row| {
            (
                build_dialogue_match_key(row),
                (
                    normalize_identifier(&row.teacher_name).to_ascii_lowercase(),
                    normalize_identifier(&row.shift_group).to_ascii_lowercase(),
                ),
            )
        })
        .collect::<HashMap<DialogueMatchKey, (String, String)>>();

    let mut internal_pick_up_keys = HashSet::new();
    let mut dropped_and_picked_up_keys = HashSet::new();

    for second_dialogue_row in second_dialogue_rows {
        let match_key = build_dialogue_match_key(second_dialogue_row);

        if pickup_keys.contains(&match_key) {
            continue;
        }

        if let Some((previous_teacher, previous_shift_group)) =
            previous_shift_assignments.get(&match_key)
        {
            let current_teacher =
                normalize_identifier(&second_dialogue_row.teacher_name).to_ascii_lowercase();
            let current_shift_group =
                normalize_identifier(&second_dialogue_row.shift_group).to_ascii_lowercase();

            if previous_teacher != &current_teacher && previous_shift_group == &current_shift_group
            {
                internal_pick_up_keys.insert(match_key.clone());
            }

            if previous_teacher != &current_teacher && previous_shift_group != &current_shift_group
            {
                dropped_and_picked_up_keys.insert(match_key);
            }
        }
    }

    let mut consolidated_rows = Vec::new();

    for current_dialogue_row in first_dialogue_rows {
        let match_key = build_dialogue_match_key(current_dialogue_row);

        if dropped_keys.contains(&match_key) {
            consolidated_rows.push(DialogueConsolidatedRow {
                shift_group: current_dialogue_row.shift_group.clone(),
                shift: current_dialogue_row.shift.clone(),
                shift_type: "Dropped".to_string(),
                teacher_name: current_dialogue_row.teacher_name.clone(),
                start_date: current_dialogue_row.start_date.clone(),
                end_date: current_dialogue_row.end_date.clone(),
            });
        }
    }

    for current_dialogue_row in second_dialogue_rows {
        let match_key = build_dialogue_match_key(current_dialogue_row);

        let shift_type = if pickup_keys.contains(&match_key) {
            "Pickup"
        } else if internal_pick_up_keys.contains(&match_key) {
            "Internal Pickup"
        } else if dropped_and_picked_up_keys.contains(&match_key) {
            "Dropped & Picked Up"
        } else {
            "-"
        };

        consolidated_rows.push(DialogueConsolidatedRow {
            shift_group: current_dialogue_row.shift_group.clone(),
            shift: current_dialogue_row.shift.clone(),
            shift_type: shift_type.to_string(),
            teacher_name: current_dialogue_row.teacher_name.clone(),
            start_date: current_dialogue_row.start_date.clone(),
            end_date: current_dialogue_row.end_date.clone(),
        });
    }

    consolidated_rows
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
    let teacher_name = find_header_index(
        headers,
        &["Resource: Resource Name", "Resource Name", "Teacher Name"],
    )
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
    let teacher_name = find_header_index(
        headers,
        &[
            "Resource: Resource Name",
            "Resource Name",
            "Teacher_Name",
            "Teacher Name",
        ],
    )
    .ok_or_else(|| {
        let cols: Vec<&str> = headers.iter().collect();
        anyhow::anyhow!(
            "Invoicing CSV is missing a Teacher Name column. Found columns: {:?}",
            cols
        )
    })?;
    let eligible = find_header_index(headers, &["Eligible_Status", "Eligible Status", "Eligible"]);
    let activity_start = find_header_index(
        headers,
        &[
            "Activity_Start_Time",
            "Activity Start Time",
            "Activity Start",
            "Start",
        ],
    )
    .ok_or_else(|| anyhow::anyhow!("Invoicing CSV is missing an Activity Start column"))?;
    let activity_end = find_header_index(
        headers,
        &[
            "Activity_End_Time",
            "Activity End Time",
            "Activity End",
            "Finish",
        ],
    )
    .ok_or_else(|| anyhow::anyhow!("Invoicing CSV is missing an Activity End column"))?;

    let shift = [
        "shift_name_tsm",
        "Shift_Name",
        "Shift Name",
        "Shift: Shift Number",
        "Shift Number",
        "Shift",
    ]
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
    let file_contents = decode_bytes_to_string(&file_contents);
    let file_contents = prepare_dialogue_csv_for_parsing(&file_contents);
    let delimiter = detect_csv_delimiter(&file_contents);

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
    let mut skipped_missing_columns = 0usize;
    let mut skipped_invalid_datetime = 0usize;
    let mut skipped_outside_process_date = 0usize;

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
            skipped_missing_columns += 1;
            if skipped_missing_columns <= 3 {
                tracing::warn!(
                    "Skipping dialogue CSV row {} in {} due to missing required columns",
                    index + 2,
                    file_path
                );
            }
            continue;
        }

        let Some((start_date, end_date)) = select_dialogue_start_end(start, finish, process_date)
        else {
            if dialogue_datetime_candidates(start).is_empty()
                || dialogue_datetime_candidates(finish).is_empty()
            {
                skipped_invalid_datetime += 1;
                if skipped_invalid_datetime <= 3 {
                    tracing::warn!(
                        "Skipping dialogue CSV row {} in {} due to invalid start/finish datetimes: start={:?}, finish={:?}",
                        index + 2,
                        file_path,
                        start,
                        finish
                    );
                }
            } else {
                skipped_outside_process_date += 1;
            }
            continue;
        };

        rows.push(DialogueRow {
            shift_group: shift_group.to_string(),
            shift: shift.to_string(),
            teacher_name: teacher_name.to_string(),
            start_date: format_dialogue_datetime(start_date),
            end_date: format_dialogue_datetime(end_date),
        });
    }

    if skipped_missing_columns > 3 {
        tracing::warn!(
            "Skipped {} additional dialogue rows in {} due to missing required columns",
            skipped_missing_columns - 3,
            file_path
        );
    }

    if skipped_invalid_datetime > 3 {
        tracing::warn!(
            "Skipped {} additional dialogue rows in {} due to invalid datetimes",
            skipped_invalid_datetime - 3,
            file_path
        );
    }

    if skipped_outside_process_date > 0 {
        tracing::info!(
            "Skipped {} dialogue rows in {} that do not fall on process date {}",
            skipped_outside_process_date,
            file_path,
            process_date.format("%Y-%m-%d")
        );
    }

    tracing::info!(
        "📄 Loaded {} dialogue rows from {}",
        rows.len(),
        file_path
    );

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
                    start_date: format_dialogue_datetime(start_date),
                    end_date: format_dialogue_datetime(end_date),
                });
            }
        }
    }

    tracing::info!(
        "📄 Loaded {} dialogue rows from {}",
        rows.len(),
        file_path
    );

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
        let file_name =
            if !extension.is_empty() && !name_lower.ends_with(&format!(".{}", extension)) {
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

    tracing::info!(
        "🕐 Consolidating with source timezone {} → app timezone {}",
        source_timezone(),
        app_timezone()
    );

    tracing::info!("✅ Successfully opened all dialogue files.");

    let invoicing_file_contents = fs::read(&invoicing_file_path)?;
    let invoicing_file_contents = decode_bytes_to_string(&invoicing_file_contents);

    // Preprocess to remove all quotes
    let invoicing_file_contents = preprocess_malformed_csv(&invoicing_file_contents);

    let invoicing_delimiter = detect_csv_delimiter(&invoicing_file_contents);

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
        let eligible = invoicing_columns
            .eligible
            .and_then(|idx| row.get(idx))
            .map(parse_eligible_status)
            .unwrap_or(true);
        let activity_start = row
            .get(invoicing_columns.activity_start)
            .unwrap_or("")
            .trim();
        let activity_end = row.get(invoicing_columns.activity_end).unwrap_or("").trim();
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
            "#,
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

    let mut consolidated_rows =
        consolidate_dialogue_rows(&first_dialogue_rows, &second_dialogue_rows);

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
        let start_date =
            parse_dialogue_datetime(&consolidated_row.start_date).map_err(|error| {
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
                    "INSERT INTO teachers (name) VALUES ($1) RETURNING id",
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

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, Timelike};

    use super::{
        build_dialogue_csv_columns, consolidate_dialogue_rows, load_dialogue_rows_from_csv,
        normalize_shift_identifier, parse_dialogue_datetime, parse_process_date,
        preprocess_malformed_csv, DialogueRow,
    };
    use csv::ReaderBuilder;

    fn make_row(
        shift_group: &str,
        shift: &str,
        teacher_name: &str,
        start_date: &str,
        end_date: &str,
    ) -> DialogueRow {
        DialogueRow {
            shift_group: shift_group.to_string(),
            shift: shift.to_string(),
            teacher_name: teacher_name.to_string(),
            start_date: start_date.to_string(),
            end_date: end_date.to_string(),
        }
    }

    #[test]
    fn normalizes_numeric_shift_identifiers_from_csv_and_xlsx() {
        assert_eq!(normalize_shift_identifier("12345"), "12345");
        assert_eq!(normalize_shift_identifier("12345.0"), "12345");
        assert_eq!(normalize_shift_identifier("12,345.000"), "12345");
    }

    #[test]
    fn does_not_mark_rows_as_dropped_when_shift_only_differs_by_numeric_format() {
        let first_dialogue_rows = vec![make_row(
            "Alpha",
            "12345",
            "Teacher One",
            "2026-04-20 08:00:00",
            "2026-04-20 09:00:00",
        )];
        let second_dialogue_rows = vec![make_row(
            "Alpha",
            "12345.0",
            "Teacher One",
            "2026-04-20 08:00:00",
            "2026-04-20 09:00:00",
        )];

        let consolidated_rows =
            consolidate_dialogue_rows(&first_dialogue_rows, &second_dialogue_rows);

        assert_eq!(consolidated_rows.len(), 1);
        assert_eq!(consolidated_rows[0].shift_type, "-");
        assert_eq!(consolidated_rows[0].shift, "12345.0");
    }

    #[test]
    fn parses_dialogue_datetime_with_single_digit_month_day_and_hour() {
        let parsed = parse_dialogue_datetime("5/2/2026 9:00 AM").expect("datetime");
        assert_eq!(parsed.date(), NaiveDate::from_ymd_opt(2026, 5, 2).unwrap());
        // 09:00 US Eastern (EDT) -> 15:00 Africa/Johannesburg
        assert_eq!(parsed.hour(), 15);
        assert_eq!(parsed.minute(), 0);
    }

    #[test]
    fn us_late_evening_shift_falls_on_next_day_in_johannesburg() {
        let parsed =
            parse_dialogue_datetime("5/1/2026 11:00 PM").expect("datetime");
        assert_eq!(parsed.date(), NaiveDate::from_ymd_opt(2026, 5, 2).unwrap());
        assert_eq!(parsed.hour(), 5);

        let dir = std::env::temp_dir().join(format!(
            "sergio-ar-dialogue-tz-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("dialogue-2.csv");
        std::fs::write(
            &file_path,
            "Start,Finish,Shift: Shift Number,Resource: Shift Group,Resource: Resource Name,Day of Week\n\
             5/1/2026 11:00 PM,5/2/2026 1:00 AM,T-1,Group,Teacher,Saturday\n",
        )
        .unwrap();

        let rows_may_1 = load_dialogue_rows_from_csv(
            file_path.to_str().unwrap(),
            parse_process_date("2026-05-01").unwrap(),
        )
        .expect("rows");
        let rows_may_2 = load_dialogue_rows_from_csv(
            file_path.to_str().unwrap(),
            parse_process_date("2026-05-02").unwrap(),
        )
        .expect("rows");

        std::fs::remove_dir_all(&dir).ok();

        assert_eq!(rows_may_1.len(), 0);
        assert_eq!(rows_may_2.len(), 1);
    }

    #[test]
    fn parses_dialogue_export_csv_headers_and_row() {
        let contents = r#""Start,""Finish"",""Shift: Shift Number"",""Resource: Shift Group"",""Resource: Resource Name"",""Day of Week"""#
            .to_string()
            + "\n"
            + r#""5/2/2026 9:00 AM,""5/2/2026 11:00 AM"",""T-5412533"",""JEN 4 - PM"",""Babalwa Magongo"",""Saturday"""#;

        let preprocessed = preprocess_malformed_csv(&contents);
        let mut reader = ReaderBuilder::new()
            .trim(csv::Trim::All)
            .delimiter(b',')
            .flexible(true)
            .from_reader(preprocessed.as_bytes());

        let headers = reader.headers().unwrap().clone();
        let columns = build_dialogue_csv_columns(&headers).expect("columns");

        let record = reader.records().next().unwrap().unwrap();
        assert_eq!(record.get(columns.start).unwrap(), "5/2/2026 9:00 AM");
        assert_eq!(record.get(columns.finish).unwrap(), "5/2/2026 11:00 AM");
        assert_eq!(record.get(columns.shift).unwrap(), "T-5412533");
        assert_eq!(
            record.get(columns.shift_group).unwrap(),
            "JEN 4 - PM"
        );
        assert_eq!(
            record.get(columns.teacher_name).unwrap(),
            "Babalwa Magongo"
        );
    }

    #[test]
    fn loads_uk_formatted_dates_on_us_process_date() {
        let dir = std::env::temp_dir().join(format!(
            "sergio-ar-dialogue-uk-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("dialogue-2.csv");
        std::fs::write(
            &file_path,
            "Start,Finish,Shift: Shift Number,Resource: Shift Group,Resource: Resource Name,Day of Week\n\
             1/5/2026 9:00 AM,1/5/2026 11:00 AM,T-5412533,JEN 4 - PM,Babalwa Magongo,Friday\n",
        )
        .unwrap();

        let process_date = parse_process_date("2026-05-01").unwrap();
        let rows = load_dialogue_rows_from_csv(file_path.to_str().unwrap(), process_date)
            .expect("rows");

        std::fs::remove_dir_all(&dir).ok();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].start_date, "2026-05-01 15:00:00");
    }

    #[test]
    fn excel_export_requires_preprocess_for_data_rows_even_when_headers_parse() {
        let contents = r#""Start,""Finish"",""Shift: Shift Number"",""Resource: Shift Group"",""Resource: Resource Name"",""Day of Week"""#
            .to_string()
            + "\n"
            + r#""5/2/2026 9:00 AM,""5/2/2026 11:00 AM"",""T-5412533"",""JEN 4 - PM"",""Babalwa Magongo"",""Saturday"""#;

        let prepared = super::prepare_dialogue_csv_for_parsing(&contents);
        assert_ne!(
            prepared, contents,
            "excel export must be preprocessed before parsing"
        );

        let process_date_may_1 = parse_process_date("2026-05-01").unwrap();
        let process_date_may_2 = parse_process_date("2026-05-02").unwrap();

        let dir = std::env::temp_dir().join(format!(
            "sergio-ar-dialogue-excel-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("dialogue-2.csv");
        std::fs::write(&file_path, &contents).unwrap();

        let rows_may_1 = load_dialogue_rows_from_csv(
            file_path.to_str().unwrap(),
            process_date_may_1,
        )
        .expect("rows");
        let rows_may_2 = load_dialogue_rows_from_csv(
            file_path.to_str().unwrap(),
            process_date_may_2,
        )
        .expect("rows");

        std::fs::remove_dir_all(&dir).ok();

        assert_eq!(rows_may_1.len(), 0);
        assert_eq!(rows_may_2.len(), 1);
        assert_eq!(rows_may_2[0].shift, "T-5412533");
    }

    #[test]
    fn prepare_dialogue_csv_accepts_excel_export_format() {
        let contents = r#""Start,""Finish"",""Shift: Shift Number"",""Resource: Shift Group"",""Resource: Resource Name"",""Day of Week"""#
            .to_string()
            + "\n"
            + r#""5/2/2026 9:00 AM,""5/2/2026 11:00 AM"",""T-5412533"",""JEN 4 - PM"",""Babalwa Magongo"",""Saturday"""#;

        let prepared = super::prepare_dialogue_csv_for_parsing(&contents);
        let mut reader = ReaderBuilder::new()
            .trim(csv::Trim::All)
            .delimiter(b',')
            .flexible(true)
            .from_reader(prepared.as_bytes());

        let headers = reader.headers().unwrap();
        assert_eq!(headers.len(), 6);
        build_dialogue_csv_columns(headers).expect("dialogue columns");
    }

    #[test]
    fn identical_export_rows_match_across_two_dialogue_snapshots() {
        let row = make_row(
            "JEN 4 - PM",
            "T-5412533",
            "Babalwa Magongo",
            "5/2/2026 9:00 AM",
            "5/2/2026 11:00 AM",
        );
        let first = vec![row.clone()];
        let second = vec![row];

        let consolidated = consolidate_dialogue_rows(&first, &second);
        assert_eq!(consolidated.len(), 1);
        assert_eq!(consolidated[0].shift_type, "-");
        assert!(
            consolidated
                .iter()
                .all(|entry| entry.shift_type != "Dropped"),
            "expected no dropped rows when snapshots match"
        );
    }

    #[test]
    fn loads_dialogue_rows_from_export_style_csv() {
        let dir = std::env::temp_dir().join(format!(
            "sergio-ar-dialogue-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("dialogue-1.csv");
        std::fs::write(
            &file_path,
            r#""Start,""Finish"",""Shift: Shift Number"",""Resource: Shift Group"",""Resource: Resource Name"",""Day of Week"""#
                .to_string()
                + "\n"
                + r#""5/2/2026 9:00 AM,""5/2/2026 11:00 AM"",""T-5412533"",""JEN 4 - PM"",""Babalwa Magongo"",""Saturday"""#,
        )
        .unwrap();

        let process_date = parse_process_date("2026-05-02").unwrap();
        let rows = load_dialogue_rows_from_csv(
            file_path.to_str().unwrap(),
            process_date,
        )
        .expect("rows");

        std::fs::remove_dir_all(&dir).ok();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].shift, "T-5412533");
        assert_eq!(rows[0].teacher_name, "Babalwa Magongo");
        assert_eq!(rows[0].start_date, "2026-05-02 15:00:00");
        assert_eq!(rows[0].end_date, "2026-05-02 17:00:00");
    }

    #[test]
    fn matching_snapshots_with_different_datetime_strings_are_not_marked_dropped() {
        let first_dialogue_rows = vec![make_row(
            "JEN 4 - PM",
            "T-5412533",
            "Babalwa Magongo",
            "5/2/2026 9:00 AM",
            "5/2/2026 11:00 AM",
        )];
        let second_dialogue_rows = vec![make_row(
            "JEN 4 - PM",
            "T-5412533",
            "Babalwa Magongo",
            "2026-05-02 09:00:00",
            "2026-05-02 11:00:00",
        )];

        let consolidated =
            consolidate_dialogue_rows(&first_dialogue_rows, &second_dialogue_rows);

        assert_eq!(consolidated.len(), 1);
        assert_eq!(consolidated[0].shift_type, "-");
    }

    #[test]
    fn marks_internal_pickups_after_shift_normalization() {
        let first_dialogue_rows = vec![make_row(
            "Alpha",
            "12345",
            "Teacher One",
            "2026-04-20 08:00:00",
            "2026-04-20 09:00:00",
        )];
        let second_dialogue_rows = vec![make_row(
            "Alpha",
            "12345.0",
            "Teacher Two",
            "2026-04-20 08:00:00",
            "2026-04-20 09:00:00",
        )];

        let consolidated_rows =
            consolidate_dialogue_rows(&first_dialogue_rows, &second_dialogue_rows);

        assert_eq!(consolidated_rows.len(), 1);
        assert_eq!(consolidated_rows[0].shift_type, "Internal Pickup");
    }
}
