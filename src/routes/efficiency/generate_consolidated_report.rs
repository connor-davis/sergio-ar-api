use axum::{
    body::Body,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct ConsolidatedReportParams {
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub shift_group: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Schedule {
    pub id: i32,
    pub start_date: NaiveDateTime,
    pub end_date: NaiveDateTime,
    pub teacher_name: String,
    pub shift_group: String,
    pub shift: String,
    pub shift_type: String,
}

pub async fn generate_consolidated_report(
    Query(params): Query<ConsolidatedReportParams>,
    State(app_state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    let start_date = params.start_date;
    let end_date = params.end_date;
    let shift_group = params.shift_group;

    let start_date = NaiveDateTime::new(
        start_date,
        NaiveTime::parse_from_str("00:00:00", "%H:%M:%S").unwrap(),
    );
    let end_date = NaiveDateTime::new(
        end_date,
        NaiveTime::parse_from_str("23:59:59", "%H:%M:%S").unwrap(),
    );

    let mut schedules_for_range = sqlx::query_as!(
        Schedule,
        r#"
            SELECT
                schedules.id as id,
                schedules.start_date as start_date,
                schedules.end_date as end_date,
                teachers.name as teacher_name,
                schedules.shift_group as shift_group,
                schedules.shift as shift,
                schedules.shift_type as shift_type
            FROM schedules
            LEFT JOIN teachers ON schedules.teacher_id = teachers.id
            WHERE schedules.start_date >= $1 AND schedules.end_date <= $2 AND schedules.shift_group = $3
        "#,
        start_date,
        end_date,
        shift_group
    )
    .fetch_all(&app_state.db)
    .await
    .map_err(|error| {
        tracing::error!("Error fetching schedules for range: {:?}", error);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "status": StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                "message": "Error fetching schedules for range. Please contact the developer.",
            })),
        )
    })?;

    schedules_for_range.sort_by(|a, b| {
        let a_cmb = format!(
            "{}-{}-{}",
            a.start_date.date(),
            a.teacher_name,
            a.start_date.time()
        );
        let b_cmb = format!(
            "{}-{}-{}",
            b.start_date.date(),
            b.teacher_name,
            b.start_date.time()
        );

        a_cmb.to_lowercase().cmp(&&b_cmb.to_lowercase())
    });

    let mut consolidated_report_csv =
        "Teacher,Shift,Shift Type,Start Date,End Date,,,,Teacher,Scheduled,Picked Up,Dropped\n"
            .to_string();

    let mut csv_lines: Vec<String> = Vec::new();

    let mut table_teachers: Vec<String> = Vec::new();

    for schedule in schedules_for_range.clone() {
        if !table_teachers.contains(&schedule.teacher_name) {
            table_teachers.push(schedule.teacher_name.clone());
        }

        csv_lines.push(format!(
            "{},{},{},{},{},,,,\n",
            schedule.teacher_name,
            schedule.shift,
            schedule.shift_type,
            schedule.start_date,
            schedule.end_date
        ));
    }

    let mut current_teacher = 0;
    let mut total_scheduled = 0;
    let mut total_picked_up = 0;
    let mut total_dropped = 0;

    table_teachers.sort();

    for teacher in table_teachers {
        let mut scheduled = 0;
        let mut picked_up = 0;
        let mut dropped = 0;

        for schedule in schedules_for_range
            .clone()
            .into_iter()
            .filter(|schedule| schedule.teacher_name.eq(&teacher))
            .collect::<Vec<Schedule>>()
        {
            scheduled += 1;

            match schedule.shift_type.as_str() {
                "Pickup" => picked_up += 1,
                "Internal Pickup" => picked_up += 1,
                "Dropped & Picked Up" => picked_up += 1,
                "Dropped" => dropped += 1,
                _ => {}
            }
        }

        let initial_line = csv_lines[current_teacher].clone();

        let mut new_line = initial_line.replace("\n", "");

        new_line.push_str(&format!(
            "{},{},{},{}\n",
            teacher, scheduled, picked_up, dropped
        ));

        csv_lines[current_teacher] = new_line;

        total_scheduled += scheduled;
        total_picked_up += picked_up;
        total_dropped += dropped;

        current_teacher += 1;
    }

    let initial_line = csv_lines[current_teacher].clone();

    let mut new_line = initial_line.replace("\n", "");

    let total_line = format!(
        "Total,{},{},{}\n",
        total_scheduled, total_picked_up, total_dropped
    );

    new_line.push_str(&total_line);

    csv_lines[current_teacher] = new_line;

    for line in csv_lines {
        consolidated_report_csv.push_str(&line);
    }

    Ok(Body::from(consolidated_report_csv).into_response())
}
