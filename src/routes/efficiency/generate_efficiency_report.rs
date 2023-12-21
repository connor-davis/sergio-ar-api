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
pub struct EfficiencyReportParams {
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EfficiencyTable {
    pub shift_group: String,
    pub table_teachers: Vec<String>,
    pub table_days_efficiencies: Vec<EfficiencyDay>,
    pub table_days: Vec<String>,
    pub table_summary: Vec<EfficiencySummary>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EfficiencyDay {
    pub date: NaiveDate,
    pub scheduled: i32,
    pub taught: i32,
    pub no_shows: i32,
    pub variance: i32,
    pub teacher_name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EfficiencySummary {
    pub teacher_name: String,
    pub scheduled: i32,
    pub taught: i32,
    pub no_shows: i32,
    pub variance: i32,
    pub percentage_variance: f32,
}

pub async fn generate_efficiency_report(
    Query(params): Query<EfficiencyReportParams>,
    State(app_state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    let start_date = params.start_date;
    let end_date = params.end_date;
    let shift_group = params.shift_group;

    // convert the start and end dates to naivedatetime
    let start_date = NaiveDateTime::new(
        start_date,
        NaiveTime::parse_from_str("00:00:00", "%H:%M:%S").unwrap(),
    );
    let end_date = NaiveDateTime::new(
        end_date,
        NaiveTime::parse_from_str("23:59:59", "%H:%M:%S").unwrap(),
    );

    let schedules_for_range = sqlx::query_as!(
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

    let mut table_teachers: Vec<String> = Vec::new();
    let mut table_days_efficiencies: Vec<EfficiencyDay> = Vec::new();
    let mut table_days: Vec<String> = Vec::new();
    let mut table_summary: Vec<EfficiencySummary> = Vec::new();

    for schedule in schedules_for_range.clone() {
        if !table_teachers.contains(&schedule.teacher_name) {
            table_teachers.push(schedule.teacher_name.clone());
        }
    }

    for teacher in table_teachers.clone() {
        let mut current_date = start_date.date();

        let mut total_scheduled = 0;
        let mut total_taught = 0;
        let mut total_no_shows = 0;
        let mut total_variance = 0;

        while current_date <= end_date.date() {
            let scheduled = schedules_for_range
                .clone()
                .into_iter()
                .filter(|schedule| {
                    schedule.teacher_name == teacher && schedule.start_date.date() == current_date
                })
                .count()
                * 2;

            let mut taught = 0;
            let mut no_shows = 0;

            for schedule in schedules_for_range.clone().into_iter().filter(|schedule| {
                schedule.teacher_name == teacher && schedule.start_date.date() == current_date
            }) {
                let invoices_for_schedule = sqlx::query!(
                        r#"
                            SELECT
                                eligible
                            FROM invoices
                            WHERE invoices.shift = $1
                        "#,
                        schedule.shift
                    )
                    .fetch_all(&app_state.db)
                    .await
                    .map_err(|error| {
                        tracing::error!("Error fetching invoices for schedule: {:?}", error);
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({
                                "status": StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                                "message": "Error fetching invoices for schedule. Please contact the developer.",
                            })),
                        )
                    })?;

                for invoice in invoices_for_schedule {
                    if invoice.eligible {
                        taught += 1;
                    } else {
                        no_shows += 1;
                    }
                }
            }

            let initial_variance = taught - scheduled;
            let variance = initial_variance - no_shows;

            table_days_efficiencies.push(EfficiencyDay {
                date: current_date,
                scheduled: scheduled as i32,
                taught: taught as i32,
                no_shows: no_shows as i32,
                variance: variance as i32,
                teacher_name: teacher.clone(),
            });

            total_scheduled += scheduled;
            total_taught += taught;
            total_no_shows += no_shows;
            total_variance += variance;

            if !table_days.contains(&current_date.to_string()) {
                table_days.push(current_date.to_string());
            }

            current_date = current_date.succ_opt().unwrap_or(current_date);
        }

        let percentage_variance =
            (total_taught as f32 - total_scheduled as f32) / total_scheduled as f32 * 100.0;

        table_summary.push(EfficiencySummary {
            teacher_name: teacher.clone(),
            scheduled: total_scheduled as i32,
            taught: total_taught as i32,
            no_shows: total_no_shows as i32,
            variance: total_variance as i32,
            percentage_variance,
        });
    }

    // Convert the efficiency table to a CSV file
    let mut csv_file = String::new();

    // Use below as an example
    // teacher, 01-11-2023, scheduled, taught, no_shows, variance, total, scheduled, taught, no_shows, variance, percentage_variance
    // teacher, , 4, 4, 0, 0, , 4, 4, 0, 0, 0

    // Write the header
    csv_file.push_str("teacher,");

    for day in table_days.clone() {
        csv_file.push_str(&format!("{},scheduled,taught,no_shows,variance,", day));
    }

    csv_file.push_str("total,scheduled,taught,no_shows,variance,percentage_variance\n");

    let mut csv_rows: Vec<String> = Vec::new();

    table_teachers.sort_by(|a, b| a.cmp(b));

    for teacher in table_teachers.clone() {
        let mut csv_row = String::new();

        csv_row.push_str(&format!("{}", teacher));

        for day_efficiency in table_days_efficiencies
            .clone()
            .into_iter()
            .filter(|day_efficiency| day_efficiency.teacher_name == teacher)
        {
            csv_row.push_str(&format!(
                ",,{},{},{},{}",
                day_efficiency.scheduled,
                day_efficiency.taught,
                day_efficiency.no_shows,
                day_efficiency.variance
            ));
        }

        let teacher_summary = table_summary
            .clone()
            .into_iter()
            .filter(|teacher_summary| teacher_summary.teacher_name == teacher)
            .next()
            .unwrap();

        csv_row.push_str(&format!(
            ",,{},{},{},{},{}",
            teacher_summary.scheduled,
            teacher_summary.taught,
            teacher_summary.no_shows,
            teacher_summary.variance,
            teacher_summary.percentage_variance
        ));

        csv_rows.push(csv_row);
    }

    for csv_row in csv_rows {
        csv_file.push_str(&format!("{}\n", csv_row));
    }

    csv_file.push_str(&format!("total"));

    for day in table_days.clone() {
        let mut total_scheduled = 0;
        let mut total_taught = 0;
        let mut total_no_shows = 0;
        let mut total_variance = 0;

        for day_efficiency in table_days_efficiencies
            .clone()
            .into_iter()
            .filter(|day_efficiency| day_efficiency.date.to_string() == day)
        {
            total_scheduled += day_efficiency.scheduled;
            total_taught += day_efficiency.taught;
            total_no_shows += day_efficiency.no_shows;
            total_variance += day_efficiency.variance;
        }

        csv_file.push_str(&format!(
            ",,{},{},{},{}",
            total_scheduled, total_taught, total_no_shows, total_variance
        ));
    }

    let mut total_scheduled = 0;
    let mut total_taught = 0;
    let mut total_no_shows = 0;
    let mut total_variance = 0;
    let mut total_percentage_variance = 0.0;

    for teacher_summary in table_summary.clone() {
        total_scheduled += teacher_summary.scheduled;
        total_taught += teacher_summary.taught;
        total_no_shows += teacher_summary.no_shows;
        total_variance += teacher_summary.variance;
        total_percentage_variance += teacher_summary.percentage_variance;
    }

    csv_file.push_str(&format!(
        ",,{},{},{},{},{}",
        total_scheduled, total_taught, total_no_shows, total_variance, total_percentage_variance
    ));

    Ok(Body::from(csv_file).into_response())
}
