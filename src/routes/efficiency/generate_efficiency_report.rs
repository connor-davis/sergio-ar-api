use std::collections::HashMap;

use axum::{
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

    let mut grouped_schedules: HashMap<String, Vec<Schedule>> = HashMap::new();
    let mut efficiency_tables: Vec<EfficiencyTable> = Vec::new();

    for schedule in schedules_for_range {
        let schedule_group = schedule.clone().shift_group;

        if grouped_schedules.contains_key(&schedule_group) {
            grouped_schedules
                .get_mut(&schedule_group)
                .unwrap()
                .push(schedule.clone());
        } else {
            grouped_schedules.insert(schedule_group, vec![schedule.clone()]);
        }
    }

    for (shift_group, schedules) in grouped_schedules {
        let mut table_teachers: Vec<String> = Vec::new();
        let mut table_days_efficiencies: Vec<EfficiencyDay> = Vec::new();
        let mut table_days: Vec<String> = Vec::new();

        for schedule in schedules.clone() {
            if !table_teachers.contains(&schedule.teacher_name) {
                table_teachers.push(schedule.teacher_name.clone());
            }
        }

        for teacher in table_teachers.clone() {
            let mut current_date = start_date.date();

            while current_date <= end_date.date() {
                let scheduled = schedules
                    .clone()
                    .into_iter()
                    .filter(|schedule| {
                        schedule.teacher_name == teacher
                            && schedule.start_date.date() == current_date
                    })
                    .count()
                    * 2;

                let mut taught = 0;
                let mut no_shows = 0;

                for schedule in schedules.clone().into_iter().filter(|schedule| {
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

                if !table_days.contains(&current_date.to_string()) {
                    table_days.push(current_date.to_string());
                }

                current_date = current_date.succ_opt().unwrap_or(current_date);
            }
        }

        efficiency_tables.push(EfficiencyTable {
            shift_group: shift_group.clone(),
            table_teachers: table_teachers.clone(),
            table_days_efficiencies: table_days_efficiencies.clone(),
            table_days: table_days.clone(),
        });
    }

    Ok(Json(json!({
        "status": StatusCode::OK.as_u16(),
        "message": "Efficiency report generated successfully!",
        "data": efficiency_tables,
    })))
}
