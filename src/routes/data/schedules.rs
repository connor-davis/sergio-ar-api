use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct GetSchedulesParams {
    pub start_date: String,
    pub end_date: String,
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

pub async fn get_schedules(
    Query(params): Query<GetSchedulesParams>,
    State(app_state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    let start_date = NaiveDateTime::parse_from_str(&params.start_date, "%Y-%m-%d %H:%M:%S")
        .map_err(|e| {
            tracing::error!("Error parsing start_date: {:?}", e);
            (
                StatusCode::BAD_REQUEST,
                json!({ "error": "Error parsing start_date" }),
            )
        });

    match start_date {
        Ok(start_date) => {
            let end_date = NaiveDateTime::parse_from_str(&params.end_date, "%Y-%m-%d %H:%M:%S")
                .map_err(|e| {
                    tracing::error!("Error parsing end_date: {:?}", e);
                    (
                        StatusCode::BAD_REQUEST,
                        json!({ "error": "Error parsing end_date" }),
                    )
                });

            match end_date {
                Ok(end_date) => {
                    let schedules = sqlx::query_as!(
                        Schedule,
                        r#"
                        SELECT
                            schedules.id,
                            schedules.start_date,
                            schedules.end_date,
                            teachers.name AS teacher_name,
                            schedules.shift_group,
                            schedules.shift,
                            schedules.shift_type
                        FROM schedules
                        LEFT JOIN teachers ON schedules.teacher_id = teachers.id
                        WHERE schedules.shift_group = $1 AND schedules.start_date >= $2 AND schedules.start_date <= $3
                        ORDER BY teachers.name, schedules.start_date ASC
                        "#,
                        params.shift_group,
                        start_date,
                        end_date
                    )
                    .fetch_all(&app_state.db)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error fetching schedules: {:?}", e);
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            json!({ "error": "Error fetching schedules" }),
                        )
                    });

                    match schedules {
                        Ok(schedules) => Ok((
                            StatusCode::OK,
                            Json(json!({
                                "status": StatusCode::OK.as_u16(),
                                "schedules": schedules
                            })),
                        )),
                        Err((status_code, json)) => Err((status_code, Json(json))),
                    }
                }
                Err((status_code, json)) => Err((status_code, Json(json))),
            }
        }
        Err((status_code, json)) => Err((status_code, Json(json))),
    }
}
