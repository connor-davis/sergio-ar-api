use axum::extract::State;
use serde::{Deserialize, Serialize};

use crate::AppState;

use axum::{http::StatusCode, response::IntoResponse, Json};
use serde_json::{json, Value};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ShiftGroup {
    pub shift_group: String,
}

pub async fn get_shift_groups(
    State(app_state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    let shift_groups = sqlx::query_as!(
        ShiftGroup,
        r#"
        SELECT DISTINCT shift_group
        FROM schedules
        "#,
    )
    .fetch_all(&app_state.db)
    .await
    .map_err(|e| {
        tracing::error!("Error fetching shift groups: {:?}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": "Error fetching shift groups" }),
        )
    });

    match shift_groups {
        Ok(shift_groups) => Ok(Json(json!({
            "status": StatusCode::OK.as_u16(),
            "shift_groups": shift_groups
        }))),
        Err((status_code, json)) => Err((status_code, Json(json))),
    }
}
