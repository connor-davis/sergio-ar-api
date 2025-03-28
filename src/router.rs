use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};

use crate::{
    routes::{consolidator, data, efficiency},
    AppState,
};

pub async fn create_router(app_state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route(
            "/upload-and-process",
            post(consolidator::upload_and_process::upload_and_process),
        )
        .route(
            "/generate-consolidated-report",
            get(efficiency::generate_consolidated_report::generate_consolidated_report),
        )
        .route("/shift-groups", get(data::shift_groups::get_shift_groups))
        .route("/schedules", get(data::schedules::get_schedules))
        .fallback(fallback)
        .with_state(app_state)
}

async fn index() -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    Ok(Json(json!({
        "status": StatusCode::OK.as_u16(),
        "message": "Welcome to Core Capital Automatic Reports API!",
        "database": true
    })))
}

async fn fallback() -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    Ok(Json(json!({
        "status": StatusCode::NOT_FOUND.as_u16(),
        "message": "Route not found. Please contact the developer.",
    })))
}
