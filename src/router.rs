use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};

use crate::{
    routes::{consolidator, efficiency},
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
            "/generate-efficiency-report",
            get(efficiency::generate_efficiency_report::generate_efficiency_report),
        )
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
