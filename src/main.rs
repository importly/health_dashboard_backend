use axum::{
    extract::{Json, Path, Query, State},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{error, info};

use backend::db::{self, DbPool, Manifest};
use backend::importer;
use backend::parser;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum JobStatus {
    Processing {
        progress: usize,
        total: Option<usize>,
    },
    Completed {
        records_processed: usize,
    },
    Failed {
        error: String,
    },
}

struct AppState {
    pool: DbPool,
    manifest: Manifest,
    jobs: RwLock<HashMap<String, JobStatus>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    info!("Starting Digital Physiologist Backend...");

    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:health.db".to_string());
    let manifest_path = "metrics_manifest.toml";

    // Initialize DB and Manifest
    let db_url_rwc = if !db_url.contains("mode=") {
        format!("{}?mode=rwc", db_url)
    } else {
        db_url
    };

    let (pool, manifest) = db::init_db(&db_url_rwc, manifest_path).await?;

    info!("Database initialized and schema verified.");

    let shared_state = Arc::new(AppState {
        pool,
        manifest,
        jobs: RwLock::new(HashMap::new()),
    });

    // Configure CORS
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([axum::http::Method::GET, axum::http::Method::POST])
        .allow_headers([axum::http::HeaderName::from_static("content-type")]);

    let app = Router::new()
        .route("/", get(root))
        .route("/health", get(health_handler))
        .route("/ingest", post(ingest_handler))
        .route("/api/ingest/status/{id}", get(get_ingest_status_handler))
        .route("/api/import/external", post(external_import_handler))
        .route("/api/ecg/{id}", get(get_ecg_handler))
        .route("/api/workouts/{id}", get(get_workout_details_handler))
        .route(
            "/api/workouts/{id}/intensity",
            get(get_workout_intensity_handler),
        )
        .route("/api/summary", get(get_summary_handler))
        .route("/api/export/{table}", get(export_data_handler))
        .route("/api/trends", get(get_trends_handler))
        .route("/api/analysis/recovery", get(get_recovery_handler))
        .route("/api/analysis/sleep", get(get_sleep_analysis_handler))
        .route("/api/data/{table}", get(get_data_handler))
        .route("/api/aggregate/{table}", get(aggregate_handler))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(shared_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    info!("Listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn root() -> &'static str {
    "Digital Physiologist Backend Online"
}

async fn health_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

#[derive(Deserialize)]
struct IngestRequest {
    file_path: String,
}

#[derive(serde::Serialize)]
struct IngestResponse {
    message: String,
    job_id: String,
}

async fn ingest_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<IngestRequest>,
) -> Result<Json<IngestResponse>, String> {
    info!("Received ingestion request for: {}", payload.file_path);

    let path = std::path::PathBuf::from(&payload.file_path);
    if !path.exists() {
        return Err(format!("File not found: {}", payload.file_path));
    }

    let job_id = uuid::Uuid::new_v4().to_string();

    // Initialize job status
    {
        let mut jobs = state.jobs.write().await;
        jobs.insert(
            job_id.clone(),
            JobStatus::Processing {
                progress: 0,
                total: None,
            },
        );
    }

    // Spawn background task
    let job_id_task = job_id.clone();
    let state_task = Arc::clone(&state);

    tokio::spawn(async move {
        // We wrap the progress update in a closure that handles the async write lock
        let progress_job_id = job_id_task.clone();
        let progress_state = Arc::clone(&state_task);

        let on_progress = move |count: usize| {
            // Since on_progress is called from synchronous context inside parse_and_ingest loop (for performance),
            // we use a background task or blocking write if necessary.
            // Better: use a channel or just use a sync-safe way to update status.
            // For now, let's keep it simple and use a runtime handle.
            let inner_state = Arc::clone(&progress_state);
            let inner_job_id = progress_job_id.clone();
            tokio::spawn(async move {
                let mut jobs = inner_state.jobs.write().await;
                jobs.insert(
                    inner_job_id,
                    JobStatus::Processing {
                        progress: count,
                        total: None,
                    },
                );
            });
        };

        match parser::parse_and_ingest(
            &path,
            &state_task.pool,
            &state_task.manifest,
            Some(on_progress),
        )
        .await
        {
            Ok(count) => {
                let mut jobs = state_task.jobs.write().await;
                jobs.insert(
                    job_id_task,
                    JobStatus::Completed {
                        records_processed: count,
                    },
                );
            }
            Err(e) => {
                error!("Ingestion failed for job {}: {:?}", job_id_task, e);
                let mut jobs = state_task.jobs.write().await;
                jobs.insert(
                    job_id_task,
                    JobStatus::Failed {
                        error: e.to_string(),
                    },
                );
            }
        }
    });

    Ok(Json(IngestResponse {
        message: "Ingestion started in background".to_string(),
        job_id,
    }))
}

async fn get_ingest_status_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<JobStatus>, String> {
    let jobs = state.jobs.read().await;
    match jobs.get(&id) {
        Some(status) => Ok(Json(status.clone())),
        None => Err(format!("Job ID {} not found", id)),
    }
}

async fn external_import_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, String> {
    info!("Triggering external import scanning...");

    let base_dir = std::path::Path::new("test_export");

    match importer::run_external_import(base_dir, &state.pool, &state.manifest).await {
        Ok(_) => Ok(Json(serde_json::json!({
            "message": "External import scan complete"
        }))),
        Err(e) => {
            error!("External import failed: {:?}", e);
            Err(format!("External import failed: {}", e))
        }
    }
}

#[derive(Deserialize)]
struct EcgQuery {
    downsample: Option<usize>,
}

async fn get_ecg_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(query): Query<EcgQuery>,
) -> Result<Json<serde_json::Value>, String> {
    info!("Fetching ECG recording ID: {}", id);

    let row: (String, String, String, String) = sqlx::query_as(
        "SELECT recorded_at, classification, sample_rate, voltage_samples FROM ecg_recordings WHERE id = ?",
    )
    .bind(id)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| format!("ECG not found: {}", e))?;

    let (recorded_at, classification, sample_rate, raw_samples) = row;

    // Parse samples
    let mut samples: Vec<f64> = raw_samples
        .split(',')
        .filter_map(|s| s.parse::<f64>().ok())
        .collect();

    // Apply downsampling if requested
    if let Some(factor) = query.downsample {
        if factor > 1 {
            samples = samples.into_iter().step_by(factor).collect();
        }
    }

    Ok(Json(serde_json::json!({
        "id": id,
        "recorded_at": recorded_at,
        "classification": classification,
        "sample_rate": sample_rate,
        "sample_count": samples.len(),
        "samples": samples
    })))
}

async fn get_workout_details_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, String> {
    info!("Fetching details for workout session: {}", id);

    let details = db::get_workout_details(&state.pool, &id)
        .await
        .map_err(|e| format!("Workout not found: {}", e))?;

    Ok(Json(details))
}

async fn get_workout_intensity_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, String> {
    info!("Analyzing intensity for workout session: {}", id);

    let intensity = db::get_workout_intensity(&state.pool, &state.manifest, &id)
        .await
        .map_err(|e| format!("Intensity analysis failed: {}", e))?;

    Ok(Json(intensity))
}

async fn get_summary_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, String> {
    let summary = db::get_db_summary(&state.pool, &state.manifest)
        .await
        .map_err(|e| format!("Failed to generate summary: {}", e))?;

    Ok(Json(summary))
}

async fn export_data_handler(
    State(state): State<Arc<AppState>>,
    Path(table): Path<String>,
) -> Result<axum::response::Response, String> {
    // Validate table exists
    if !state.manifest.tables.contains_key(&table) {
        return Err(format!("Table '{}' not defined in manifest", table));
    }

    let csv_data = db::export_table_to_csv(&state.pool, &table)
        .await
        .map_err(|e| format!("Export failed: {}", e))?;

    axum::response::Response::builder()
        .header("content-type", "text/csv")
        .header(
            "content-disposition",
            format!("attachment; filename=\"{}.csv\"", table),
        )
        .body(axum::body::Body::from(csv_data))
        .map_err(|e| e.to_string())
}

#[derive(Deserialize)]
struct TrendsQuery {
    start: String,
    end: String,
}

async fn get_trends_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TrendsQuery>,
) -> Result<Json<serde_json::Value>, String> {
    let trends = db::get_biometric_trends(&state.pool, &state.manifest, &query.start, &query.end)
        .await
        .map_err(|e| format!("Failed to fetch trends: {}", e))?;

    Ok(Json(trends))
}

async fn get_recovery_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, String> {
    let analysis = db::get_recovery_analysis(&state.pool)
        .await
        .map_err(|e| format!("Analysis failed: {}", e))?;

    Ok(Json(analysis))
}

#[derive(Deserialize)]
struct SleepQuery {
    date: String,
}

async fn get_sleep_analysis_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<SleepQuery>,
) -> Result<Json<serde_json::Value>, String> {
    let summary = db::get_sleep_summary(&state.pool, &query.date)
        .await
        .map_err(|e| format!("Sleep analysis failed: {}", e))?;

    Ok(Json(summary))
}

#[derive(Deserialize)]
struct GetDataParams {
    limit: Option<i32>,
    sort: Option<String>,
    start: Option<String>,
    end: Option<String>,
}

async fn get_data_handler(
    State(state): State<Arc<AppState>>,
    Path(table): Path<String>,
    Query(params): Query<GetDataParams>,
) -> Result<Json<Vec<serde_json::Value>>, String> {
    // Validate table exists in manifest (either in tables or external_sources)
    let exists_in_tables = state.manifest.tables.contains_key(&table);
    let exists_in_ext = if let Some(ext) = &state.manifest.external_sources {
        let is_ecg = ext
            .ecg
            .as_ref()
            .map(|e| e.target_table == table)
            .unwrap_or(false);
        let is_routes = ext
            .routes
            .as_ref()
            .map(|r| r.target_table == table)
            .unwrap_or(false);
        is_ecg || is_routes
    } else {
        false
    };

    if !exists_in_tables && !exists_in_ext {
        return Err(format!("Table '{}' not defined in manifest", table));
    }

    let limit = params.limit.unwrap_or(100);
    let sort_col = params.sort.as_deref();
    let start = params.start.as_deref();
    let end = params.end.as_deref();

    let data = db::query_table(&state.pool, &table, limit, sort_col, start, end)
        .await
        .map_err(|e| format!("Query failed: {}", e))?;

    Ok(Json(data))
}

#[derive(Deserialize)]
struct AggregateParams {
    bucket: String, // "hour", "day", "month"
    start: Option<String>,
    end: Option<String>,
}

async fn aggregate_handler(
    State(state): State<Arc<AppState>>,
    Path(table): Path<String>,
    Query(params): Query<AggregateParams>,
) -> Result<Json<Vec<serde_json::Value>>, String> {
    if !state.manifest.tables.contains_key(&table) {
        return Err(format!("Table '{}' not defined in manifest", table));
    }

    let start = params.start.as_deref();
    let end = params.end.as_deref();

    let data = db::aggregate_table(
        &state.pool,
        &state.manifest,
        &table,
        &params.bucket,
        start,
        end,
    )
    .await
    .map_err(|e| format!("Aggregation failed: {}", e))?;

    Ok(Json(data))
}
