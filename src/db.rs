use anyhow::{Context, Result};
use chrono::DateTime;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sqlx::{sqlite::SqlitePoolOptions, Column, Pool, Row, Sqlite};
use std::{
    collections::{HashMap, HashSet},
    fs,
};
use tracing::info;

#[derive(Debug, Deserialize, Clone)]
pub struct Manifest {
    pub settings: Option<Settings>,
    pub user_profile: Option<UserProfile>,
    pub tables: HashMap<String, TableConfig>,
    pub external_sources: Option<ExternalSources>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct UserProfile {
    pub max_heart_rate: Option<i32>,
    pub resting_heart_rate: Option<i32>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExternalSources {
    pub ecg: Option<EcgConfig>,
    pub routes: Option<RouteConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EcgConfig {
    pub folder: String,
    pub file_pattern: String,
    pub target_table: String,
    pub metadata_map: Vec<EcgMetadataMap>,
    pub payload: EcgPayload,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EcgMetadataMap {
    pub csv_key: String,
    pub db_column: String,
    pub data_type: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EcgPayload {
    pub db_column: String,
    pub data_type: String,
    pub source_unit: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RouteConfig {
    pub folder: String,
    pub file_pattern: String,
    pub target_table: String,
    pub columns: Vec<RouteColumn>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RouteColumn {
    pub xml_tag: String,
    pub db_column: String,
    pub data_type: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub batch_size: Option<usize>,
    pub timezone: Option<String>,
    pub import_dirs: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TableConfig {
    pub description: Option<String>,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ColumnDefinition {
    #[serde(alias = "name")]
    pub field_name: String,

    #[serde(alias = "hk_type")]
    pub hk_identifier: Option<String>,

    pub hk_attribute: Option<String>,

    #[serde(default)]
    pub is_primary_key: bool,

    pub extraction_source: Option<String>,

    // Optional, defaults to "raw" if missing (implied by manifest absence)
    #[serde(default = "default_aggregate")]
    pub aggregate: String,

    pub data_type: String,
    pub expression: Option<String>,
}

fn default_aggregate() -> String {
    "raw".to_string()
}

pub type DbPool = Pool<Sqlite>;

pub async fn init_db(db_url: &str, manifest_path: &str) -> Result<(DbPool, Manifest)> {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(db_url)
        .await
        .context("Failed to connect to SQLite")?;

    let manifest_content =
        fs::read_to_string(manifest_path).context("Failed to read metrics_manifest.toml")?;
    let manifest: Manifest =
        toml::from_str(&manifest_content).context("Failed to parse metrics_manifest.toml")?;

    ensure_schema(&pool, &manifest).await?;
    ensure_indices(&pool, &manifest).await?;
    ensure_external_schema(&pool, &manifest).await?;

    Ok((pool, manifest))
}

pub async fn query_table(
    pool: &DbPool,
    table_name: &str,
    limit: i32,
    sort_col: Option<&str>,
    start: Option<&str>,
    end: Option<&str>,
) -> Result<Vec<Value>> {
    let sort_by = sort_col.unwrap_or("start_date");
    
    let mut query_parts = Vec::new();
    if start.is_some() {
        query_parts.push(format!("{} >= ?", sort_by));
    }
    if end.is_some() {
        query_parts.push(format!("{} <= ?", sort_by));
    }

    let where_clause = if query_parts.is_empty() {
        "".to_string()
    } else {
        format!("WHERE {}", query_parts.join(" AND "))
    };

    let sql = format!(
        "SELECT * FROM {} {} ORDER BY {} DESC LIMIT ?",
        table_name, where_clause, sort_by
    );

    let mut q = sqlx::query(&sql);
    if let Some(s) = start {
        q = q.bind(s);
    }
    if let Some(e) = end {
        q = q.bind(e);
    }
    q = q.bind(limit);

    let rows = q
        .fetch_all(pool)
        .await
        .with_context(|| format!("Failed to query table {}", table_name))?;

    let mut results = Vec::new();

    for row in rows {
        let mut map = Map::new();

        for col in row.columns() {
            let col_name = col.name();
            if let Ok(val) = row.try_get::<f64, _>(col_name) {
                map.insert(col_name.to_string(), json!(val));
            } else if let Ok(val) = row.try_get::<i64, _>(col_name) {
                map.insert(col_name.to_string(), json!(val));
            } else if let Ok(val) = row.try_get::<String, _>(col_name) {
                map.insert(col_name.to_string(), json!(val));
            } else {
                map.insert(col_name.to_string(), Value::Null);
            }
        }
        results.push(Value::Object(map));
    }

    Ok(results)
}

pub async fn get_workout_details(
    pool: &DbPool,
    session_id: &str,
) -> Result<Value> {
    // 1. Fetch workout
    let row = sqlx::query("SELECT * FROM workouts WHERE session_id = ?")
        .bind(session_id)
        .fetch_one(pool)
        .await
        .context("Workout not found")?;

    let mut workout_map = Map::new();
    for col in row.columns() {
        let col_name = col.name();
        if let Ok(val) = row.try_get::<f64, _>(col_name) {
            workout_map.insert(col_name.to_string(), json!(val));
        } else if let Ok(val) = row.try_get::<i64, _>(col_name) {
            workout_map.insert(col_name.to_string(), json!(val));
        } else if let Ok(val) = row.try_get::<String, _>(col_name) {
            workout_map.insert(col_name.to_string(), json!(val));
        } else {
            workout_map.insert(col_name.to_string(), Value::Null);
        }
    }

    // 2. Fetch route points if linked
    if let Some(route_file) = workout_map.get("route_file").and_then(|v| v.as_str()) {
        let points = sqlx::query("SELECT timestamp, latitude, longitude, elevation, speed_ms FROM route_points WHERE file_name = ? ORDER BY timestamp ASC")
            .bind(route_file)
            .fetch_all(pool)
            .await?;

        let mut points_vec = Vec::new();
        let mut total_distance_m = 0.0;
        let mut total_elevation_gain_m = 0.0;
        let mut prev_point: Option<(f64, f64, f64)> = None;

        for p_row in points {
            let lat = p_row.get::<f64, _>("latitude");
            let lon = p_row.get::<f64, _>("longitude");
            let elev = p_row.get::<f64, _>("elevation");
            
            let mut p_map = Map::new();
            p_map.insert("timestamp".to_string(), json!(p_row.get::<String, _>("timestamp")));
            p_map.insert("latitude".to_string(), json!(lat));
            p_map.insert("longitude".to_string(), json!(lon));
            p_map.insert("elevation".to_string(), json!(elev));
            p_map.insert("speed_ms".to_string(), json!(p_row.get::<f64, _>("speed_ms")));
            points_vec.push(Value::Object(p_map));

            if let Some((p_lat, p_lon, p_elev)) = prev_point {
                total_distance_m += calculate_haversine(p_lat, p_lon, lat, lon);
                if elev > p_elev {
                    total_elevation_gain_m += elev - p_elev;
                }
            }
            prev_point = Some((lat, lon, elev));
        }
        workout_map.insert("route_points".to_string(), json!(points_vec));
        workout_map.insert("calculated_distance_km".to_string(), json!(total_distance_m / 1000.0));
        workout_map.insert("calculated_elevation_gain_m".to_string(), json!(total_elevation_gain_m));
    }

    Ok(Value::Object(workout_map))
}

fn calculate_haversine(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6371000.0; // Earth radius in meters
    let phi1 = lat1.to_radians();
    let phi2 = lat2.to_radians();
    let d_phi = (lat2 - lat1).to_radians();
    let d_lambda = (lon2 - lon1).to_radians();

    let a = (d_phi / 2.0).sin().powi(2)
        + phi1.cos() * phi2.cos() * (d_lambda / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    r * c
}

pub async fn get_db_summary(
    pool: &DbPool,
    manifest: &Manifest,
) -> Result<Value> {
    let mut summary = Map::new();
    let mut table_counts = Map::new();

    // 1. Core Tables from Manifest
    for table_name in manifest.tables.keys() {
        let count: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {}", table_name))
            .fetch_one(pool)
            .await
            .unwrap_or((0,));
        table_counts.insert(table_name.clone(), json!(count.0));
    }

    // 2. External Tables
    if let Some(ext) = &manifest.external_sources {
        if let Some(ecg) = &ext.ecg {
            let count: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {}", ecg.target_table))
                .fetch_one(pool)
                .await
                .unwrap_or((0,));
            table_counts.insert(ecg.target_table.clone(), json!(count.0));
        }
        if let Some(routes) = &ext.routes {
            let count: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {}", routes.target_table))
                .fetch_one(pool)
                .await
                .unwrap_or((0,));
            table_counts.insert(routes.target_table.clone(), json!(count.0));
        }
    }

    summary.insert("tables".to_string(), Value::Object(table_counts));
    summary.insert("database_size_mb".to_string(), json!(fs::metadata("health.db")?.len() / 1024 / 1024));

    Ok(Value::Object(summary))
}

pub async fn get_workout_intensity(
    pool: &DbPool,
    manifest: &Manifest,
    session_id: &str,
) -> Result<Value> {
    // 1. Fetch workout range
    let workout: (String, String) = sqlx::query_as("SELECT start_date, end_date FROM workouts WHERE session_id = ?")
        .bind(session_id)
        .fetch_one(pool)
        .await
        .context("Workout not found")?;

    // 2. Fetch HR samples during workout
    let samples: Vec<(f64,)> = sqlx::query_as("SELECT heart_rate FROM vitals WHERE heart_rate > 0 AND start_date >= ? AND start_date <= ?")
        .bind(&workout.0)
        .bind(&workout.1)
        .fetch_all(pool)
        .await?;

    let max_hr = manifest.user_profile.as_ref().and_then(|u| u.max_heart_rate).unwrap_or(190) as f64;

    let mut zones = HashMap::new();
    zones.insert("Z1_Recovery", 0);   // < 60%
    zones.insert("Z2_Aerobic", 0);    // 60-70%
    zones.insert("Z3_Steady", 0);     // 70-80%
    zones.insert("Z4_Threshold", 0);  // 80-90%
    zones.insert("Z5_Anaerobic", 0);  // > 90%

    for (hr,) in &samples {
        let pct = (hr / max_hr) * 100.0;
        let zone = match pct {
            p if p < 60.0 => "Z1_Recovery",
            p if p < 70.0 => "Z2_Aerobic",
            p if p < 80.0 => "Z3_Steady",
            p if p < 90.0 => "Z4_Threshold",
            _ => "Z5_Anaerobic",
        };
        *zones.entry(zone).or_insert(0) += 1;
    }

    Ok(json!({
        "session_id": session_id,
        "sample_count": samples.len(),
        "max_hr_used": max_hr,
        "zones": zones
    }))
}

pub async fn export_table_to_csv(
    pool: &DbPool,
    table_name: &str,
) -> Result<String> {
    let sql = format!("SELECT * FROM {}", table_name);
    let rows = sqlx::query(&sql)
        .fetch_all(pool)
        .await?;

    let mut wtr = csv::Writer::from_writer(vec![]);

    if !rows.is_empty() {
        // Write Headers
        let headers: Vec<&str> = rows[0].columns().iter().map(|c| c.name()).collect();
        wtr.write_record(&headers)?;

        // Write Rows
        for row in rows {
            let mut record = Vec::new();
            for col in row.columns() {
                let val: String = if let Ok(v) = row.try_get::<f64, _>(col.name()) {
                    v.to_string()
                } else if let Ok(v) = row.try_get::<i64, _>(col.name()) {
                    v.to_string()
                } else if let Ok(v) = row.try_get::<String, _>(col.name()) {
                    v
                } else {
                    "".to_string()
                };
                record.push(val);
            }
            wtr.write_record(&record)?;
        }
    }

    let inner = wtr.into_inner().map_err(|e| anyhow::anyhow!("CSV error: {}", e))?;
    Ok(String::from_utf8(inner)?)
}

pub async fn aggregate_table(
    pool: &DbPool,
    manifest: &Manifest,
    table_name: &str,
    bucket: &str,
    start: Option<&str>,
    end: Option<&str>,
) -> Result<Vec<Value>> {
    let table_config = manifest
        .tables
        .get(table_name)
        .ok_or_else(|| anyhow::anyhow!("Table {} not found in manifest", table_name))?;

    let time_fmt = match bucket {
        "hour" => "%Y-%m-%dT%H:00:00Z",
        "day" => "%Y-%m-%d",
        "month" => "%Y-%m",
        _ => {
            return Err(anyhow::anyhow!(
                "Invalid bucket. Use 'hour', 'day', or 'month'"
            ))
        }
    };

    let mut query_parts = Vec::new();
    if start.is_some() {
        query_parts.push("start_date >= ?".to_string());
    }
    if end.is_some() {
        query_parts.push("start_date <= ?".to_string());
    }

    let where_clause = if query_parts.is_empty() {
        "".to_string()
    } else {
        format!("WHERE {}", query_parts.join(" AND "))
    };

    let mut select_parts = vec![format!(
        "strftime('{}', start_date) as time_bucket",
        time_fmt
    )];

    for col in &table_config.columns {
        match col.aggregate.as_str() {
            "avg" => select_parts.push(format!("AVG({}) as {}", col.field_name, col.field_name)),
            "sum" => select_parts.push(format!("SUM({}) as {}", col.field_name, col.field_name)),
            "min" => select_parts.push(format!("MIN({}) as {}", col.field_name, col.field_name)),
            "max" => select_parts.push(format!("MAX({}) as {}", col.field_name, col.field_name)),
            "count" => {
                select_parts.push(format!("COUNT({}) as {}", col.field_name, col.field_name))
            }
            _ => {}
        }
    }

    let select_clause = select_parts.join(", ");
    let sql = format!(
        "SELECT {} FROM {} {} GROUP BY time_bucket ORDER BY time_bucket DESC",
        select_clause, table_name, where_clause
    );

    let mut q = sqlx::query(&sql);
    if let Some(s) = start {
        q = q.bind(s);
    }
    if let Some(e) = end {
        q = q.bind(e);
    }

    let rows = q
        .fetch_all(pool)
        .await
        .with_context(|| format!("Failed to aggregate table {}", table_name))?;

    let mut results = Vec::new();
    for row in rows {
        let mut map = Map::new();
        if let Ok(val) = row.try_get::<String, _>("time_bucket") {
            map.insert("time_bucket".to_string(), json!(val));
        }

        for col in &table_config.columns {
            if ["avg", "sum", "min", "max", "count"].contains(&col.aggregate.as_str()) {
                if let Ok(val) = row.try_get::<f64, _>(col.field_name.as_str()) {
                    map.insert(col.field_name.clone(), json!(val));
                } else if let Ok(val) = row.try_get::<i64, _>(col.field_name.as_str()) {
                    map.insert(col.field_name.clone(), json!(val));
                } else {
                    map.insert(col.field_name.clone(), Value::Null);
                }
            }
        }
        results.push(Value::Object(map));
    }

    Ok(results)
}

pub async fn get_biometric_trends(
    pool: &DbPool,
    manifest: &Manifest,
    start: &str,
    end: &str,
) -> Result<Value> {
    let table_config = manifest.tables.get("vitals")
        .ok_or_else(|| anyhow::anyhow!("Vitals table not found in manifest"))?;

    let mut select_parts = Vec::new();
    for col in &table_config.columns {
        if col.data_type == "REAL" || col.data_type == "INTEGER" {
            select_parts.push(format!("AVG({0}) as {0}_avg", col.field_name));
            select_parts.push(format!("MIN({0}) as {0}_min", col.field_name));
            select_parts.push(format!("MAX({0}) as {0}_max", col.field_name));
        }
    }

    if select_parts.is_empty() {
        return Ok(json!({}));
    }

    let sql = format!(
        "SELECT {} FROM vitals WHERE start_date >= ? AND start_date <= ?",
        select_parts.join(", ")
    );

    let row = sqlx::query(&sql)
        .bind(start)
        .bind(end)
        .fetch_one(pool)
        .await?;

    let mut map = Map::new();
    for col in row.columns() {
        let name = col.name();
        if let Ok(val) = row.try_get::<f64, _>(name) {
            map.insert(name.to_string(), json!(val));
        } else {
            map.insert(name.to_string(), Value::Null);
        }
    }

    Ok(Value::Object(map))
}

pub async fn get_recovery_analysis(
    pool: &DbPool,
) -> Result<Value> {
    // 1. Get 7-day HRV Baseline
    let baseline_hrv: (f64,) = sqlx::query_as(
        "SELECT AVG(hrv_sdnn) FROM vitals WHERE hrv_sdnn > 0 AND start_date >= date('now', '-7 days')"
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0.0,));

    // 2. Get Last 24h HRV
    let current_hrv: (f64,) = sqlx::query_as(
        "SELECT AVG(hrv_sdnn) FROM vitals WHERE hrv_sdnn > 0 AND start_date >= date('now', '-1 day')"
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0.0,));

    // 3. Get RHR Baseline vs Current
    let baseline_rhr: (f64,) = sqlx::query_as(
        "SELECT AVG(resting_hr) FROM vitals WHERE resting_hr > 0 AND start_date >= date('now', '-7 days')"
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0.0,));

    let current_rhr: (f64,) = sqlx::query_as(
        "SELECT AVG(resting_hr) FROM vitals WHERE resting_hr > 0 AND start_date >= date('now', '-1 day')"
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0.0,));

    // Simple Recovery Logic
    let mut score = 0.0;
    if baseline_hrv.0 > 0.0 {
        score = (current_hrv.0 / baseline_hrv.0) * 100.0;
    }

    // Penalize if RHR is elevated
    if current_rhr.0 > baseline_rhr.0 && baseline_rhr.0 > 0.0 {
        let diff = current_rhr.0 - baseline_rhr.0;
        score -= diff * 5.0; // Subtract 5 points per beat of elevation
    }

    let final_score = score.clamp(0.0, 100.0).round() as i32;

    let status = match final_score {
        s if s > 80 => "Optimal",
        s if s > 50 => "Good",
        s if s > 30 => "Strained",
        _ => "Recovery Needed",
    };

    Ok(json!({
        "recovery_score": final_score,
        "status": status,
        "metrics": {
            "hrv_baseline": baseline_hrv.0,
            "hrv_current": current_hrv.0,
            "rhr_baseline": baseline_rhr.0,
            "rhr_current": current_rhr.0
        }
    }))
}

pub async fn get_sleep_summary(
    pool: &DbPool,
    date: &str, // YYYY-MM-DD
) -> Result<Value> {
    // 1. Fetch all sleep records for the window (e.g. 6PM previous day to 12PM current day)
    // For simplicity, we'll just use the provided date string as a start_date prefix
    let sql = "SELECT sleep_stage, start_date, end_date FROM sleep WHERE start_date LIKE ? ORDER BY start_date ASC";
    
    let rows = sqlx::query(sql)
        .bind(format!("{}%", date))
        .fetch_all(pool)
        .await?;

    let mut staging_seconds = HashMap::new();
    let mut total_seconds = 0.0;

    for row in rows {
        let stage: i64 = row.get("sleep_stage");
        let start: String = row.get("start_date");
        let end: String = row.get("end_date");

        if let (Ok(s_dt), Ok(e_dt)) = (DateTime::parse_from_rfc3339(&start), DateTime::parse_from_rfc3339(&end)) {
            let duration = e_dt.signed_duration_since(s_dt).num_seconds() as f64;
            let stage_name = match stage {
                0 => "In Bed",
                1 => "Asleep",
                2 => "Awake",
                3 => "Core",
                4 => "Deep",
                5 => "REM",
                _ => "Unknown",
            };
            
            *staging_seconds.entry(stage_name.to_string()).or_insert(0.0) += duration;
            total_seconds += duration;
        }
    }

    Ok(json!({
        "date": date,
        "total_sleep_hours": total_seconds / 3600.0,
        "breakdown": staging_seconds
    }))
}

async fn ensure_indices(pool: &DbPool, manifest: &Manifest) -> Result<()> {
    for table_name in manifest.tables.keys() {
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_start_date ON {} (start_date)",
            table_name, table_name
        );
        let _ = sqlx::query(&sql).execute(pool).await;
    }
    Ok(())
}

async fn ensure_external_schema(pool: &DbPool, manifest: &Manifest) -> Result<()> {
    if let Some(ext) = &manifest.external_sources {
        if let Some(ecg) = &ext.ecg {
            let mut cols = vec![
                "id INTEGER PRIMARY KEY AUTOINCREMENT".to_string(),
                "file_name TEXT UNIQUE".to_string(),
                "sample_count INTEGER".to_string(),
                "mean_voltage REAL".to_string(),
                "calculated_hr REAL".to_string(),
            ];
            for m in &ecg.metadata_map {
                cols.push(format!("{} {}", m.db_column, m.data_type));
            }
            cols.push(format!("{} {}", ecg.payload.db_column, ecg.payload.data_type));

            let sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", ecg.target_table, cols.join(", "));
            sqlx::query(&sql).execute(pool).await?;
        }

        if let Some(routes) = &ext.routes {
            let mut cols = vec![
                "id INTEGER PRIMARY KEY AUTOINCREMENT".to_string(),
                "file_name TEXT".to_string(),
            ];
            for c in &routes.columns {
                cols.push(format!("{} {}", c.db_column, c.data_type));
            }

            let sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", routes.target_table, cols.join(", "));
            sqlx::query(&sql).execute(pool).await?;
            
            let idx_sql = format!("CREATE INDEX IF NOT EXISTS idx_{}_ts ON {} (timestamp)", routes.target_table, routes.target_table);
            let _ = sqlx::query(&idx_sql).execute(pool).await;
        }
    }
    Ok(())
}

async fn ensure_schema(pool: &DbPool, manifest: &Manifest) -> Result<()> {
    for (table_name, table_config) in &manifest.tables {
        let pk_col = table_config.columns.iter().find(|c| c.is_primary_key);
        
        let create_sql = if let Some(pk) = pk_col {
            format!(
                "CREATE TABLE IF NOT EXISTS {} ({} {} PRIMARY KEY, creation_date TEXT, start_date TEXT, end_date TEXT)",
                table_name, pk.field_name, pk.data_type
            )
        } else {
            format!(
                "CREATE TABLE IF NOT EXISTS {} (uuid TEXT PRIMARY KEY, creation_date TEXT, start_date TEXT, end_date TEXT)",
                table_name
            )
        };

        sqlx::query(&create_sql)
            .execute(pool)
            .await
            .with_context(|| format!("Failed to create base table {}", table_name))?;

        let query_sql = format!("PRAGMA table_info({})", table_name);
        let rows = sqlx::query(&query_sql)
            .fetch_all(pool)
            .await
            .with_context(|| format!("Failed to fetch table info for {}", table_name))?;

        let existing_columns: HashSet<String> = rows
            .iter()
            .map(|row| row.get::<String, _>("name"))
            .collect();

        for col_def in &table_config.columns {
            if !existing_columns.contains(&col_def.field_name) {
                info!(
                    "Adding new column to {} table: {} ({})",
                    table_name, col_def.field_name, col_def.data_type
                );

                let sql = if let Some(expr) = &col_def.expression {
                    format!(
                        "ALTER TABLE {} ADD COLUMN {} {} GENERATED ALWAYS AS ({}) VIRTUAL",
                        table_name, col_def.field_name, col_def.data_type, expr
                    )
                } else {
                    format!(
                        "ALTER TABLE {} ADD COLUMN {} {}",
                        table_name, col_def.field_name, col_def.data_type
                    )
                };

                sqlx::query(&sql).execute(pool).await.with_context(|| {
                    format!(
                        "Failed to add column {} to table {}",
                        col_def.field_name, table_name
                    )
                })?;
            }
        }
    }

    Ok(())
}
