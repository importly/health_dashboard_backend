use crate::db::{DbPool, Manifest};
use anyhow::Result;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::fs;
use std::io::BufReader;
use std::path::Path;
use tracing::{error, info};

pub async fn run_external_import(
    base_dir: &Path,
    pool: &DbPool,
    manifest: &Manifest,
) -> Result<()> {
    let ext = match &manifest.external_sources {
        Some(e) => e,
        None => return Ok(()),
    };

    if let Some(ecg_cfg) = &ext.ecg {
        let folder_path = base_dir.join(&ecg_cfg.folder);
        if folder_path.exists() {
            import_ecgs(&folder_path, ecg_cfg, pool).await?;
        }
    }

    if let Some(route_cfg) = &ext.routes {
        let folder_path = base_dir.join(&route_cfg.folder);
        if folder_path.exists() {
            import_routes(&folder_path, route_cfg, pool, manifest).await?;
        }
    }

    Ok(())
}

async fn import_ecgs(folder: &Path, cfg: &crate::db::EcgConfig, pool: &DbPool) -> Result<()> {
    info!("Scanning for ECGs in {:?}", folder);
    let entries = fs::read_dir(folder)?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("csv") {
            let file_name = path.file_name().unwrap().to_string_lossy().to_string();

            let exists: (i64,) = sqlx::query_as(&format!(
                "SELECT COUNT(*) FROM {} WHERE file_name = ?",
                cfg.target_table
            ))
            .bind(&file_name)
            .fetch_one(pool)
            .await?;

            if exists.0 > 0 {
                continue;
            }

            match process_single_ecg(&path, cfg, pool).await {
                Ok(_) => info!("Successfully imported ECG: {}", file_name),
                Err(e) => error!("Failed to import ECG {}: {:?}", file_name, e),
            }
        }
    }
    Ok(())
}

async fn process_single_ecg(path: &Path, cfg: &crate::db::EcgConfig, pool: &DbPool) -> Result<()> {
    let content = fs::read_to_string(path)?;
    let lines: Vec<&str> = content.lines().collect();

    let mut metadata = std::collections::HashMap::new();
    let mut samples = Vec::new();
    let mut in_samples = false;

    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        // Header section parsing
        if !in_samples {
            // Check for metadata keys from config
            let mut found_meta = false;
            for m in &cfg.metadata_map {
                if line.starts_with(&m.csv_key) {
                    if let Some((_, val)) = line.split_once(',') {
                        metadata
                            .insert(m.csv_key.clone(), val.trim_matches('"').trim().to_string());
                        found_meta = true;
                        break;
                    }
                }
            }

            // Lead/Unit lines often mark the end of metadata
            if line.starts_with("Lead,") || line.starts_with("Unit,") {
                continue;
            }

            // If we hit a number or a minus sign at the start of a line after some headers, it's likely a sample
            if !found_meta
                && !line.is_empty()
                && (line.chars().next().unwrap().is_ascii_digit() || line.starts_with('-'))
            {
                in_samples = true;
                samples.push(line.to_string());
            }
        } else {
            // Sample data section
            samples.push(line.to_string());
        }
    }

    let payload = samples.join(",");
    let file_name = path.file_name().unwrap().to_string_lossy().to_string();

    // Calculate derived metrics
    let numeric_samples: Vec<f64> = samples
        .iter()
        .filter_map(|s| s.parse::<f64>().ok())
        .collect();
    let sample_count = numeric_samples.len();
    let mean_voltage = if sample_count > 0 {
        numeric_samples.iter().sum::<f64>() / sample_count as f64
    } else {
        0.0
    };

    // Calculate HR from ECG
    let sample_rate_hz = metadata
        .get("Sample Rate")
        .and_then(|s| s.split_whitespace().next())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(512.0);

    let calculated_hr = calculate_ecg_hr(&numeric_samples, sample_rate_hz);

    let mut col_names = vec![
        "file_name".to_string(),
        "sample_count".to_string(),
        "mean_voltage".to_string(),
        "calculated_hr".to_string(),
    ];
    let mut values = vec![
        file_name,
        sample_count.to_string(),
        mean_voltage.to_string(),
        calculated_hr.to_string(),
    ];

    for m in &cfg.metadata_map {
        col_names.push(m.db_column.clone());
        values.push(metadata.get(&m.csv_key).cloned().unwrap_or_default());
    }
    col_names.push(cfg.payload.db_column.clone());
    values.push(payload);

    let placeholders: Vec<String> = (1..=col_names.len()).map(|_| "?".to_string()).collect();
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        cfg.target_table,
        col_names.join(", "),
        placeholders.join(", ")
    );

    let mut q = sqlx::query(&sql);
    for v in values {
        q = q.bind(v);
    }
    q.execute(pool).await?;

    Ok(())
}

async fn import_routes(
    folder: &Path,
    cfg: &crate::db::RouteConfig,
    pool: &DbPool,
    manifest: &Manifest,
) -> Result<()> {
    info!("Scanning for Routes in {:?}", folder);
    let entries = fs::read_dir(folder)?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("gpx") {
            let file_name = path.file_name().unwrap().to_string_lossy().to_string();

            let exists: (i64,) = sqlx::query_as(&format!(
                "SELECT COUNT(*) FROM {} WHERE file_name = ?",
                cfg.target_table
            ))
            .bind(&file_name)
            .fetch_one(pool)
            .await?;

            if exists.0 > 0 {
                continue;
            }

            match process_single_route(&path, cfg, pool, manifest).await {
                Ok(_) => info!("Successfully imported Route: {}", file_name),
                Err(e) => error!("Failed to import Route {}: {:?}", file_name, e),
            }
        }
    }
    Ok(())
}

async fn process_single_route(
    path: &Path,
    cfg: &crate::db::RouteConfig,
    pool: &DbPool,
    manifest: &Manifest,
) -> Result<()> {
    let batch_size = manifest
        .settings
        .as_ref()
        .and_then(|s| s.batch_size)
        .unwrap_or(5000);

    let file = fs::File::open(path)?;
    let mut reader = Reader::from_reader(BufReader::new(file));
    let mut buf = Vec::new();
    let file_name = path.file_name().unwrap().to_string_lossy().to_string();

    let mut point_buffer = Vec::with_capacity(batch_size);
    let mut current_point: Option<std::collections::HashMap<String, String>> = None;
    let mut current_tag = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if name == "trkpt" {
                    let mut map = std::collections::HashMap::new();
                    for attr in e.attributes() {
                        let attr = attr?;
                        let k = String::from_utf8_lossy(attr.key.as_ref());
                        let v = String::from_utf8_lossy(attr.value.as_ref()).to_string();
                        if k == "lat" {
                            map.insert("lat".to_string(), v.clone());
                        }
                        if k == "lon" {
                            map.insert("lon".to_string(), v);
                        }
                    }
                    current_point = Some(map);
                }
                current_tag = name;
            }
            Ok(Event::Text(e)) => {
                if let Some(ref mut p) = current_point {
                    let val = e.unescape()?.to_string();
                    p.insert(current_tag.clone(), val);
                }
            }
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if name == "trkpt" {
                    if let Some(p) = current_point.take() {
                        point_buffer.push(p);
                    }
                }
                current_tag = String::new();
            }
            Ok(Event::Eof) => break,
            _ => {}
        }

        if point_buffer.len() >= batch_size {
            flush_route_points(&file_name, &point_buffer, cfg, pool).await?;
            point_buffer.clear();
        }
        buf.clear();
    }

    if !point_buffer.is_empty() {
        flush_route_points(&file_name, &point_buffer, cfg, pool).await?;
    }

    Ok(())
}

async fn flush_route_points(
    file_name: &str,
    points: &[std::collections::HashMap<String, String>],
    cfg: &crate::db::RouteConfig,
    pool: &DbPool,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    for p in points {
        let mut col_names = vec!["file_name".to_string()];
        let mut values = vec![file_name.to_string()];

        for c in &cfg.columns {
            col_names.push(c.db_column.clone());
            values.push(p.get(&c.xml_tag).cloned().unwrap_or_default());
        }

        let placeholders: Vec<String> = (1..=col_names.len()).map(|_| "?".to_string()).collect();
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            cfg.target_table,
            col_names.join(", "),
            placeholders.join(", ")
        );

        let mut q = sqlx::query(&sql);
        for v in values {
            q = q.bind(v);
        }
        q.execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

fn calculate_ecg_hr(samples: &[f64], sample_rate: f64) -> f64 {
    if samples.is_empty() || sample_rate <= 0.0 {
        return 0.0;
    }

    // Simple peak detection (Threshold + refractory period)
    // 1. Determine threshold (e.g., 75th percentile or mean + offset)
    let max = samples.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let threshold = mean + (max - mean) * 0.6; // Heuristic for R-peak

    let mut peak_indices = Vec::new();
    let refractory_samples = (0.2 * sample_rate) as usize; // 200ms refractory period (max ~300bpm)
    let mut last_peak = 0;

    for (i, &val) in samples.iter().enumerate() {
        if val > threshold && (i - last_peak > refractory_samples || last_peak == 0) {
            peak_indices.push(i);
            last_peak = i;
        }
    }

    if peak_indices.len() < 2 {
        return 0.0;
    }

    // 2. Calculate RR-intervals in seconds
    let mut rr_intervals = Vec::new();
    for window in peak_indices.windows(2) {
        let diff_samples = window[1] - window[0];
        rr_intervals.push(diff_samples as f64 / sample_rate);
    }

    // 3. Average HR = 60 / avg_rr
    let avg_rr = rr_intervals.iter().sum::<f64>() / rr_intervals.len() as f64;
    if avg_rr > 0.0 {
        60.0 / avg_rr
    } else {
        0.0
    }
}
