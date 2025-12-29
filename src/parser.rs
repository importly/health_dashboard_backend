use crate::db::{DbPool, Manifest};
use chrono::{DateTime, Utc};
use quick_xml::events::{BytesStart, Event};
use quick_xml::reader::Reader;
use sha2::Digest;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use tracing::{error, info};

pub struct DataPoint {
    pub table_name: String,
    pub columns: HashMap<String, String>, // column_name -> value
}

pub async fn parse_and_ingest(
    file_path: &Path,
    pool: &DbPool,
    manifest: &Manifest,
    on_progress: Option<impl Fn(usize) + Send + Sync>,
) -> anyhow::Result<usize> {
    let batch_size = manifest
        .settings
        .as_ref()
        .and_then(|s| s.batch_size)
        .unwrap_or(5000);

    let file = File::open(file_path)?;
    let file_reader = BufReader::new(file);
    let mut reader = Reader::from_reader(file_reader);
    reader.config_mut().trim_text(true);

    let mut table_buffers: HashMap<String, Vec<DataPoint>> = HashMap::new();
    let mut total_count = 0;

    // Pre-process manifest for quick lookup
    let mut record_map: HashMap<String, (String, String)> = HashMap::new();
    for (table_name, config) in &manifest.tables {
        for col in &config.columns {
            if let Some(hk_id) = &col.hk_identifier {
                if col.extraction_source.is_none()
                    || col.extraction_source.as_deref() == Some("value")
                {
                    record_map.insert(hk_id.clone(), (table_name.clone(), col.field_name.clone()));
                }
            }
            table_buffers
                .entry(table_name.clone())
                .or_insert_with(|| Vec::with_capacity(batch_size));
        }
    }

    info!("Starting streaming parse of {:?}", file_path);

    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Empty(e)) => {
                let name = e.name();
                if name.as_ref() == b"Record" {
                    if let Some(dp) = extract_record_data(&e, &record_map) {
                        if let Some(buffer) = table_buffers.get_mut(&dp.table_name) {
                            buffer.push(dp);
                        }
                    }
                } else if name.as_ref() == b"ActivitySummary" {
                    let mut summary_data = HashMap::new();
                    for attr in e.attributes() {
                        let attr = attr?;
                        let key = String::from_utf8_lossy(attr.key.as_ref());
                        let val = String::from_utf8_lossy(attr.value.as_ref()).to_string();

                        if let Some(config) = manifest.tables.get("activity_summaries") {
                            for col in &config.columns {
                                if col.hk_attribute.as_ref() == Some(&key.to_string()) {
                                    summary_data.insert(col.field_name.clone(), val.clone());
                                }
                            }
                        }
                    }
                    if let Some(buffer) = table_buffers.get_mut("activity_summaries") {
                        buffer.push(DataPoint {
                            table_name: "activity_summaries".to_string(),
                            columns: summary_data,
                        });
                    }
                }
            }
            Ok(Event::Start(e)) => {
                let name = e.name();
                if name.as_ref() == b"Record" {
                    // Non-empty Record (has children like MetadataEntry)
                    if let Some(dp) = extract_record_data(&e, &record_map) {
                        if let Some(buffer) = table_buffers.get_mut(&dp.table_name) {
                            buffer.push(dp);
                        }
                    }
                    // Skip children for now as they are not mapped in manifest for standard records
                    reader.read_to_end_into(e.to_end().name(), &mut Vec::new())?;
                } else if name.as_ref() == b"Workout" {
                    let mut workout_data = HashMap::new();
                    let mut start_date_raw = String::new();
                    let mut end_date_raw = String::new();
                    let mut creation_date_raw = String::new();

                    for attr in e.attributes() {
                        let attr = attr?;
                        let key = String::from_utf8_lossy(attr.key.as_ref());
                        let val = String::from_utf8_lossy(attr.value.as_ref()).to_string();

                        match key.as_ref() {
                            "startDate" => start_date_raw = val.clone(),
                            "endDate" => end_date_raw = val.clone(),
                            "creationDate" => creation_date_raw = val.clone(),
                            _ => {}
                        }

                        for (table_name, config) in &manifest.tables {
                            if table_name == "workouts" {
                                for col in &config.columns {
                                    if col.extraction_source.as_deref() == Some("attribute") {
                                        if let Some(hk_attr) = &col.hk_attribute {
                                            if hk_attr == &key {
                                                workout_data
                                                    .insert(col.field_name.clone(), val.clone());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    workout_data.insert("start_date".to_string(), normalize_date(&start_date_raw));
                    workout_data.insert("end_date".to_string(), normalize_date(&end_date_raw));
                    workout_data.insert(
                        "creation_date".to_string(),
                        normalize_date(&creation_date_raw),
                    );

                    let mut child_buf = Vec::new();
                    loop {
                        match reader.read_event_into(&mut child_buf) {
                            Ok(Event::Empty(ce)) => {
                                let cname = ce.name();
                                if cname.as_ref() == b"WorkoutStatistics" {
                                    let mut stat_type = String::new();
                                    let mut stat_sum = String::new();
                                    for attr in ce.attributes() {
                                        let attr = attr?;
                                        match attr.key.as_ref() {
                                            b"type" => {
                                                stat_type =
                                                    String::from_utf8_lossy(&attr.value).to_string()
                                            }
                                            b"sum" => {
                                                stat_sum =
                                                    String::from_utf8_lossy(&attr.value).to_string()
                                            }
                                            _ => {}
                                        }
                                    }
                                    for col in &manifest.tables["workouts"].columns {
                                        if col.extraction_source.as_deref()
                                            == Some("statistics_sum")
                                            && col.hk_identifier.as_ref() == Some(&stat_type)
                                        {
                                            workout_data
                                                .insert(col.field_name.clone(), stat_sum.clone());
                                        }
                                    }
                                } else if cname.as_ref() == b"MetadataEntry" {
                                    let mut mkey = String::new();
                                    let mut mval = String::new();
                                    for attr in ce.attributes() {
                                        let attr = attr?;
                                        match attr.key.as_ref() {
                                            b"key" => {
                                                mkey =
                                                    String::from_utf8_lossy(&attr.value).to_string()
                                            }
                                            b"value" => {
                                                mval =
                                                    String::from_utf8_lossy(&attr.value).to_string()
                                            }
                                            _ => {}
                                        }
                                    }
                                    for col in &manifest.tables["workouts"].columns {
                                        if col.extraction_source.as_deref()
                                            == Some("metadata_value")
                                            && col.hk_identifier.as_ref() == Some(&mkey)
                                        {
                                            workout_data
                                                .insert(col.field_name.clone(), mval.clone());
                                        }
                                    }
                                } else if cname.as_ref() == b"FileReference" {
                                    for attr in ce.attributes() {
                                        let attr = attr?;
                                        if attr.key.as_ref() == b"path" {
                                            let path_val =
                                                String::from_utf8_lossy(&attr.value).to_string();
                                            let file_name = Path::new(&path_val)
                                                .file_name()
                                                .unwrap_or_default()
                                                .to_string_lossy()
                                                .to_string();
                                            for col in &manifest.tables["workouts"].columns {
                                                if col.extraction_source.as_deref()
                                                    == Some("route_ref")
                                                {
                                                    workout_data.insert(
                                                        col.field_name.clone(),
                                                        file_name.clone(),
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(Event::Start(ce)) if ce.name().as_ref() == b"WorkoutRoute" => {}
                            Ok(Event::End(ce)) if ce.name().as_ref() == b"Workout" => break,
                            Ok(Event::Eof) => break,
                            _ => {}
                        }
                        child_buf.clear();
                    }

                    if let Some(buffer) = table_buffers.get_mut("workouts") {
                        buffer.push(DataPoint {
                            table_name: "workouts".to_string(),
                            columns: workout_data,
                        });
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                error!("Error at position {}: {:?}", reader.buffer_position(), e);
                break;
            }
            _ => (),
        }

        // Check buffer sizes
        let mut needs_flush = false;
        for buffer in table_buffers.values() {
            if buffer.len() >= batch_size {
                needs_flush = true;
                break;
            }
        }

        if needs_flush {
            let mut batch_count = 0;
            for buffer in table_buffers.values() {
                batch_count += buffer.len();
            }
            total_count += batch_count;
            flush_buffers(&mut table_buffers, pool).await?;
            info!("Processed {} records...", total_count);
            if let Some(ref cb) = on_progress {
                cb(total_count);
            }
        }
        buf.clear();
    }

    // Final flush
    let mut final_count = 0;
    for buffer in table_buffers.values() {
        final_count += buffer.len();
    }
    if final_count > 0 {
        total_count += final_count;
        flush_buffers(&mut table_buffers, pool).await?;
    }

    info!("Finished processing. Total records: {}", total_count);
    Ok(total_count)
}

fn extract_record_data(
    e: &BytesStart,
    record_map: &HashMap<String, (String, String)>,
) -> Option<DataPoint> {
    let mut hk_type = String::new();
    let mut value = String::new();
    let mut creation_date = String::new();
    let mut start_date = String::new();
    let mut end_date = String::new();

    for attr in e.attributes().flatten() {
        let key = attr.key.as_ref();
        let val = String::from_utf8_lossy(&attr.value);
        match key {
            b"type" => hk_type = val.to_string(),
            b"value" => value = val.to_string(),
            b"creationDate" => creation_date = normalize_date(&val),
            b"startDate" => start_date = normalize_date(&val),
            b"endDate" => end_date = normalize_date(&val),
            _ => {}
        }
    }

    if let Some((table_name, col_name)) = record_map.get(&hk_type) {
        // Content-based ID for deduplication
        let mut hasher = sha2::Sha256::new();
        sha2::Digest::update(&mut hasher, table_name.as_bytes());
        sha2::Digest::update(&mut hasher, col_name.as_bytes());
        sha2::Digest::update(&mut hasher, start_date.as_bytes());
        sha2::Digest::update(&mut hasher, end_date.as_bytes());
        sha2::Digest::update(&mut hasher, value.as_bytes());
        let hash_id = format!("{:x}", sha2::Digest::finalize(hasher));

        let mut columns = HashMap::new();
        columns.insert("uuid".to_string(), hash_id);
        columns.insert("creation_date".to_string(), creation_date);
        columns.insert("start_date".to_string(), start_date);
        columns.insert("end_date".to_string(), end_date);
        columns.insert(col_name.clone(), value);

        Some(DataPoint {
            table_name: table_name.clone(),
            columns,
        })
    } else {
        None
    }
}

fn normalize_date(input: &str) -> String {
    match DateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S %z") {
        Ok(dt) => dt.with_timezone(&Utc).to_rfc3339(),
        Err(_) => input.to_string(),
    }
}

async fn flush_buffers(
    table_buffers: &mut HashMap<String, Vec<DataPoint>>,
    pool: &DbPool,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;

    for (table_name, records) in table_buffers.iter_mut() {
        for record in records.iter() {
            let mut col_names = Vec::new();
            let mut placeholders = Vec::new();
            let mut values = Vec::new();

            for (col, val) in &record.columns {
                col_names.push(col.clone());
                placeholders.push("?");
                values.push(val.clone());
            }

            let query = format!(
                "INSERT OR IGNORE INTO {} ({}) VALUES ({})",
                table_name,
                col_names.join(", "),
                placeholders.join(", ")
            );

            let mut q = sqlx::query(&query);
            for val in values {
                q = q.bind(val);
            }
            q.execute(&mut *tx).await?;
        }
        records.clear();
    }

    tx.commit().await?;
    Ok(())
}
