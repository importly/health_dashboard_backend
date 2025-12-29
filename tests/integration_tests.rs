use backend::{db, parser};
use std::fs;
use std::path::Path;

#[tokio::test]
async fn test_end_to_end_ingestion() -> anyhow::Result<()> {
    // Setup
    let test_dir = "target/tmp_test";
    if Path::new(test_dir).exists() {
        fs::remove_dir_all(test_dir)?;
    }
    fs::create_dir_all(test_dir)?;

    let db_url = format!("sqlite:{}/test.db?mode=rwc", test_dir);
    let manifest_path = format!("{}/manifest.toml", test_dir);
    let xml_path = format!("{}/export.xml", test_dir);

    // Create Manifest
    let manifest_content = r#"
[tables.records]
columns = [
    { name = "heart_rate", hk_type = "HKQuantityTypeIdentifierHeartRate", aggregate = "avg", data_type = "REAL" },
    { name = "step_count", hk_type = "HKQuantityTypeIdentifierStepCount", aggregate = "sum", data_type = "INTEGER" }
]
"#;
    fs::write(&manifest_path, manifest_content)?;

    // Create Dummy XML
    let xml_content = r#"
<HealthData>
 <Record type="HKQuantityTypeIdentifierHeartRate" creationDate="2024-01-01 10:00:00 -0500" startDate="2024-01-01 10:00:00 -0500" endDate="2024-01-01 10:01:00 -0500" value="60"/>
 <Record type="HKQuantityTypeIdentifierHeartRate" creationDate="2024-01-01 10:05:00 -0500" startDate="2024-01-01 10:05:00 -0500" endDate="2024-01-01 10:06:00 -0500" value="80"/>
 <Record type="HKQuantityTypeIdentifierStepCount" creationDate="2024-01-01 10:10:00 -0500" startDate="2024-01-01 10:10:00 -0500" endDate="2024-01-01 10:15:00 -0500" value="500"/>
</HealthData>
"#;
    fs::write(&xml_path, xml_content)?;

    // Initialize DB
    let (pool, manifest) = db::init_db(&db_url, &manifest_path).await?;

    // Ingest
    let count = parser::parse_and_ingest(Path::new(&xml_path), &pool, &manifest, None::<fn(usize)>).await?;
    assert_eq!(count, 3);

    // Verify Data
    let records = db::query_table(&pool, "records", 100, None, None, None).await?;
    assert_eq!(records.len(), 3);

        // Verify Aggregation (Hourly)
        // 10:00 EST is 15:00 UTC. 
        // HR: (60+80)/2 = 70. Steps: 500.
        let agg = db::aggregate_table(&pool, &manifest, "records", "hour", None, None).await?;
    // Check if we have the bucket
    // Note: Result is a Vec<Value>. We need to find the right bucket.
    // Since we only have one hour of data, it should be the first row.
    let first_row = &agg[0];
    let bucket = first_row
        .get("time_bucket")
        .and_then(|v| v.as_str())
        .unwrap();
    // 2024-01-01 10:00 -0500 -> 15:00 UTC
    assert_eq!(bucket, "2024-01-01T15:00:00Z");

    let hr = first_row
        .get("heart_rate")
        .and_then(|v| v.as_f64())
        .unwrap();
    assert_eq!(hr, 70.0);

    let steps = first_row
        .get("step_count")
        .and_then(|v| v.as_i64())
        .unwrap();
    assert_eq!(steps, 500);

    // Cleanup
    pool.close().await;
    // fs::remove_dir_all(test_dir)?; // Optional: keep for inspection if failed

    Ok(())
}
