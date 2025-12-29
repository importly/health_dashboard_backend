use backend::{db, importer, parser};
use sqlx::Row;
use std::fs;
use std::path::Path;

#[tokio::test]
async fn test_sample_export_ingestion() -> anyhow::Result<()> {
    // Setup
    let test_db_path = "target/test_sample_export.db";
    let db_url = format!("sqlite:{}?mode=rwc", test_db_path);

    // Clean up previous run
    if Path::new(test_db_path).exists() {
        fs::remove_file(test_db_path)?;
    }

    // Use the real manifest
    let manifest_path = "metrics_manifest.toml";

    // Initialize DB
    let (pool, manifest) = db::init_db(&db_url, manifest_path).await?;

    // 1. Ingest Main XML
    let xml_path = Path::new("tests/sample_export/export.xml");
    println!("Ingesting XML from {:?}", xml_path);

    let count = parser::parse_and_ingest(xml_path, &pool, &manifest, None::<fn(usize)>).await?;
    println!("Processed {} records", count);
    assert!(count > 0, "Should process records from sample export");

    // 2. Ingest External Files (ECG, Routes)
    let base_dir = Path::new("tests/sample_export");
    println!("Scanning external files in {:?}", base_dir);
    importer::run_external_import(base_dir, &pool, &manifest).await?;

    // 3. Verification

    // Check Vitals (Heart Rate samples should be present)
    let vitals_count: i64 = sqlx::query("SELECT count(*) FROM vitals")
        .fetch_one(&pool)
        .await?
        .get(0);
    println!("Vitals records found: {}", vitals_count);
    assert!(vitals_count > 0);

    // Check ECG
    let ecg_count: i64 = sqlx::query("SELECT count(*) FROM ecg_recordings")
        .fetch_one(&pool)
        .await?
        .get(0);
    println!("ECGs found: {}", ecg_count);
    assert!(ecg_count > 0);

    // Check Routes
    let route_count: i64 = sqlx::query("SELECT count(*) FROM route_points")
        .fetch_one(&pool)
        .await?
        .get(0);
    println!("Route points found: {}", route_count);
    assert!(route_count > 0);

    pool.close().await;
    Ok(())
}
