use backend::{db, importer, parser};
use chrono::{Duration, TimeZone, Utc};
use sqlx::Row;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

#[tokio::test]
async fn test_sample_export_ingestion() -> anyhow::Result<()> {
    // Setup Paths
    let base_dir = Path::new("target/test_data_gen");
    if base_dir.exists() {
        fs::remove_dir_all(base_dir)?;
    }
    fs::create_dir_all(base_dir)?;

    let xml_path = base_dir.join("export.xml");
    let ecg_dir = base_dir.join("electrocardiograms");
    let route_dir = base_dir.join("workout-routes");
    fs::create_dir_all(&ecg_dir)?;
    fs::create_dir_all(&route_dir)?;

    // 1. Generate Minimal export.xml
    {
        println!("Generating temporary export.xml at {:?}", xml_path);
        let file = File::create(&xml_path)?;
        let mut writer = BufWriter::new(file);
        writeln!(writer, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")?;
        writeln!(writer, "<HealthData locale=\"en_US\">")?;
        writeln!(writer, " <ExportDate value=\"2024-01-01 12:00:00 -0500\"/>")?;

        let start_time = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        // Generate 50 records
        for i in 0..50 {
            let timestamp = start_time + Duration::minutes(i);
            let timestamp_str = timestamp.format("%Y-%m-%d %H:%M:%S %z").to_string();
            // Heart Rate
            let hr = 60.0 + (i as f64);
            writeln!(
                writer,
                " <Record type=\"HKQuantityTypeIdentifierHeartRate\" sourceName=\"Generator\" unit=\"count/min\" creationDate=\"{0}\" startDate=\"{0}\" endDate=\"{0}\" value=\"{1:.1}\" />",
                timestamp_str, hr
            )?;
        }
        writeln!(writer, "</HealthData>")?;
    }

    // 2. Generate Minimal ECG CSV
    {
        let ecg_path = ecg_dir.join("sample_ecg.csv");
        println!("Generating temporary ECG at {:?}", ecg_path);
        let file = File::create(&ecg_path)?;
        let mut writer = BufWriter::new(file);
        // Headers matching metrics_manifest.toml
        writeln!(
            writer,
            "Recorded Date,Sample Rate,Classification,Device,Lead,Unit"
        )?;
        writeln!(writer, "\"2024-01-01T12:00:00Z\",\"512 Hz\",\"Sinus Rhythm\",\"Apple Watch\",\"1\",\"microvolts\"")?;
        // Some sample data
        for i in 0..10 {
            writeln!(writer, "{}", (i as f64).sin())?;
        }
    }

    // 3. Generate Minimal GPX Route
    {
        let route_path = route_dir.join("sample_route.gpx");
        println!("Generating temporary Route at {:?}", route_path);
        let file = File::create(&route_path)?;
        let mut writer = BufWriter::new(file);
        writeln!(writer, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")?;
        writeln!(writer, "<gpx version=\"1.1\" creator=\"HealthTracking\">")?;
        writeln!(writer, "<trk><name>Test Route</name><trkseg>")?;
        // Single point
        writeln!(writer, "<trkpt lat=\"37.7749\" lon=\"-122.4194\"><ele>10.0</ele><time>2024-01-01T12:00:00Z</time></trkpt>")?;
        writeln!(writer, "</trkseg></trk></gpx>")?;
    }

    // Initialize DB
    let test_db_path = "target/test_sample_export.db";
    let db_url = format!("sqlite:{}?mode=rwc", test_db_path);
    if Path::new(test_db_path).exists() {
        fs::remove_file(test_db_path)?;
    }

    // Use the real manifest (assumed to exist in project root)
    let manifest_path = "metrics_manifest.toml";
    let (pool, manifest) = db::init_db(&db_url, manifest_path).await?;

    // 4. Ingest Main XML
    println!("Ingesting XML...");
    let count = parser::parse_and_ingest(&xml_path, &pool, &manifest, None::<fn(usize)>).await?;
    println!("Processed {} records", count);
    assert!(count > 0, "Should process records from generated export");

    // 5. Ingest External Files
    println!("Scanning external files in {:?}", base_dir);
    importer::run_external_import(base_dir, &pool, &manifest).await?;

    // 6. Verification

    // Check Vitals
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
