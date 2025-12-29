use std::fs::File;
use std::io::{BufWriter, Write};
use chrono::{Duration, TimeZone, Utc};

fn main() -> std::io::Result<()> {
    let file_path = "large_export.xml";
    println!("Generating {} with 100,000 records...", file_path);

    let file = File::create(file_path)?;
    let mut writer = BufWriter::new(file);

    writeln!(writer, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")?;
    writeln!(writer, "<HealthData locale=\"en_US\">")?;
    writeln!(writer, " <ExportDate value=\"2024-01-01 12:00:00 -0500\"/>")?;

    let start_time = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

    for i in 0..100_000 {
        let timestamp = start_time + Duration::minutes(i);
        let timestamp_str = timestamp.format("%Y-%m-%d %H:%M:%S %z").to_string();
        
        // Alternate between HeartRate and Steps to fill multiple tables
        if i % 2 == 0 {
            // Heart Rate: 60 + sin wave variation
            let hr = 60.0 + (i as f64 * 0.1).sin() * 20.0 + 20.0;
            writeln!(
                writer,
                " <Record type=\"HKQuantityTypeIdentifierHeartRate\" sourceName=\"Generator\" unit=\"count/min\" creationDate=\"{0}\" startDate=\"{0}\" endDate=\"{0}\" value=\"{1:.1}\" />",
                timestamp_str, hr
            )?;
        } else {
            // Steps
            writeln!(
                writer,
                " <Record type=\"HKQuantityTypeIdentifierStepCount\" sourceName=\"Generator\" unit=\"count\" creationDate=\"{0}\" startDate=\"{0}\" endDate=\"{0}\" value=\"15\"/>",
                timestamp_str
            )?;
        }
    }

    writeln!(writer, "</HealthData>")?;
    println!("Done.");
    Ok(())
}
