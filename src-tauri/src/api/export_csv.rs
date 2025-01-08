use orc_rust::ArrowReaderBuilder;
use std::{fs::File, io::Write};

#[tauri::command]
pub fn export_orc_file_csv(orc_filename: &str, target_csv_file_path: &str) -> Result<(), String> {
    let orc_file = File::open(orc_filename).map_err(|e| format!("Failed to open file: {}", e))?;

    let mut orc_reader = ArrowReaderBuilder::try_new(orc_file)
        .map_err(|e| format!("Failed to create reader: {}", e))?
        .build();

    let mut result_columns: Vec<String> = vec![];
    let mut csv_file =
        File::create(target_csv_file_path).map_err(|e| format!("Failed to create file: {}", e))?;

    loop {
        if let Some(Ok(batch)) = orc_reader.next() {
            if result_columns.is_empty() {
                for field in batch.schema().fields() {
                    result_columns.push(field.name().to_owned());
                }
                csv_file
                    .write(result_columns.join(",").as_bytes())
                    .map_err(|e| format!("Failed to write file: {}", e))?;
                csv_file
                    .write("\n".as_bytes())
                    .map_err(|e| format!("Failed to write file: {}", e))?;
            }
            let buf = Vec::new();
            let mut writer = arrow::json::ArrayWriter::new(buf);
            writer
                .write_batches(&vec![&batch])
                .map_err(|e| format!("Failed to write batch: {}", e))?;
            writer
                .finish()
                .map_err(|e| format!("Failed to write batch: {}", e))?;
            let buf = writer.into_inner();
            let json_str =
                String::from_utf8(buf).map_err(|e| format!("Failed to parse json: {}", e))?;
            let json: Vec<serde_json::Value> = serde_json::from_str(&json_str)
                .map_err(|e| format!("Failed to parse json: {}", e))?;
            for item in json {
                if let Some(object) = item.as_object() {
                    let row: Vec<String> = result_columns
                        .iter()
                        .map(|c| {
                            let s = object.get(c).unwrap_or(&serde_json::Value::Null);
                            let mut ss = "".to_owned();
                            if s.is_string() {
                                ss = s.as_str().unwrap_or_default().to_owned().replace("\"","\"\"");
                            } else {
                                ss = s.to_string().replace("\"","\"\"");
                            }
                            if ss.contains(",") {
                                format!("\"{}\"", &ss)
                            } else {
                                ss
                            }
                        })
                        .collect();
                    csv_file
                        .write(row.join(",").as_bytes())
                        .map_err(|e| format!("Failed to write file: {}", e))?;
                    csv_file
                        .write("\n".as_bytes())
                        .map_err(|e| format!("Failed to write file: {}", e))?;
                }
            }
        } else {
            break;
        }
    }
    csv_file
        .flush()
        .map_err(|e| format!("Failed to write file: {}", e))?;
    Ok(())
}
