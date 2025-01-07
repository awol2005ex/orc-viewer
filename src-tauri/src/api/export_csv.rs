use arrow::csv;
use orc_rust::ArrowReaderBuilder;
use std::fs::File;

#[tauri::command]
pub fn export_orc_file_csv(orc_filename: &str, target_csv_file_path: &str) -> Result<(), String> {

    let orc_file = File::open(orc_filename).map_err(|e| format!("Failed to open file: {}", e))?;

    let mut orc_reader = ArrowReaderBuilder::try_new(orc_file)
        .map_err(|e| format!("Failed to create reader: {}", e))?
        .build();

    loop {
    
        if let Some( Ok( batch)) =orc_reader.next(){
            let csv_file = File::create(target_csv_file_path).map_err(|e| format!("Failed to create file: {}", e))?;
            let mut writer = csv::WriterBuilder::new().with_delimiter(b',').with_show_nested(true).build(csv_file);
            
            let _ = writer.write(&batch).map_err(|e| format!("Failed to write file: {}", e))?;
        } else {
            break;
        }
    }
    Ok(())
}
