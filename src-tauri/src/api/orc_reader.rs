use arrow::util::display::{ArrayFormatter, FormatOptions};
use orc_rust::ArrowReaderBuilder;
use orc_rust::{reader::metadata::read_metadata, stripe::Stripe};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ReadOrcResultColumn {
    pub name: String,
    pub data_type: String,
}
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ReadOrcResult {
    pub columns: Vec<ReadOrcResultColumn>,
    pub rows: Vec<HashMap<String, String>>,
    pub code: i64,
    pub message: String,
    pub total: i64,
    pub compression_type: Option<String>,
}

#[tauri::command]
pub fn read_orc_file(filename: &str) -> ReadOrcResult {
    println!("Reading file: {}", filename);
    let mut compression_type:String="NONE".to_string();
    let mut total = 0;
    match File::open(filename) {
        Ok(mut f) => {
            match read_metadata(&mut f) {
                Ok(meta) => {
                    println!("{:?}", meta);
                    if let Some(compression_info) = meta.compression() {
                        compression_type=compression_info.compression_type().to_string();
                    }
                    total =meta.number_of_rows();

                }
                Err(e) => {
                    return ReadOrcResult {
                        code: 1,
                        message: e.to_string(),
                        columns: vec![],
                        rows: vec![],
                        total: 0,
                        compression_type: None,
                    }
                }
            }
            match ArrowReaderBuilder::try_new(f) {
                Ok(afb) => {
                    let reader = afb.build();

                    let mut result_columns: Vec<ReadOrcResultColumn> = vec![];

                    let mut result_data: Vec<HashMap<String, String>> = vec![];

                    let mut total = 0;
                    match reader.collect::<Result<Vec<_>, _>>() {
                        Ok(batchs) => {
                            let options = FormatOptions::default().with_display_error(true);

                            for batch in batchs {
                                if result_columns.is_empty() {
                                    for field in batch.schema().fields() {
                                        result_columns.push(ReadOrcResultColumn {
                                            name: field.name().to_owned(),
                                            data_type: field.data_type().to_string(),
                                        });
                                    }
                                }

                                let mut batch_result_data: Vec<HashMap<String, String>> = vec![];
                                let batch_size = batch.num_rows();
                                for (columnindex, column) in batch.columns().into_iter().enumerate()
                                {
                                    for rowindex in 0..batch_size {
                                        let formatter_result =
                                            ArrayFormatter::try_new(column.as_ref(), &options);
                                        if formatter_result.is_ok() {
                                            let formatter = formatter_result.unwrap();
                                            let value = formatter.value(rowindex);
                                            if rowindex >= batch_result_data.len() {
                                                let mut row = HashMap::new();
                                                row.insert(
                                                    result_columns[columnindex].name.clone(),
                                                    value.to_string(),
                                                );
                                                batch_result_data.push(row);
                                            } else {
                                                batch_result_data[rowindex].insert(
                                                    result_columns[columnindex].name.clone(),
                                                    value.to_string(),
                                                );
                                            }
                                        }
                                    }
                                }

                                result_data.append(&mut batch_result_data);
                                total = total + batch_size;
                            }
                            return ReadOrcResult {
                                code: 0,
                                message: "".to_owned(),
                                columns: result_columns,
                                rows: result_data,
                                total: total as i64,
                                compression_type: Some(compression_type),
                            };
                        }
                        Err(e) => {
                            return ReadOrcResult {
                                code: 1,
                                message: e.to_string(),
                                columns: vec![],
                                rows: vec![],
                                total: 0,
                                compression_type: None,
                            }
                        }
                    }
                }
                Err(e) => {
                    return ReadOrcResult {
                        code: 1,
                        message: e.to_string(),
                        columns: vec![],
                        rows: vec![],
                        total: 0,
                        compression_type: None,
                    }
                }
            }
        }
        Err(e) => {
            return ReadOrcResult {
                code: 1,
                message: e.to_string(),
                columns: vec![],
                rows: vec![],
                total: 0,
                compression_type: None,
            }
        }
    }
}

#[tauri::command]
pub fn read_orc_file_by_page(
    file_name: &str,
    page_size: usize,
    page_number: usize,
) -> ReadOrcResult {
    println!(
        "Reading file: {} ,page_size: {}, page_number: {}",
        &file_name, &page_size, &page_number
    );
    let mut compression_type: String= "NONE".to_string();
    let mut total = 0;
    match File::open(file_name) {
        Ok(mut f) => {
            match read_metadata(&mut f) {
                Ok(meta) => {
                    println!("{:?}", meta);
                    if let Some(compression_info) = meta.compression() {
                        compression_type=compression_info.compression_type().to_string();
                    }
                    total =meta.number_of_rows();

                }
                Err(e) => {
                    return ReadOrcResult {
                        code: 1,
                        message: e.to_string(),
                        columns: vec![],
                        rows: vec![],
                        total: 0,
                        compression_type: None,
                    }
                }
            }
            match ArrowReaderBuilder::try_new(f) {
                Ok(afb) => {
                    let reader = afb.with_batch_size(page_size).build();

                    let mut result_columns: Vec<ReadOrcResultColumn> = vec![];

                    
                    let mut result_data: Vec<HashMap<String, String>> = vec![];

                    let mut it = reader.skip(page_number - 1);
                    match it.next().take() {
                        Some(Ok(batch)) => {
                            println!("Read batch with {} rows", batch.num_rows());

                            if result_columns.is_empty() {
                                for field in batch.schema().fields() {
                                    result_columns.push(ReadOrcResultColumn {
                                        name: field.name().to_owned(),
                                        data_type: field.data_type().to_string(),
                                    });
                                }
                            }
                            let mut batch_result_data: Vec<HashMap<String, String>> = vec![];
                            let buf = Vec::new();
                            let mut writer = arrow::json::ArrayWriter::new(buf);
                            writer.write_batches(&vec![&batch]).unwrap_or_default();
                            writer.finish().unwrap_or_default();
                            let buf = writer.into_inner();
                            let json_str = String::from_utf8(buf).unwrap();
                            let json: Vec<serde_json::Value> =
                                serde_json::from_str(&json_str).unwrap_or_default();
                            for item in json {
                                let mut row = HashMap::new();
                                if let Some(object) = item.as_object() {
                                    object.iter().for_each(|(k, v)| {
                                        if v.is_string() {
                                            row.insert(
                                                k.to_string(),
                                                v.as_str().unwrap_or_default().to_string(),
                                            );
                                        } else {
                                            row.insert(k.to_string(), v.to_string());
                                        }
                                    });
                                }
                                batch_result_data.push(row);
                            }

                            result_data.append(&mut batch_result_data);
                        }
                        Some(Err(e)) => {
                            return ReadOrcResult {
                                code: 1,
                                message: e.to_string(),
                                columns: vec![],
                                rows: vec![],
                                total: 0,
                                compression_type: None,
                            }
                        }
                        None => {}
                    }
                    return ReadOrcResult {
                        code: 0,
                        message: "".to_owned(),
                        columns: result_columns,
                        rows: result_data,
                        total: total as i64,
                        compression_type: Some(compression_type),
                    };
                }
                Err(e) => {
                    return ReadOrcResult {
                        code: 1,
                        message: e.to_string(),
                        columns: vec![],
                        rows: vec![],
                        total: 0,
                        compression_type: None,
                    }
                }
            }
        }
        Err(e) => {
            return ReadOrcResult {
                code: 1,
                message: e.to_string(),
                columns: vec![],
                rows: vec![],
                total: 0,
                compression_type: None,
            }
        }
    }
}
