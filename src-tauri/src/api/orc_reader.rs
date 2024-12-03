use arrow::array::{downcast_array, make_array, Array, AsArray, StringArray};
use orc_rust::ArrowReaderBuilder;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, fs::File, sync::Arc};
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
}

pub fn get_column_value( column :&Arc<dyn Array>, rowindex: usize) -> String {
    match column.data_type() {
        arrow::datatypes::DataType::Utf8 => {
           return  downcast_array::<StringArray>(column.as_ref()).value(rowindex).to_string();
            
        }
        arrow::datatypes::DataType::Null => {
            return  "NULL".to_owned();
        },
        arrow::datatypes::DataType::Boolean => {
            return  downcast_array::<arrow::array::BooleanArray>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Int8 => {
            return  downcast_array::<arrow::array::Int8Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Int16 => {
            return  downcast_array::<arrow::array::Int16Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Int32 => {
            return  downcast_array::<arrow::array::Int32Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Int64 => {
            return  downcast_array::<arrow::array::Int64Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::UInt8 => {
            return  downcast_array::<arrow::array::UInt8Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::UInt16 => {
            return  downcast_array::<arrow::array::UInt16Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::UInt32 => {
            return  downcast_array::<arrow::array::UInt32Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::UInt64 => {
            return  downcast_array::<arrow::array::UInt64Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Float16 => {
            return  downcast_array::<arrow::array::Float16Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Float32 => {
            return  downcast_array::<arrow::array::Float32Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Float64 => {
            return  downcast_array::<arrow::array::Float64Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Timestamp(time_unit, arc) => {
            return  downcast_array::<arrow::array::TimestampNanosecondArray>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Date32 => {
            return  downcast_array::<arrow::array::Date32Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Date64 => {
            return  downcast_array::<arrow::array::Date64Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Time32(time_unit) => {
            return  downcast_array::<arrow::array::Time32SecondArray>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Time64(time_unit) => {
            return  downcast_array::<arrow::array::Time64NanosecondArray>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Duration(time_unit) => {
            return  downcast_array::<arrow::array::DurationNanosecondArray>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Interval(interval_unit) => {
            return  downcast_array::<arrow::array::IntervalDayTimeArray>(column.as_ref()).value(rowindex).milliseconds.to_string();
        },
        arrow::datatypes::DataType::Binary => {
            return  String::from_utf8_lossy(&downcast_array::<arrow::array::BinaryArray>(column.as_ref()).value(rowindex)).to_string();
        },
        arrow::datatypes::DataType::FixedSizeBinary(_) => {
            return  String::from_utf8_lossy(&downcast_array::<arrow::array::FixedSizeBinaryArray>(column.as_ref()).value(rowindex)).to_string();
        },
        arrow::datatypes::DataType::LargeBinary => {
            return  String::from_utf8_lossy(&downcast_array::<arrow::array::LargeBinaryArray>(column.as_ref()).value(rowindex)).to_string();
        },
        arrow::datatypes::DataType::BinaryView => {
            return  String::from_utf8_lossy(&downcast_array::<arrow::array::BinaryArray>(column.as_ref()).value(rowindex)).to_string();
        },
        arrow::datatypes::DataType::LargeUtf8 => {
            return  downcast_array::<arrow::array::LargeStringArray>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Utf8View => {
            return  downcast_array::<arrow::array::StringArray>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::List(arc) => {
            return  format!("{:?}",downcast_array::<arrow::array::ListArray>(column.as_ref()).value(rowindex));
        },
        arrow::datatypes::DataType::ListView(arc) => {
            return  format!("{:?}",downcast_array::<arrow::array::ListArray>(column.as_ref()).value(rowindex));
        },
        arrow::datatypes::DataType::FixedSizeList(arc, _) => {
            return  format!("{:?}",downcast_array::<arrow::array::FixedSizeListArray>(column.as_ref()).value(rowindex));
        },
        arrow::datatypes::DataType::LargeList(arc) => {
            return  format!("{:?}",downcast_array::<arrow::array::LargeListArray>(column.as_ref()).value(rowindex));
        },
        arrow::datatypes::DataType::LargeListView(arc) => {
            return  format!("{:?}",downcast_array::<arrow::array::LargeListArray>(column.as_ref()).value(rowindex));
        },
        arrow::datatypes::DataType::Struct(fields) => {
            return  "NULL".to_owned();
        },
        arrow::datatypes::DataType::Union(union_fields, union_mode) => {
            return  format!("{:?}",downcast_array::<arrow::array::UnionArray>(column.as_ref()).value(rowindex));
        },
        arrow::datatypes::DataType::Dictionary(data_type, data_type1) => {
            return  "NULL".to_owned();
        },
        arrow::datatypes::DataType::Decimal128(_, _) => {
            return  downcast_array::<arrow::array::Decimal128Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Decimal256(_, _) => {
            return  downcast_array::<arrow::array::Decimal256Array>(column.as_ref()).value(rowindex).to_string();
        },
        arrow::datatypes::DataType::Map(arc, _) => {
            return  format!("{:?}",downcast_array::<arrow::array::MapArray>(column.as_ref()).value(rowindex));
        },
        arrow::datatypes::DataType::RunEndEncoded(arc, arc1) => {
            return  "NULL".to_owned();
        },
    }
    return "NULL".to_owned();
}

#[tauri::command]
pub fn read_orc_file(filename: &str) -> ReadOrcResult {
    println!("Reading file: {}", filename);

    match File::open(filename) {
        Ok(f) => match ArrowReaderBuilder::try_new(f) {
            Ok(afb) => {
                let reader = afb.build();

                let mut result_columns: Vec<ReadOrcResultColumn> = vec![];

                let mut result_data: Vec<HashMap<String, String>> = vec![];

                match reader.collect::<Result<Vec<_>, _>>() {
                    Ok(batchs) => {
                        for batch in batchs {
                            if result_columns.is_empty() {
                                for field in batch.schema().fields() {
                                    result_columns.push(ReadOrcResultColumn {
                                        name: field.name().to_uppercase(),
                                        data_type: field.data_type().to_string(),
                                    });
                                }
                            }

                            let mut batch_result_data: Vec<HashMap<String, String>> = vec![];
                            let batch_size = batch.num_rows();
                            for (columnindex, column) in batch.columns().into_iter().enumerate() {
                                for rowindex in 0..batch_size {
                                    let value = get_column_value(&column, rowindex);
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

                            result_data.append(&mut batch_result_data);
                        }
                        return ReadOrcResult {
                            code: 0,
                            message: "".to_owned(),
                            columns: result_columns,
                            rows: result_data,
                        };
                    }
                    Err(e) => {
                        return ReadOrcResult {
                            code: 1,
                            message: e.to_string(),
                            columns: vec![],
                            rows: vec![],
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
                }
            }
        },
        Err(e) => {
            return ReadOrcResult {
                code: 1,
                message: e.to_string(),
                columns: vec![],
                rows: vec![],
            }
        }
    }

    ReadOrcResult {
        code: 0,
        message: "success".to_string(),
        columns: vec![],
        rows: vec![],
    }
}