use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct TableRow {
    pub columns: Vec<ColumnData>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct QueryDTO {
    pub query: String,
    pub params: Vec<ColumnData>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct ResponseDTO {
    pub status: String,
    pub rows: Vec<TableRow>,
    pub column_count: usize,
    // pub column_names: Vec<&'static str>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum DataType {
    NULL,
    INTEGER,
    FLOAT,
    TEXT,
    BLOB,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct ColumnData {
    pub data: String,
    pub data_type: DataType,
}
