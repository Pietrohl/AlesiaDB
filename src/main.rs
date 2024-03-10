pub mod types;
use crate::types::dto::{ColumnData, QueryDTO, ResponseDTO};
use rusqlite::Connection;
use std::borrow::Borrow;
use std::str::from_utf8;
use std::vec;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use types::dto::{DataType, TableRow};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // print_test();

    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Server started on 127.0.0.1:8080");

    loop {
        let (socket, _) = listener.accept().await?;
        println!("Accepted connection from: {}", socket.peer_addr()?);

        tokio::spawn(async move {
            if let Err(e) = handle_client(socket).await {
                eprintln!("Error handling client: {}", e);
            }
        });
    }
}

async fn handle_client(mut socket: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = [0; 1024];
    let addr = socket.peer_addr()?;
    println!("Handling client: {}", addr);

    loop {
        let n = socket.read(&mut bytes).await?;
        if n == 0 {
            println!("Client disconnected: {}", addr);
            return Ok(());
        }

        let result: String = handle_message(&bytes, n - 1)
            .await
            .map_or_else(|e| format!("Error handling message: {}", e), |r| r);

        socket.write_all(result.as_bytes()).await?;
        socket.write_all(b"\n").await?;
    }
}

async fn handle_message(
    bytes: &[u8; 1024],
    n: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    let msg = String::from_utf8((&bytes[..n]).to_vec())?;

    let query: QueryDTO = serde_json::from_str(&msg)?;

    let response = match execute_sql_query(&query).await {
        Ok(result) => serde_json::to_string(&result)?,
        Err(err) => format!("Error: {}", err),
    };

    Ok(response)
}

async fn execute_sql_query(
    query_msg: &QueryDTO,
) -> Result<ResponseDTO, Box<dyn std::error::Error>> {
    let conn: Connection = Connection::open("sqlite.db")?;

    {
        let mut stmt = conn.prepare(query_msg.query.as_ref())?;

        for i in 0..query_msg.params.len() {
            let err = stmt.raw_bind_parameter(i + 1, 1);

            if let Err(err) = err {
                return Err(Box::new(err));
            }
        }

        let column_count = stmt.borrow().column_count();

        let mut rows = stmt.raw_query();
        let mut result = ResponseDTO {
            status: "OK".to_string(),
            rows: vec![],
            column_count,
        };

        while let Some(row) = rows.next()? {
            let mut new_table_row = TableRow { columns: vec![] };

            for i in 0..result.column_count {
                let value: ColumnData = match row.get_ref::<usize>(i).unwrap().data_type() {
                    rusqlite::types::Type::Integer => ColumnData {
                        data: row
                            .get_ref::<usize>(i)
                            .unwrap()
                            .as_i64()
                            .unwrap()
                            .to_string(),
                        data_type: DataType::INTEGER,
                    },
                    rusqlite::types::Type::Real => ColumnData {
                        data: row
                            .get_ref::<usize>(i)
                            .unwrap()
                            .as_f64()
                            .unwrap()
                            .to_string(),
                        data_type: DataType::FLOAT,
                    },
                    rusqlite::types::Type::Text => ColumnData {
                        data: row
                            .get_ref::<usize>(i)
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .to_string(),
                        data_type: DataType::TEXT,
                    },
                    rusqlite::types::Type::Blob => ColumnData {
                        data: from_utf8(row.get_ref::<usize>(i).unwrap().as_blob().unwrap())
                            .unwrap()
                            .to_string(),
                        data_type: DataType::BLOB,
                    },
                    _ => ColumnData {
                        data: "".into(),
                        data_type: DataType::NULL,
                    },
                };

                new_table_row.columns.push(value);
            }
            result.rows.push(new_table_row);
        }

        Ok(result)
    }
}
// fn print_test() -> Vec<u8> {
//     let message = QueryDTO {
//         query: "SELECT * FROM clients WHERE id = ?1;".to_string().into(),
//         params: vec![ColumnData {
//             data: "1".into(),
//             data_type: DataType::INTEGER,
//         }],
//     };

//     let result = serde_json::to_string(&message).unwrap();
//     let result_as_bytes = result.as_bytes();

//     println!(
//         "The message should be: {0}",
//         from_utf8(&result_as_bytes).unwrap()
//     );

//     return result_as_bytes.into();
// }
