use alesia_client::types::dto::{ColumnData, DataType, QueryType, RequestDTO};
use alesia_client::types::dto::{ResponseDTO, TableRowDTO};
use rusqlite::params_from_iter;
use rusqlite::types::ToSql;
use rusqlite::Connection;
use std::borrow::Borrow;
use std::str::from_utf8;
use std::vec;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = Server::new("localhost:8080").await?;

    loop {
        let (socket, _) = server.listener.accept().await?;
        println!("Accepted connection from: {}", socket.peer_addr()?);

        tokio::spawn(async move {
            if let Err(e) = handle_client(socket).await {
                eprintln!("Error handling client: {}", e);
            }
        });
    }
}

struct Server {
    listener: TcpListener,
}

impl Server {
    pub async fn new(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(url).await?;
        println!("Server started on {url}");

        Ok(Server { listener })
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
        // socket.write_all(b"\n").await?;
    }
}

async fn handle_message(
    bytes: &[u8; 1024],
    n: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    let msg = String::from_utf8((&bytes[..n]).to_vec())?;

    let query: RequestDTO = serde_json::from_str(&msg)?;

    let result = match query.query_type {
        QueryType::QUERY => execute_sql_query(&query).await,
        QueryType::EXEC => execute_sql_stm(&query).await,
        QueryType::INSERT => execute_sql_insert(&query),
    };

    let response = match result {
        Ok(result) => serde_json::to_string(&result)?,
        Err(err) => format!("Error: {}", err),
    };

    Ok(response)
}

async fn execute_sql_stm(
    query_msg: &RequestDTO,
) -> Result<ResponseDTO, Box<dyn std::error::Error>> {
    let conn: Connection = Connection::open("sqlite.db").unwrap();

    let mut stmt = conn.prepare(query_msg.query.as_ref()).unwrap();

    for i in 0..query_msg.params.len() {
        let err = stmt.raw_bind_parameter(i + 1, query_msg.params[i].data.to_sql()?);

        if let Err(err) = err {
            println!("Error: {}", err);
        }
    }

    match stmt.raw_execute() {
        Ok(rows_affected) => Ok(ResponseDTO {
            rows_affected,
            status: "OK".to_string(),
            rows: vec![],
            column_count: 0,
            column_names: vec![],
        }),
        Err(err) => Err(Box::new(err)),
    }
}

async fn execute_sql_query(
    query_msg: &RequestDTO,
) -> Result<ResponseDTO, Box<dyn std::error::Error>> {
    let conn: Connection = Connection::open("sqlite.db")?;

    {
        let mut stmt = conn.prepare(query_msg.query.as_ref())?;

        for i in 0..query_msg.params.len() {
            let err = stmt.raw_bind_parameter(i + 1, query_msg.params[i].data.to_sql()?);

            if let Err(err) = err {
                return Err(Box::new(err));
            }
        }

        let column_count = stmt.borrow().column_count();
        let column_names = stmt
            .borrow()
            .column_names()
            .into_iter()
            .map(|x| x.to_string())
            .collect();

        let mut rows = stmt.raw_query();
        let mut result = ResponseDTO {
            rows_affected: 0,
            status: "OK".to_string(),
            rows: vec![],
            column_count,
            column_names,
        };

        while let Some(row) = rows.next()? {
            let mut new_table_row = TableRowDTO { columns: vec![] };

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

fn execute_sql_insert(query_msg: &RequestDTO) -> Result<ResponseDTO, Box<dyn std::error::Error>> {
    let conn: Connection = Connection::open("sqlite.db")?;

    let mut stmt = conn.prepare(&query_msg.query)?;

    for i in 0..query_msg.params.len() {
        let err = stmt.raw_bind_parameter(i + 1, query_msg.params[i].data.to_sql()?);

        if let Err(err) = err {
            return Err(Box::new(err));
        }
    }

    match stmt.insert(params_from_iter(
        query_msg.params.iter().map(|x| x.data.to_sql().unwrap()),
    )) {
        Ok(_) => Ok(ResponseDTO {
            rows_affected: 1,
            status: "OK".to_string(),
            rows: vec![],
            column_count: 0,
            column_names: vec![],
        }),
        Err(err) => Err(Box::new(err)),
    }
}
