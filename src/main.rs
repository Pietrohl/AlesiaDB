use alesia_client::connection::{Deserialize, Serialize};
use alesia_client::errors::Error;
use alesia_client::types::dto::{ColumnData, DataType, QueryType, RequestDTO};
use alesia_client::types::dto::{ResponseDTO, TableRowDTO};
use rusqlite::types::ToSql;
use rusqlite::Connection;
use rusqlite::{ffi, params_from_iter};
use std::borrow::Borrow;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

type SafeConnection = Arc<Mutex<Connection>>;

struct DatabaseConfig {
    path: String,
}

struct Server {
    listener: TcpListener,
    db_config: DatabaseConfig,
}

impl Server {
    pub async fn new(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(url).await?;
        println!("Server started on {url}");

        Ok(Server {
            listener,
            db_config: DatabaseConfig {
                path: String::from("sqlite.db"),
            },
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server: Server = Server::new("127.0.0.1:8080").await?;

    run_server(server).await
}

async fn run_server(server: Server) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let (socket, _) = server.listener.accept().await?;
        println!("Accepted connection from: {}", socket.peer_addr()?);

        let path = server.db_config.path.to_owned();
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, &path).await {
                eprintln!("Error handling client: {}", e);
            }
        });
    }
}

async fn handle_client<'a>(
    socket: TcpStream,
    path: &'a str,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = socket.peer_addr()?;
    println!("Handling client: {}", addr);
    let conn: SafeConnection = Arc::new(Mutex::new(Connection::open(path)?));
    let (mut reader, mut writer) = socket.into_split();
    loop {
        let message = RequestDTO::deserialize(&mut reader).await?;

        let reponse_message = handle_message(message, &conn).await?;

        reponse_message.serialize(&mut writer).await?;
    }
}

async fn handle_message(
    query: RequestDTO,
    conn: &Arc<Mutex<Connection>>,
) -> Result<ResponseDTO, Error> {
    let result = match query.query_type {
        QueryType::QUERY => execute_sql_query(&query, conn).await?,
        QueryType::EXEC => execute_sql_stm(&query, conn).await?,
        QueryType::INSERT => execute_sql_insert(&query, conn).await?,
    };

    Ok(result)
}

async fn execute_sql_stm(
    query_msg: &RequestDTO,
    conn: &Arc<Mutex<Connection>>,
) -> Result<ResponseDTO, Error> {
    let conn_lock = conn.lock().await;

    // if conn_lock.isbusy() wait 5ms and try again, repeat 10 times
    let mut retries = 0;
    while conn_lock.is_busy() && retries < 10 {
        tokio::time::sleep(Duration::from_millis(5)).await;
        retries += 1;
    }

    if conn_lock.is_busy() {
        return Err(Error::RusqliteError(rusqlite::Error::SqliteFailure(
            ffi::Error {
                code: ffi::ErrorCode::DatabaseBusy,
                extended_code: 0,
            },
            Some("Database is busy".to_string()),
        )));
    }

    let mut stmt = conn_lock.prepare(query_msg.query.as_ref()).unwrap();

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
        Err(err) => Err(Error::RusqliteError(err)),
    }
}

async fn execute_sql_query(
    query_msg: &RequestDTO,
    conn: &Arc<Mutex<Connection>>,
) -> Result<ResponseDTO, Error> {
    let conn_lock = conn.lock().await;

    {
        let mut stmt = conn_lock.prepare(query_msg.query.as_ref())?;

        for i in 0..query_msg.params.len() {
            let err = stmt.raw_bind_parameter(i + 1, query_msg.params[i].data.to_sql()?);

            if let Err(err) = err {
                return Err(Error::RusqliteError(err));
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

async fn execute_sql_insert(
    query_msg: &RequestDTO,
    conn: &Arc<Mutex<Connection>>,
) -> Result<ResponseDTO, Error> {
    let conn_lock = conn.lock().await;
    let mut stmt = conn_lock.prepare(&query_msg.query)?;

    for i in 0..query_msg.params.len() {
        let err = stmt.raw_bind_parameter(i + 1, query_msg.params[i].data.to_sql()?);

        if let Err(err) = err {
            return Err(Error::RusqliteError(err));
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
        Err(err) => Err(Error::RusqliteError(err)),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    #[tokio::test]
    async fn test_execute_sql_select() {
        // Mock the connection
        let conn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        let query_msg = RequestDTO {
            query: "SELECT * FROM users".to_string(),
            query_type: QueryType::QUERY,
            params: vec![],
        };

        // Insert test data into the database
        conn.lock()
            .await
            .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
            .unwrap();
        conn.lock()
            .await
            .execute("INSERT INTO users (name) VALUES ('John'), ('Jane')", [])
            .unwrap();

        // Execute the SQL select query
        let result = execute_sql_query(&query_msg, &conn).await.unwrap();

        // Assert the result
        assert_eq!(result.rows_affected, 0);
        assert_eq!(result.status, "OK");
        assert_eq!(result.column_count, 2);
        assert_eq!(result.column_names, vec!["id", "name"]);

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].columns.len(), 2);
        assert_eq!(result.rows[0].columns[0].data, "1");
        assert!(matches!(
            result.rows[0].columns[0].data_type,
            DataType::INTEGER
        ));
        assert_eq!(result.rows[0].columns[1].data, "John");
        assert!(matches!(
            result.rows[0].columns[1].data_type,
            DataType::TEXT
        ));

        assert_eq!(result.rows[1].columns.len(), 2);
        assert_eq!(result.rows[1].columns[0].data, "2");
        assert!(matches!(
            result.rows[1].columns[0].data_type,
            DataType::INTEGER
        ));
        assert_eq!(result.rows[1].columns[1].data, "Jane");
        assert!(matches!(
            result.rows[1].columns[1].data_type,
            DataType::TEXT
        ));

        conn.lock().await.execute("DROP TABLE users", []).unwrap();
    }

    #[tokio::test]
    async fn test_execute_sql_insert() {
        // Mock the connection
        let conn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));

        // Insert test data into the database
        conn.lock()
            .await
            .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
            .unwrap();

        let query_msg = RequestDTO {
            query: "INSERT INTO users (name) VALUES (?)".to_string(),
            query_type: QueryType::INSERT,
            params: vec![ColumnData {
                data: "John".to_string(),
                data_type: DataType::TEXT,
            }],
        };

        // Execute the SQL insert query
        let result = execute_sql_insert(&query_msg, &conn).await.unwrap();

        // Assert the result
        assert_eq!(result.rows_affected, 1);
        assert_eq!(result.status, "OK");
        assert_eq!(result.column_count, 0);
        assert_eq!(
            result.column_names,
            std::vec::Vec::<std::string::String>::new()
        );
        assert_eq!(result.rows.len(), 0);

        conn.lock().await.execute("DROP TABLE users", []).unwrap();
    }

    #[tokio::test]
    async fn test_client_server() {
        let url = "127.0.0.1:8080";

        let conn = Connection::open("sqlite.db").unwrap();
        conn.execute("DROP TABLE users", []).ok();
        // Insert test data into the database
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO users (name) VALUES ('John'), ('Jane')", [])
            .unwrap();
        conn.close().unwrap();
        let server = Server::new(url).await.unwrap();

        tokio::spawn(async {
            run_server(server).await.unwrap();
        });

        // Create a client

        let req_params = [];
        let mut client = alesia_client::new_from_url(url).await.unwrap();

        let result = client
            .query("SELECT * FROM users", &req_params)
            .await
            .unwrap();

        // Assert the result
        assert_eq!(result[0].get_by_name("name").data, "John");
        assert_eq!(result[1].get_by_name("name").data, "Jane");
    }
}
