use alesia_client::connection::{Deserialize, Serialize};
use alesia_client::errors::Error;
use alesia_client::types::dto::{ColumnData, DataType, QueryType, RequestDTO};
use alesia_client::types::dto::{ResponseDTO, TableRowDTO};
use rusqlite::types::ToSql;
use rusqlite::Connection;
use rusqlite::{ffi, params_from_iter};
use std::borrow::Borrow;
use std::fs::File;
use std::io::BufReader;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::{rustls, TlsAcceptor};

#[derive(Debug, Clone)]
struct ServerConfig {
    socket_address: String,
    tls_config: Option<rustls::ServerConfig>,
}

#[derive(Debug, Clone)]
struct DatabaseConfig {
    path: String,
}

#[derive(Debug, Clone)]
struct ConfigState(DatabaseConfig, ServerConfig);

impl Default for ConfigState {
    fn default() -> Self {
        ConfigState(
            DatabaseConfig {
                path: "sqlite.db".to_string(),
            },
            ServerConfig {
                socket_address: "127.0.0.1:8080".to_string(),
                tls_config: None,
            },
        )
    }
}

impl ConfigState {
    fn init_from_env() -> ConfigState {
        let path = std::env::var("DATABASE_PATH").unwrap_or_else(|_| "sqlite.db".to_string());
        let socket_address =
            std::env::var("ADDRESS").unwrap_or_else(|_| "127.0.0.1:8080".to_string());

        let tls = std::env::var("TLS")
            .unwrap_or_else(|_| "false".to_string())
            .parse()
            .unwrap();

        let tls_config = if tls {
            let cert_file =
                std::env::var("TLS_CERT_PATH").expect("missing certificate file argument");
            let private_key_file =
                std::env::var("PRIVATE_KEY_PATH").expect("missing private key file argument");

            let certs = rustls_pemfile::certs(&mut BufReader::new(
                &mut File::open(cert_file).expect("Certificate file not found"),
            ))
            .collect::<Result<Vec<_>, _>>()
            .expect("Error reading certificate file");

            let private_key = rustls_pemfile::private_key(&mut BufReader::new(
                &mut File::open(private_key_file).expect("Private key file not found"),
            ))
            .expect("Error reading private key file")
            .expect("No private key found on file");

            Some(
                rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(certs, private_key)
                    .unwrap(),
            )
        } else {
            None
        };

        let db_config = DatabaseConfig { path };
        let server_config = ServerConfig {
            socket_address,
            tls_config,
        };

        ConfigState(db_config, server_config)
    }
}

struct Server {
    listener: TcpListener,
    acceptor: Option<Arc<TlsAcceptor>>,
}

impl Server {
    pub async fn new(config: &ServerConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let acceptor: Option<Arc<TlsAcceptor>> = match config.tls_config.clone() {
            Some(tls_config) => Some(Arc::new(TlsAcceptor::from(Arc::new(tls_config)))),
            None => None,
        };

        let listener = TcpListener::bind(&config.socket_address).await?;

        println!("Server started on {}", &config.socket_address);

        Ok(Server { listener, acceptor })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ConfigState::init_from_env();

    let server: Server = Server::new(&config.1).await?;

    run_server(server, &config).await
}

async fn run_server(
    server: Server,
    ConfigState(db_config, _): &ConfigState,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let (socket, peer_addr) = server.listener.accept().await?;

        let path = db_config.path.to_owned();

        println!("Accepted connection from: {}", peer_addr);

        if let Some(acceptor) = server.acceptor.clone() {
            let acceptor = acceptor.clone();
            let socket = Box::new(acceptor.accept(socket).await?);
            tokio::spawn(async move {
                if let Err(e) = handle_client(socket, &path, peer_addr).await {
                    eprintln!("Error handling client: {}", e);
                }
            });
        } else {
            tokio::spawn(async move {
                if let Err(e) = handle_client(socket, &path, peer_addr).await {
                    eprintln!("Error handling client: {}", e);
                }
            });
        }
    }
}

async fn handle_client<'a, T: AsyncReadExt + AsyncWriteExt + Send + Sync>(
    socket: T,
    path: &'a str,
    addr: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Handling client: {}", addr);
    let mut conn = Connection::open(path)?;
    let (mut reader, mut writer) = split(socket);
    loop {
        let message = RequestDTO::deserialize(&mut reader).await?;

        let reponse_message = handle_message(message, &mut conn).await;

        match reponse_message {
            Ok(response) => response.serialize(&mut writer).await?,
            Err(e) => {
                println!("Error: {:?}", e);
                e.serialize(&mut writer).await?
            }
        }
    }
}

async fn handle_message(query: RequestDTO, conn: &mut Connection) -> Result<ResponseDTO, Error> {
    // if conn_lock.isbusy() wait 5ms and try again, repeat 100 times
    let mut retries = 0;
    while conn.is_busy() && retries < 100 {
        tokio::time::sleep(Duration::from_millis(5)).await;
        retries += 1;
    }

    if conn.is_busy() {
        return Err(Error::RusqliteError(rusqlite::Error::SqliteFailure(
            ffi::Error {
                code: ffi::ErrorCode::DatabaseBusy,
                extended_code: 0,
            },
            Some("Database is busy".to_string()),
        )));
    }

    let result = match query.query_type {
        QueryType::QUERY => execute_sql_query(&query, conn).await?,
        QueryType::EXEC => execute_sql_stm(&query, conn).await?,
        QueryType::INSERT => execute_sql_insert(&query, conn).await?,
    };

    Ok(result)
}

async fn execute_sql_stm(
    query_msg: &RequestDTO,
    conn: &mut Connection,
) -> Result<ResponseDTO, Error> {
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
        Err(err) => Err(Error::RusqliteError(err)),
    }
}

async fn execute_sql_query(
    query_msg: &RequestDTO,
    conn: &mut Connection,
) -> Result<ResponseDTO, Error> {
    {
        let mut stmt = conn.prepare(query_msg.query.as_ref())?;

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
    conn: &mut Connection,
) -> Result<ResponseDTO, Error> {
    let mut stmt = conn.prepare(&query_msg.query)?;

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
        let mut conn = Connection::open_in_memory().unwrap();
        let query_msg = RequestDTO {
            query: "SELECT * FROM users".to_string(),
            query_type: QueryType::QUERY,
            params: vec![],
        };

        // Insert test data into the database
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO users (name) VALUES ('John'), ('Jane')", [])
            .unwrap();

        // Execute the SQL select query
        let result = execute_sql_query(&query_msg, &mut conn).await.unwrap();

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

        conn.execute("DROP TABLE users", []).unwrap();
    }

    #[tokio::test]
    async fn test_execute_sql_insert() {
        // Mock the connection
        let mut conn = Connection::open_in_memory().unwrap();

        // Insert test data into the database
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
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
        let result = execute_sql_insert(&query_msg, &mut conn).await.unwrap();

        // Assert the result
        assert_eq!(result.rows_affected, 1);
        assert_eq!(result.status, "OK");
        assert_eq!(result.column_count, 0);
        assert_eq!(
            result.column_names,
            std::vec::Vec::<std::string::String>::new()
        );
        assert_eq!(result.rows.len(), 0);

        conn.execute("DROP TABLE users", []).unwrap();
    }

    #[tokio::test]
    async fn test_client_server() {
        let url = "127.0.0.1:3232";
        let server_config = ServerConfig {
            socket_address: url.into(),
            tls_config: None,
        };
        let database_path = "sqlite.db";
        let db_config = DatabaseConfig {
            path: database_path.into(),
        };

        let conn = Connection::open(database_path).unwrap();
        conn.execute("DROP TABLE users", []).ok();
        // Insert test data into the database
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO users (name) VALUES ('John'), ('Jane')", [])
            .unwrap();
        conn.close().unwrap();

        tokio::spawn(async move {
            let config = ConfigState(db_config, server_config);
            let server = Server::new(&config.1).await.unwrap();
            run_server(server, &config).await.unwrap();
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_secs(2)).await;

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
