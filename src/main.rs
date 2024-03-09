use rusqlite::{Connection, Result as SqliteResult};
use std::borrow::Borrow;
use std::net::SocketAddr;
use std::str::from_utf8;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    let mut buf = [0; 1024];
    let addr = socket.peer_addr()?;
    println!("Handling client: {}", addr);

    loop {
        let n = socket.read(&mut buf).await?;
        if n == 0 {
            println!("Client disconnected: {}", addr);
            return Ok(());
        }

        let query = std::str::from_utf8(&buf[..n])?;
        let response = match execute_sql_query(query).await {
            Ok(result) => result,
            Err(err) => format!("Error: {}", err),
        };

        socket.write_all(response.as_bytes()).await?;
        socket.write_all(b"\n").await?;
    }
}

async fn execute_sql_query(query: &str) -> Result<String, Box<dyn std::error::Error>> {
    let conn = Connection::open("sqlite.db")?;
    let mut stmt = conn.prepare(query)?;


    let column_count = &stmt.column_count();

    let mut rows = stmt.query((1, "two"))?;

    let mut result = String::new();
    while let Some(row) = rows.next()? {
        for i in 0..column_count.to_owned() {
            let value = match row.get_ref::<usize>(i).unwrap().data_type() {
                rusqlite::types::Type::Integer => row
                    .get_ref::<usize>(i)
                    .unwrap()
                    .as_i64()
                    .unwrap()
                    .to_string(),
                rusqlite::types::Type::Real => row
                    .get_ref::<usize>(i)
                    .unwrap()
                    .as_f64()
                    .unwrap()
                    .to_string(),
                rusqlite::types::Type::Text => row
                    .get_ref::<usize>(i)
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string(),
                rusqlite::types::Type::Blob => {
                    from_utf8(row.get_ref::<usize>(i).unwrap().as_bytes().unwrap())
                        .unwrap()
                        .to_string()
                }
                _ => "NULL".to_string(),
            };

            result += &format!("{:?} ", value);
        }
        result += "\n";
    }

    Ok(result)
}
