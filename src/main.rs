use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use mini_redis::{Connection, Frame};
use tokio::net::{TcpListener, TcpStream};

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() {
    // bind the listener to the address
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    println!("listening");

    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        // the second item contains the IP and port of the new connection
        let (socket, _) = listener.accept().await.unwrap();
        // Clone the handle to the hashmap
        let db = db.clone();

        // A new taks is spawned for each inbound socket. The socket is
        // moved to the new task and processed there.
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{self, Get, Set};

    // Connection, provided by mini-redis, handles parsing frames from
    // the socket
    let mut connection = Connection::new(socket);

    // use read_frame to receive a command from the connection
    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db.lock().unwrap();
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("ok".to_string())
            }
            Get(cmd) => {
                let db = db.lock().unwrap();
                if let Some(value) = db.get(cmd.key()) {
                    // Frame::Bulk expects data to be of type bytes. this
                    // type will be covered later in the tutorial. For now,
                    // &Vec<u8> is converted to Bytes using into()
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        // write the response to the client
        connection.write_frame(&response).await.unwrap();
    }
}
