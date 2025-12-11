use std::env;
// use test::test_client::TestClient;
use test::test_server::{Test, TestServer};
use test::{TestReply, TestRequest};
use tokio::runtime::Runtime;
use tokio::sync::watch;
use tokio::time::timeout;
use tokio::time::{sleep, Duration};
use tonic::{transport::Server, Request, Response, Status};

pub mod test {
    tonic::include_proto!("test");
}

#[derive(Debug)]
pub struct MyTest {
    tx: tokio::sync::watch::Sender<i32>,
}

#[derive(Debug, Default)]
pub struct Timeout {}

#[tonic::async_trait]
impl Test for MyTest {
    async fn request(&self, request: Request<TestRequest>) -> Result<Response<TestReply>, Status> {
        // println!("Got a request: {:?}", request);
        let reply = TestReply {
            message: format!("Hello {}!", request.into_inner().name),
        };

        sleep(Duration::from_millis(100)).await;
        let _ = self.tx.send(1);

        Ok(Response::new(reply))
    }
}

// enum LogType {
//     set,
//     delete,
//     append,
// }

// pub struct Log {
//     type: LogType,
//     key: &String,
//     val: &String,
// }

// pub struct Node {
//     map: HashMap<&String, &mut String>,
//     current_term: i32,
//     voted_for: i32,
//     log: Vec<Log>,
//     commit_index: i32,
//     last_applied: i32,
//     next_index: Vec<i32>,
//     match_index: Vec<i32>,
//     my_port: &mut String,

// }

// impl Node {
//     pub fn initialize(&mut self, port: &String) {
//         println!{"TODO: initialize with port: {port}"};
//     }
//     pub fn
// }

async fn receiver(tx: tokio::sync::watch::Sender<i32>) -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let ip_addr = &args[1];
    let port = &args[2];
    let mut addr_str = String::from("");
    addr_str.push_str(ip_addr);
    addr_str.push(':');
    addr_str.push_str(port);
    let addr = addr_str.parse()?;
    let test = MyTest { tx: tx };

    Server::builder()
        .add_service(TestServer::new(test))
        .serve(addr)
        .await?;

    Ok(())
}

async fn sender(
    rx: &mut tokio::sync::watch::Receiver<i32>,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        match timeout(Duration::from_millis(5000), rx.changed()).await {
            Ok(_) => println!("not time out yayyy"),
            Err(_) => println!("time out brow"),
        }
    }
}

fn main() {
    let rt = Runtime::new().unwrap();
    let (tx, mut rx) = watch::channel::<i32>(0);

    print!("{:?}\n", tx);
    rt.block_on(async move {
        println!("hello from the async block");

        tokio::spawn(async {
            let _ = receiver(tx).await;
        });
        tokio::spawn(async move {
            let _ = sender(&mut rx).await;
        });
    });
    loop {}
}
