use tonic::{transport::Server, Request, Response, Status};
use tokio::runtime::Runtime;
use tokio::time::{sleep, Duration};
use hello_world::greeter_server::{Greeter, GreeterServer};
use hello_world::{HelloReply, HelloRequest};
use hello_world::greeter_client::GreeterClient;
use std::env;

pub mod hello_world {
    tonic::include_proto!("hello_world");
}

#[derive(Debug, Default)]
pub struct MyGreeter {}

#[derive(Debug, Default)]
pub struct Timeout {}

#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply = HelloReply {
            message: format!("Hello {}!", request.into_inner().name),
        };

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


async fn receiver() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let query = &args[1]; 
    let mut addr_str = String::from("[::1]:");
    addr_str.push_str(query);
    let addr = addr_str.parse()?;
    let greeter = MyGreeter::default();

    Server::builder()
        .add_service(GreeterServer::new(greeter))
        .serve(addr)
        .await?;


    Ok(())
}

async fn sender() -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..3 {
        sleep(Duration::from_millis(1000)).await;
        let args: Vec<String> = env::args().collect();
        let query = &args[1];
        let mut addr_str = String::from("http://[::1]:");
        addr_str.push_str(query); 
        let mut client = GreeterClient::connect(addr_str).await?;
        let request = tonic::Request::new(HelloRequest {
            name: "Hello".into(),
        });

        let response = client.say_hello(request).await?;

        println!("RESPONSE={:?}", response);
    }

    Ok(())
}


fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async move {
        println!("hello from the async block");
        tokio::spawn(async { let _ = receiver().await; });
        tokio::spawn(async { let _ = sender().await; });
    });
    loop {}
}
