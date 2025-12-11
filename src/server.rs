use tonic::{transport::Server, Request, Response, Status};
// use std::collections::HashMap;
// use std::thread;
use hello_world::greeter_server::{Greeter, GreeterServer};
use hello_world::{HelloReply, HelloRequest};
use std::env;
pub mod hello_world {
    tonic::include_proto!("hello_world");
}

#[derive(Debug, Default)]
pub struct MyGreeter {}

#[derive(Debug, Default)]
pub struct Timeout {}

#[tonic::async_trait] // TODO: handle semua here termasuk bikin thread baru segala
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


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

// inisialisasi server: broadcast ke ip address port tertentu --> kalau di respon berarti masuk ke
// peer? list of known nodes.
// abis itu ada service utk terima input: 
// tapi selain service yang nungguin input, ada thread yang jalan sendiri yang actively memberi
// request. setiap kali mau memberikan request bikin thread baru utk kirim grpc ke mrk gitu kali
// yasdfgh
