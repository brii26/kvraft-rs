use raft_service::raft_service_client::RaftServiceClient;
use raft_service::{ClientRequest, ClientReply};
use std::env;
pub mod raft_service {
    tonic::include_proto!("raft_service");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {panic!("Brow aint no way salah input: liat readme pls")}
    let ip_addr = &args[1];
    let port = &args[2];
    let mut addr_str = String::from("http://");
    addr_str.push_str(ip_addr);
    addr_str.push(':');
    addr_str.push_str(port); 
    let mut client = RaftServiceClient::connect(addr_str).await?;
    let request = tonic::Request::new(ClientRequest {
        name: "Hello".into(),
    });

    let response = client.client(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
