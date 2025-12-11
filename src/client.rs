use hello_world::greeter_client::GreeterClient;
use hello_world::HelloRequest;
use std::env;
pub mod hello_world {
    tonic::include_proto!("hello_world");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    Ok(())
}
