use test::test_client::TestClient;
use test::TestRequest;
use std::env;
pub mod test {
    tonic::include_proto!("test");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let query = &args[1];
    let mut addr_str = String::from("http://[::1]:");
    addr_str.push_str(query); 
    let mut client = TestClient::connect(addr_str).await?;
    let request = tonic::Request::new(TestRequest {
        name: "Hello".into(),
    });

    let response = client.request(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
