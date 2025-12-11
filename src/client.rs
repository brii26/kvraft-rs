use test::test_client::TestClient;
use test::TestRequest;
use std::env;
pub mod test {
    tonic::include_proto!("test");
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
    let mut client = TestClient::connect(addr_str).await?;
    let request = tonic::Request::new(TestRequest {
        name: "Hello".into(),
    });

    let response = client.request(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
