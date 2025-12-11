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


// STRUCT
enum LogType {
	ping,
	get,
	set,
	append,
	del,
	strlen
}

pub struct Log {
	type: LogType,
	key: &String,
	val: &String,
}

enum NodeType {
	leader,
	candidate,
	follower
}
	
pub struct Node {
	address: &mut String, 						// Node Address
	node_type: NodeType, 						// leader, candidate , follower
	log: Vec<Log>,								// Stores Logs
	data: HashMap<&String, &mut String>,		// Key value pair data
	cluster_addr_list : List[String],			// Address List Cluster
	cluster_leader_addr: String,				// Current Leader Address
	election_term: i32,							// Current Election Term
	voted_for: i32,								// Voting ID
	commit_index: i32,							// Last Commit Index
	last_applied: i32,							// Highes Log Entry Index
	next_index: Vec<i32>,						// Expected Next Index Log
	match_index: Vec<i32>,						// ndex of highest log entry known to be replicated on server
}



// STRUCT IMPLEMENTATION
impl LogType {
	fn ping() {

	}
	fn get() {

	}
	fn set() {

	}
	fn append() {

	}
	fn del() {

	}
	fn strlen() {

	}
}

impl Log {
	fn createLog(data : String) {
		let new_log : Log = serde_json::from_str(data)?;
		return new_log;
	}
}

impl NodeType {
	fn	init() {

	}
}

impl Node {
	fn init() {

	}
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
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {panic!("Brow aint no way salah input: liat readme pls")}

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
