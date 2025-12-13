use std::env;
use std::collections::HashMap;
use raft_service::raft_service_server::{RaftService, RaftServiceServer};
use raft_service::{ClientRequest, ClientReply,
					AppendRequest, AppendReply, 
					VoteRequest, VoteReply,
					MembershipRequest, MembershipReply
				};
use tokio::runtime::Runtime;
use tokio::sync::watch;
use tokio::time::timeout;
use tokio::time::{sleep, Duration};
use tonic::{transport::Server, Request, Response, Status};

pub mod raft_service {
    tonic::include_proto!("raft_service");
}


// STRUCT
enum LogType {
	PING,
	GET,
	SET,
	APPEND,
	DEL,
	STRLEN
}

pub struct Log<'a> {
	log_type: LogType,
	key: &'a String,
	val: &'a String,
}

enum NodeType {
	LEADER,
	CANDIDATE,
	FOLLOWER
}
	
pub struct Node<'a> {
	address: &'a mut String, 					// Node Address
	node_type: NodeType, 						// leader, candidate , follower
	log: Vec<Log<'a>>,							// Stores Logs
	data: HashMap<&'a String, &'a mut String>,	// Key value pair data
	cluster_addr_list : Vec<String>,			// Address List Cluster
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

impl Log<'_> {
	// fn createLog(&data : String) {
	// 	let new_log : Log = serde_json::from_str(&data)?;
	// 	return new_log;
	// }
}

impl NodeType {
	fn	init() {

	}
}

impl Node<'_> {
	fn init() {

	}
}

#[derive(Debug)]
pub struct MyRaftService {
    tx: tokio::sync::watch::Sender<i32>,
}

#[derive(Debug, Default)]
pub struct Timeout {}

#[tonic::async_trait]
impl RaftService for MyRaftService {

	async fn client(&self, request: Request<ClientRequest>) -> Result<Response<ClientReply>, Status> {
        // println!("Got a request: {:?}", request);
        let reply = ClientReply {
            message: format!("Hello {}!", request.into_inner().name),
        };

        sleep(Duration::from_millis(100)).await;
        let _ = self.tx.send(1);

        Ok(Response::new(reply))
    }

	async fn append_entries(&self, request: Request <AppendRequest>) -> Result<Response<AppendReply>, Status> {
		let reply = AppendReply {
			term: Some(0),
			success: Some(false),
		};
		Ok(Response::new(reply))
	}

	async fn vote_me(&self, request: Request <VoteRequest>) -> Result<Response<VoteReply>, Status> {
		let reply = VoteReply {
			term: Some(0),
			vote_granted: Some(false),
		};
		Ok(Response::new(reply))
	}

	async fn membership(&self, request: Request <MembershipRequest>) -> Result<Response<MembershipReply>, Status> {
		let reply = MembershipReply {
			ip_addr: Some("0.0.0.0".to_string()),
			port: Some("0000".to_string()),
			status : Some(true)
		};
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
    let raft_service = MyRaftService { tx: tx };

    Server::builder()
        .add_service(RaftServiceServer::new(raft_service))
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
