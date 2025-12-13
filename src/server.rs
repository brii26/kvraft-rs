use raft_service::raft_service_server::{RaftService, RaftServiceServer};
use raft_service::raft_service_client::RaftServiceClient;

use raft_service::{
    AppendReply, AppendRequest, ClientReply, ClientRequest, MembershipReply, MembershipRequest,
    VoteReply, VoteRequest,
};
use std::any::Any;
use std::collections::HashMap;
use std::env;
use tokio::signal;
use tokio::sync::{mpsc, oneshot, watch};
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
    STRLEN,
}

pub struct Log {
    log_type: LogType,
    key: String,
    val: String,
}

#[derive(Clone, PartialEq)]
pub enum NodeType {
    LEADER,
    CANDIDATE,
    FOLLOWER,
}

#[derive(Clone, Debug)]
pub struct Address {
    ip: String,
    port: String,
}

impl Address {
    pub fn to_string(&self) -> String {
		let mut addr_str = String::from("http://");
		addr_str.push_str(&self.ip);
		addr_str.push(':');
		addr_str.push_str(&self.port);
		addr_str
    }
}

impl PartialEq for Address {
    fn eq(&self, other: &Self) -> bool {
        self.ip == other.ip && self.port == other.port
    }
}

pub enum Command {
    Init {
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    InitAsLeader {
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    // SendMembership,
    // GetMembership,
    // SendEntries,
    // GetEntries,
    // CheckTerm,
    // SendVoteReq,
    // GetVoteReq,
    // Execute,
    GetAddress {
        result_sender: oneshot::Sender<Address>,
    },
    GetType {
        result_sender: oneshot::Sender<NodeType>,
    },
    GetLeader {
        result_sender: oneshot:: Sender<Address>,
    },
    AddMember {
        address: Address,
        result_sender: oneshot::Sender<Result<(),()>>,
    },
    ChangeAddress {
        ip: String,
        result_sender: oneshot::Sender<Result<(),()>>,
    },
}

pub struct Node {
    receiver: mpsc::Receiver<Command>,
    address: Address,               // Node Address
    node_type: NodeType,            // leader, candidate , follower
    log: Vec<Log>,                  // Stores Logs
    data: HashMap<String, String>,  // Key value pair data
    cluster_addr_list: Vec<Address>, // Address List Cluster
    cluster_leader_addr: Address,   // Current Leader Address
    election_term: i32,             // Current Election Term
    voted_for: Option<Address>,     // Voting ID
    commit_index: i32,              // Last Commit Index
    last_applied: i32,              // Highes Log Entry Index
    next_index: Vec<i32>,           // Expected Next Index Log
    match_index: Vec<i32>,          // ndex of highest log entry known to be replicated on server
}


impl Node {
    fn new(
        receiver: mpsc::Receiver<Command>,
        address: Address,
        node_type: NodeType,
        cluster_leader_addr: Address,
    ) -> Self {
        let mut final_voted_for: Option<Address> = None;
        if address == cluster_leader_addr {
            final_voted_for = Some(address.clone());
        }
        Node {
            receiver,
            address,
            node_type,
            log: Vec::new(),
            data: HashMap::new(),
            cluster_addr_list: Vec::new(),
            cluster_leader_addr,
            election_term: 0,
            voted_for: final_voted_for,
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::new(),
            match_index: Vec::new(),
        }
    }
    async fn handle_command(&mut self, msg: Command) {
        match msg {
            Command::Init { result_sender } => {
                println!("test1");
                let mut client = RaftServiceClient::connect(self.cluster_leader_addr.to_string()).await.expect("cannot find contact ip address");

                let request = tonic::Request::new(MembershipRequest {
                    ip_addr: (self.address.ip.clone()),
                    port: (self.address.port.clone()),
                });

                let mut response = client.membership(request).await;
				let mut status = !response.as_mut().unwrap().get_ref().status;

				while status {
					self.cluster_leader_addr = Address {
						ip: response.as_mut().unwrap().get_ref().ip_addr.clone(),
						port:  response.as_mut().unwrap().get_ref().port.clone(),
					};

					client = RaftServiceClient::connect(self.cluster_leader_addr.to_string()).await.expect("cannot find contact ip address");
					let request = tonic::Request::new(MembershipRequest {
						ip_addr: (self.address.ip.clone()),
						port: (self.address.port.clone()),
					});

					response = client.membership(request).await;
					status = !response.as_mut().unwrap().get_ref().status;
					println!("{}",status);
				}

                println!("Init successful");
                let _ = result_sender.send(Ok(()));

            }
            Command::InitAsLeader { result_sender } => {
                self.cluster_addr_list.push(self.address.clone());
                println!("Init as leader successful");
                let _ = result_sender.send(Ok(()));
            }
            Command::GetType { result_sender } => {
                let _ = result_sender.send(self.node_type.clone());
            }
            Command::GetLeader { result_sender } => {
                let _ = result_sender.send(self.cluster_leader_addr.clone());
            }

            Command::GetAddress { result_sender } => {
                let _ = result_sender.send(self.address.clone());
            }
            Command::ChangeAddress { ip, result_sender } => {
                self.address.ip = ip;
                println!("Change is successful");
                let _ = result_sender.send(Ok(()));
            }

            Command::AddMember { address, result_sender } => {
                self.cluster_addr_list.push(address);
                println!("Add membership is successful");
                let _ = result_sender.send(Ok(()));
            }
        }
    }
}

async fn run_my_actor(mut actor: Node) {
    while let Some(msg) = actor.receiver.recv().await {
        actor.handle_command(msg).await;
    }
}

#[derive(Clone, Debug)]
pub struct MyActorHandle {
    sender: mpsc::Sender<Command>,
}

impl MyActorHandle {
    pub fn new(address: Address, node_type: NodeType, cluster_leader_addr: Address) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let actor = Node::new(receiver, address, node_type, cluster_leader_addr);
        tokio::spawn(run_my_actor(actor));
        Self { sender }
    }

    pub async fn init(&self) {
        let (send, recv) = oneshot::channel();
        let msg = Command::Init {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        let _ = recv.await.expect("Actor task has been killed");
    }

    pub async fn init_as_leader(&self) {
        let (send, recv) = oneshot::channel();
        let msg = Command::InitAsLeader {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        let _ = recv.await.expect("Actor task has been killed");
    }

    pub async fn get_address(&self) -> Address {
        let (send, recv) = oneshot::channel();
        let msg = Command::GetAddress {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn change_address(&self, ip: String) {
        let (send, recv) = oneshot::channel();
        let msg = Command::ChangeAddress {
            ip,
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        let _ = recv.await.expect("Actor task has been killed");
    }

    pub async fn get_type(&self) -> NodeType {
        let (send, recv) = oneshot::channel();
        let msg = Command::GetType {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn get_leader(&self) -> Address {
        let (send, recv) = oneshot::channel();
        let msg = Command::GetLeader {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
    
    pub async fn add_member(&self, address: Address) {
        let (send, recv) = oneshot::channel();
        let msg = Command::AddMember {
            address,
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        let res = recv.await.expect("Actor task has been killed");
    }
}

// STRUCT IMPLEMENTATION
impl LogType {
    fn ping() {}
    fn get() {}
    fn set() {}
    fn append() {}
    fn del() {}
    fn strlen() {}
}

impl Log {
    // fn createLog(&data : String) {
    // 	let new_log : Log = serde_json::from_str(&data)?;
    // 	return new_log;
    // }
}

#[derive(Debug)]
pub struct MyRaftService {
    tx: tokio::sync::watch::Sender<i32>,
    handler: MyActorHandle,
}

#[derive(Debug, Default)]
pub struct Timeout {}

#[tonic::async_trait]
impl RaftService for MyRaftService {
    async fn client(
        &self,
        request: Request<ClientRequest>,
    ) -> Result<Response<ClientReply>, Status> {
        // println!("Got a request: {:?}", request);
        let reply = ClientReply {
            message: format!("Hello {}!", request.into_inner().name),
        };

        sleep(Duration::from_millis(100)).await;
        let _ = self.tx.send(1);

        Ok(Response::new(reply))
    }

    async fn append_entries(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendReply>, Status> {
        let reply = AppendReply {
            term: Some(0),
            success: Some(false),
        };
        Ok(Response::new(reply))
    }

    async fn vote_me(&self, request: Request<VoteRequest>) -> Result<Response<VoteReply>, Status> {
        let reply = VoteReply {
            term: Some(0),
            vote_granted: Some(false),
        };
        Ok(Response::new(reply))
    }

    async fn membership(
        &self,
        request: Request<MembershipRequest>,
    ) -> Result<Response<MembershipReply>, Status> {

        let leader_addr = self.handler.get_leader().await;

        let mut reply = MembershipReply {
            ip_addr: leader_addr.ip,
            port: leader_addr.port,
            status: true,
        };
		println!("{:?}",reply.ip_addr);


        if self.handler.get_type().await != NodeType::LEADER {
           reply.status=false; 
        } else {
            let new_addr = Address {
                ip: request.get_ref().ip_addr.clone(),
                port: request.get_ref().port.clone(),
            };
            self.handler.add_member(new_addr).await;
        }

		println!("{}",reply.status);
        Ok(Response::new(reply))
    }
}

async fn receiver(
    tx: tokio::sync::watch::Sender<i32>,
    actor_handler: MyActorHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let ip_addr = &args[1];
    let port = &args[2];
    let mut addr_str = String::from("");
    addr_str.push_str(ip_addr);
    addr_str.push(':');
    addr_str.push_str(port);
    let addr = addr_str.parse()?;
    let raft_service = MyRaftService {
        tx,
        handler: actor_handler,
    };

    Server::builder()
        .add_service(RaftServiceServer::new(raft_service))
        .serve(addr)
        .await?;

    Ok(())
}

async fn sender(
    rx: &mut tokio::sync::watch::Receiver<i32>,
    actor_handler: MyActorHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    if actor_handler.get_type().await == NodeType::LEADER{
        actor_handler.init_as_leader().await;
    } else {
        actor_handler.init().await;
    }
    loop {
        match timeout(Duration::from_millis(5000), rx.changed()).await {
            Ok(_) => println!("not time out yayyy"),
            Err(_) => println!("time out brow"),
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("Brow aint no way salah input: liat readme pls")
    }

    let my_address = Address {
        ip: args[1].clone(),
        port: args[2].clone(),
    };


    if args.len() == 5 {
        let leader_address = Address {
            ip: args[3].clone(),
            port: args[4].clone(),
        };
        let my_actor_handle = MyActorHandle::new(my_address, NodeType::FOLLOWER, leader_address);
        let my_actor_handler_clone = my_actor_handle.clone();
        println!("{:?}", my_actor_handle.get_address().await);
        let (tx, mut rx) = watch::channel::<i32>(0);
        tokio::spawn(async {
            let _ = receiver(tx, my_actor_handle).await;
        });
        tokio::spawn(async move {
            let _ = sender(&mut rx, my_actor_handler_clone).await;
        });
        let ctrl_c = signal::ctrl_c();
        println!("Press Ctrl+C to exit...");
        ctrl_c.await.expect("Ctrl+C signal failed");
        println!("Ctrl+C received. Exiting...");
        Ok(())

    } else {
        let my_actor_handle = MyActorHandle::new(my_address.clone(), NodeType::LEADER, my_address);

        let my_actor_handler_clone = my_actor_handle.clone();

        println!("{:?}", my_actor_handle.get_address().await);

        let (tx, mut rx) = watch::channel::<i32>(0);

        tokio::spawn(async {
            let _ = receiver(tx, my_actor_handle).await;
        });
        tokio::spawn(async move {
            let _ = sender(&mut rx, my_actor_handler_clone).await;
        });
        let ctrl_c = signal::ctrl_c();
        println!("Press Ctrl+C to exit...");
        ctrl_c.await.expect("Ctrl+C signal failed");
        println!("Ctrl+C received. Exiting...");
        Ok(())
    }
    

}
