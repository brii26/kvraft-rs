use raft_service::raft_service_client::RaftServiceClient;
use raft_service::raft_service_server::{RaftService, RaftServiceServer};

use raft_service::{
    AppendReply, AppendRequest, ClientReply, ClientRequest, MembershipReply, MembershipRequest,
    VoteReply, VoteRequest,
};
use rand::{random_range, Rng};
use std::collections::HashMap;
use std::env;
use std::process::exit;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::Barrier;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;
use tokio::time::{sleep, Duration};
use tonic::{transport::Server, Request, Response, Status};

use crate::raft_service::Entries;

pub mod raft_service {
    tonic::include_proto!("raft_service");
}

// STRUCT
#[derive(PartialEq, Clone)]
enum LogType {
    PING,
    GET,
    SET,
    APPEND,
    DEL,
    STRLEN,
}

#[derive(PartialEq, Clone)]
pub struct Log {
    log_type: LogType,
    key: String,
    val: String,
    term: i32,
}

pub fn check_up_to_date(src: Vec<Log>, lastlogindex: i32, lastlogterm: i32) -> bool {
    println!("debug21");
    if src.len() == 0 {
        println!("debug 211");
        return true;
    }
    println!("debug22");
    let last_log = src[src.len() - 1].clone();
    println!("debug23");
    if last_log.term > lastlogterm {
        println!("debug 231");
        return false;
    }
    println!("debug24");
    if src.len() as i32 - 1 > lastlogindex {
        println!("debug 241");
        return false;
    }
    println!("debug25");
    true
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
    cluster_idx: i32,
}

impl Address {
    pub fn to_string(&self) -> String {
        let mut addr_str = String::from("http://");
        addr_str.push_str(&self.ip);
        addr_str.push(':');
        addr_str.push_str(&self.port);
        addr_str
    }

    pub fn to_proto_address(&self) -> raft_service::Address {
        raft_service::Address {
            ip: self.ip.clone(),
            port: self.port.clone(),
            cluster_idx: self.cluster_idx.clone(),
        }
    }

    pub fn equals_to(&self, other: &Address) -> bool {
        &self.ip == &other.ip && &self.port == &other.port
    }
}

pub fn get_index(list: Vec<Address>, address: Address) -> i32 {
    list.iter()
        .position(|n| n.equals_to(&address))
        .map(|i| i.try_into().unwrap())
        .unwrap_or(-1)
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
    SendEntries {
        index: i32,
        result_sender: oneshot::Sender<bool>,
    },
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
        result_sender: oneshot::Sender<Address>,
    },
    AddMember {
        address: Address,
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    ChangeAddress {
        ip: String,
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    ChangeEntries {
        entries: Vec<Entries>,
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    UpdateConverted {
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    GetTerm {
        result_sender: oneshot::Sender<i32>,
    },
    GetClusterCount {
        result_sender: oneshot::Sender<i32>,
    },
    PrepareVote {
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    UpdateCluster {
        list: Vec<raft_service::Address>,
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    ResetCtr {
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    IncCtr {
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    GetCtr {
        result_sender: oneshot::Sender<i32>,
    },
    SendVote {
        index: i32,
        result_sender: oneshot::Sender<bool>,
    },
    SyncCluster {
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    ChangeType {
        new_type: NodeType,
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    SetTerm {
        term: i32,
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    GetLog {
        result_sender: oneshot::Sender<Vec<Log>>,
    },
    GetVotedFor {
        result_sender: oneshot::Sender<Option<Address>>,
    },
    SetVotedFor {
        cluster_idx: i32,
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
}

pub struct Node {
    receiver: mpsc::Receiver<Command>,
    address: Address,                // Node Address
    node_type: NodeType,             // leader, candidate , follower
    log: Vec<Log>,                   // Stores Logs
    data: HashMap<String, String>,   // Key value pair data
    cluster_addr_list: Vec<Address>, // Address List Cluster
    cluster_leader_addr: Address,    // Current Leader Address
    election_term: i32,              // Current Election Term
    voted_for: Option<Address>,      // Voting ID
    commit_index: i32,               // Last Commit Index
    last_applied: i32,               // Highes Log Entry Index
    next_index: Vec<i32>,            // Expected Next Index Log
    match_index: Vec<i32>,           // ndex of highest log entry known to be replicated on server
    entries_to_send: Vec<Entries>,
    converted: Vec<raft_service::Address>,
    ok_response: i32,
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
            entries_to_send: Vec::new(),
            converted: Vec::new(),
            ok_response: 0,
        }
    }

    async fn handle_command(&mut self, msg: Command) {
        match msg {
            Command::Init { result_sender } => {
                loop {
                    let conn =
                        RaftServiceClient::connect(self.cluster_leader_addr.to_string()).await;
                    match conn {
                        Ok(mut client) => {
                            let request = tonic::Request::new(MembershipRequest {
                                ip_addr: (self.address.ip.clone()),
                                port: (self.address.port.clone()),
                            });
                            let mut response = client.membership(request).await;
                            let status = response.as_mut().unwrap().get_ref().status;

                            if status {
                                break;
                            }

                            self.cluster_leader_addr = Address {
                                ip: response.as_mut().unwrap().get_ref().ip_addr.clone(),
                                port: response.as_mut().unwrap().get_ref().port.clone(),
                                cluster_idx: -1,
                            };
                        }
                        Err(_) => {
                            panic!("Cannot connect to ip address :(");
                        }
                    }
                }

                println!("Init successful");
                let _ = result_sender.send(Ok(()));
            }
            Command::InitAsLeader { result_sender } => {
                self.cluster_addr_list.push(self.address.clone());
                self.election_term = 1;
                println!("Init as leader successful");
                let _ = result_sender.send(Ok(()));
            }

            Command::SendEntries {
                result_sender,
                index,
            } => match self.cluster_addr_list.get(index as usize) {
                Some(x) => {
                    // print!(
                    //     "leader's cluster index: {:?}",
                    //     self.cluster_leader_addr.cluster_idx
                    // );
                    if index == self.cluster_leader_addr.cluster_idx {
                        let _ = result_sender.send(true);
                    } else {
                        match self.log.last() {
                            Some(lastlog) => {
                                let mut client = RaftServiceClient::connect(x.to_string())
                                    .await
                                    .expect("cannot find ip addr");
                                let request = tonic::Request::new(AppendRequest {
                                    term: self.election_term,
                                    leader_id: self.cluster_leader_addr.cluster_idx,
                                    prev_log_index: self.log.len() as i32 - 1,
                                    prev_log_term: lastlog.term,
                                    leader_commit: self.commit_index,
                                    entries: self.entries_to_send.clone(),
                                    cluster_addr_list: self.converted.clone(),
                                });
                                let response = client.append_entries(request).await;
                                let _ = result_sender.send(response.unwrap().get_ref().success);
                            }
                            None => {
                                let mut client = RaftServiceClient::connect(x.to_string())
                                    .await
                                    .expect("cannot find ip addr");
                                let request = tonic::Request::new(AppendRequest {
                                    term: self.election_term,
                                    leader_id: self.cluster_leader_addr.cluster_idx,
                                    prev_log_index: self.log.len() as i32 - 1,
                                    prev_log_term: -1,
                                    leader_commit: self.commit_index,
                                    entries: self.entries_to_send.clone(),
                                    cluster_addr_list: self.converted.clone(),
                                });
                                let response = client.append_entries(request).await;
                                let _ = result_sender.send(response.unwrap().get_ref().success);
                            }
                        }
                    }
                }
                None => {
                    println!("index not found1");
                    let _ = result_sender.send(true);
                }
            },

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

            Command::AddMember {
                address,
                result_sender,
            } => {
                self.cluster_addr_list.push(address);
                println!("Add membership is successful");
                let _ = result_sender.send(Ok(()));
            }
            Command::ChangeEntries {
                entries,
                result_sender,
            } => {
                self.entries_to_send = entries;
                let _ = result_sender.send(Ok(()));
            }
            Command::UpdateConverted { result_sender } => {
                self.converted = self
                    .cluster_addr_list
                    .iter()
                    .map(|x| x.to_proto_address())
                    .collect();
                let _ = result_sender.send(Ok(()));
            }
            Command::GetTerm { result_sender } => {
                let _ = result_sender.send(self.election_term);
            }
            Command::GetClusterCount { result_sender } => {
                let _ = result_sender.send(self.cluster_addr_list.len() as i32);
            }
            Command::PrepareVote { result_sender } => {
                self.election_term += 1;
                let _ = result_sender.send(Ok(()));
            }
            Command::UpdateCluster {
                list,
                result_sender,
            } => {
                self.cluster_addr_list = list
                    .iter()
                    .map(|x| Address {
                        ip: x.ip.clone(),
                        port: x.port.clone(),
                        cluster_idx: x.cluster_idx.clone(),
                    })
                    .collect();
                let _ = result_sender.send(Ok(()));
            }
            Command::ResetCtr { result_sender } => {
                self.ok_response = 0;
                let _ = result_sender.send(Ok(()));
            }
            Command::IncCtr { result_sender } => {
                self.ok_response += 1;
                let _ = result_sender.send(Ok(()));
            }
            Command::GetCtr { result_sender } => {
                let _ = result_sender.send(self.ok_response);
            }
            Command::SendVote {
                index,
                result_sender,
            } => match self.cluster_addr_list.get(index as usize) {
                Some(x) => match self.log.last() {
                    Some(lastlog) => {
                        let mut client = RaftServiceClient::connect(x.to_string())
                            .await
                            .expect("cannot find ip addr");
                        let request = tonic::Request::new(VoteRequest {
                            term: self.election_term,
                            candidate_id: self.address.cluster_idx,
                            last_log_index: self.log.len() as i32 - 1,
                            last_log_term: lastlog.term,
                        });
                        let response = client.vote_me(request).await;
                        let _ = result_sender.send(response.unwrap().get_ref().vote_granted);
                    }
                    None => {
                        let conn = RaftServiceClient::connect(x.to_string()).await;
                        println!("{:?}", conn);
                        match conn {
                            Ok(mut client) => {
                                let request = tonic::Request::new(VoteRequest {
                                    term: self.election_term,
                                    candidate_id: self.address.cluster_idx,
                                    last_log_index: self.log.len() as i32 - 1,
                                    last_log_term: self.election_term - 1,
                                });
                                let response = client.vote_me(request).await;
                                let _ =
                                    result_sender.send(response.unwrap().get_ref().vote_granted);
                            }
                            Err(_) => {
                                let _ = result_sender.send(false);
                            }
                        }
                    }
                },
                None => {}
            },
            Command::SyncCluster { result_sender } => {
                for i in 0..self.cluster_addr_list.len() {
                    // println!(
                    //     "bool: {:?}",
                    //     self.cluster_addr_list[i] == self.cluster_leader_addr
                    // );
                    self.cluster_addr_list[i].cluster_idx = i as i32;
                    if self.cluster_addr_list[i] == self.address {
                        self.address.cluster_idx = i as i32;
                    }
                    if self.cluster_addr_list[i] == self.cluster_leader_addr {
                        self.cluster_leader_addr.cluster_idx = i as i32;
                    }
                }
                // println!("{:?}", self.cluster_addr_list);
                let _ = result_sender.send(Ok(()));
            }
            Command::ChangeType {
                new_type,
                result_sender,
            } => {
                self.node_type = new_type;
                let _ = result_sender.send(Ok(()));
            }
            Command::SetTerm {
                term,
                result_sender,
            } => {
                self.election_term = term;
                let _ = result_sender.send(Ok(()));
            }
            Command::GetLog { result_sender } => {
                let _ = result_sender.send(self.log.clone());
            }
            Command::GetVotedFor { result_sender } => {
                let _ = result_sender.send(self.voted_for.clone());
            }
            Command::SetVotedFor {
                cluster_idx,
                result_sender,
            } => {
                let address = self.cluster_addr_list[cluster_idx as usize].clone();
                self.voted_for = Some(address);
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
        println!("debug11");
        let (send, recv) = oneshot::channel();
        println!("debug12");
        let msg = Command::GetAddress {
            result_sender: send,
        };
        println!("debug13");
        let _ = self.sender.send(msg).await;
        println!("debug14");
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
        let _ = recv.await.expect("Actor task has been killed");
    }

    pub async fn send_entries(&self, index: i32) -> bool {
        let (send, recv) = oneshot::channel();
        let msg = Command::SendEntries {
            result_sender: (send),
            index,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn change_entries(&self, entries: Vec<Entries>) {
        let (send, recv) = oneshot::channel();
        let msg = Command::ChangeEntries {
            entries: entries,
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        let _ = recv.await.expect("Actor task has been killed");
    }

    pub async fn update_converted(&self) {
        let (send, recv) = oneshot::channel();
        let msg = Command::UpdateConverted {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        let _ = recv.await.expect("Actor task has been killed");
    }

    pub async fn get_term(&self) -> i32 {
        let (send, recv) = oneshot::channel();
        let msg = Command::GetTerm {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
    pub async fn get_cluster_count(&self) -> i32 {
        let (send, recv) = oneshot::channel();
        let msg = Command::GetClusterCount {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
    pub async fn prepare_vote(&self) {
        let (send, recv) = oneshot::channel();
        let msg = Command::PrepareVote {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        let _ = recv.await.expect("Actor task has been killed");
    }
    pub async fn update_cluster(&self, list: Vec<raft_service::Address>) {
        let (send, recv) = oneshot::channel();
        let msg = Command::UpdateCluster {
            list,
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        let _ = recv.await.expect("Actor task has been killed");
    }
    pub async fn reset_ctr(&self) {
        let (send, recv) = oneshot::channel();
        let msg = Command::ResetCtr {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        let _ = recv.await.expect("Actor task has been killed");
    }
    pub async fn inc_ctr(&self) {
        let (send, recv) = oneshot::channel();
        let msg = Command::IncCtr {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        let _ = recv.await.expect("Actor task has been killed");
    }
    pub async fn get_ctr(&self) -> i32 {
        let (send, recv) = oneshot::channel();
        let msg = Command::GetCtr {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
    pub async fn sync_cluster(&self) {
        let (send, recv) = oneshot::channel();
        let msg = Command::SyncCluster {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        let _ = recv.await.expect("Actor task has been killed");
    }
    pub async fn change_type(&self, new_type: NodeType) {
        let (send, recv) = oneshot::channel();
        let msg = Command::ChangeType {
            new_type,
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        let _ = recv.await.expect("Actor task has been killed");
    }
    pub async fn set_term(&self, term: i32) {
        let (send, recv) = oneshot::channel();
        let msg = Command::SetTerm {
            term,
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        let _ = recv.await.expect("Actor task has been killed");
    }
    pub async fn get_log(&self) -> Vec<Log> {
        let (send, recv) = oneshot::channel();
        let msg = Command::GetLog {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
    pub async fn get_voted_for(&self) -> Option<Address> {
        let (send, recv) = oneshot::channel();
        let msg = Command::GetVotedFor {
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
    pub async fn set_voted_for(&self, cluster_idx: i32) {
        let (send, recv) = oneshot::channel();
        let msg = Command::SetVotedFor {
            cluster_idx,
            result_sender: send,
        };
        let _ = self.sender.send(msg).await;
        let _ = recv.await.expect("Actor task has been killed");
    }
    pub async fn send_vote(&self, index: i32) -> bool {
        let (send, recv) = oneshot::channel();
        let msg = Command::SendVote {
            result_sender: (send),
            index,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
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
        // TODO: INI JUGA HARUS FIX SUCCESS HARDCODED JADI TRUE
        let request_raw = request.get_ref();
        if self.handler.get_term().await > request_raw.term {
            let reply = AppendReply {
                term: self.handler.get_term().await,
                success: false,
            };
            Ok(Response::new(reply))
        } else {
            self.handler.change_type(NodeType::FOLLOWER).await;
            self.handler.set_term(request_raw.term).await;

            self.handler
                .update_cluster(request_raw.cluster_addr_list.clone())
                .await;
            if request_raw.entries.len() == 0 {
                let reply = AppendReply {
                    term: self.handler.get_term().await,
                    success: true,
                };
                let _ = self.tx.send(1);
                Ok(Response::new(reply))
            } else {
                //TODO: cek flush commit index
                let current_term = self.handler.get_term().await;

                let reply = AppendReply {
                    term: current_term,
                    success: true,
                };
                let _ = self.tx.send(1);
                Ok(Response::new(reply))
            }
        }
    }

    async fn vote_me(&self, request: Request<VoteRequest>) -> Result<Response<VoteReply>, Status> {
        let request_raw = request.get_ref();
        println!("dapet vote me niii: {:?}", request_raw);

        println!("cek: {:?}", self.handler);

        println!(
            "my cluster idx: {}",
            self.handler.get_address().await.cluster_idx
        );

        let check_is_myself =
            request_raw.candidate_id == self.handler.get_address().await.cluster_idx;
        println!("debug3");

        let check_is_up_to_date = check_up_to_date(
            self.handler.get_log().await,
            request_raw.last_log_index,
            request_raw.last_log_term,
        );
        println!("debug4");

        let check_voted_for = self.handler.get_term().await <= request_raw.term;
        println!("debug5");

        if (check_voted_for && check_is_up_to_date) || check_is_myself {
            self.handler.set_term(request_raw.term).await;
            self.handler.set_voted_for(request_raw.candidate_id).await;
        }
        println!("debug6");

        let reply = VoteReply {
            term: self.handler.get_term().await,
            vote_granted: (check_voted_for && check_is_up_to_date) || check_is_myself,
        };

        println!("niii replynyaa: {:?}", reply);
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
            id: -1,
        };

        if self.handler.get_type().await != NodeType::LEADER {
            reply.status = false;
        } else {
            reply.id = self.handler.get_cluster_count().await;
            let new_addr = Address {
                cluster_idx: self.handler.get_cluster_count().await,
                ip: request.get_ref().ip_addr.clone(),
                port: request.get_ref().port.clone(),
            };
            self.handler.add_member(new_addr).await;
        }

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
    if actor_handler.get_type().await == NodeType::LEADER {
        actor_handler.init_as_leader().await;
    } else {
        actor_handler.init().await;
    }
    loop {
        match actor_handler.get_type().await {
            NodeType::LEADER => {
                match timeout(Duration::from_millis(1000), rx.changed()).await {
                    Ok(_) => println!("this is not supposed to happen btw"),
                    Err(_) => {
                        println!("heartbeat time baby!");
                        actor_handler.change_entries(Vec::new()).await;
                        actor_handler.update_converted().await;
                        actor_handler.sync_cluster().await;
                        actor_handler.reset_ctr().await;

                        let threads: Vec<_> = (0..actor_handler.get_cluster_count().await)
                            .map(|i| {
                                // TODO: INI NANTI DI SEND ENTRIES NYA BENERAN HRS HANDLE PEMBUATAN
                                // ENTRY SAMPAI HANDLE LOG INCONSISTENCY (DENGAN LOOP)
                                // handle juga kalau termnya kita lebih kecil
                                let new_handler = actor_handler.clone();
                                tokio::spawn(async move {
                                    if new_handler.send_entries(i).await {
                                        new_handler.inc_ctr().await
                                    }
                                })
                            })
                            .collect();
                        // TODO: handle kalau udah mayoritas commmit, baru kita commit
                        for handle in threads {
                            let _ = handle.await.unwrap();
                        }
                        println!("Success responses: {}", actor_handler.get_ctr().await);
                    }
                }
            }
            NodeType::FOLLOWER => {
                match timeout(Duration::from_millis(5000), rx.changed()).await {
                    // TODO: KALAU TIMEOUT JADI CANDIDATEEE
                    Ok(_) => println!("not time out yayyy"),
                    Err(_) => {
                        println!("time out brow");
                        actor_handler.change_type(NodeType::CANDIDATE).await;
                    }
                }
            }
            NodeType::CANDIDATE => {
                actor_handler.reset_ctr().await;
                actor_handler.prepare_vote().await;
                actor_handler.sync_cluster().await;
                let mut task_one =
                    async || match timeout(Duration::from_millis(1234567), rx.changed()).await {
                        Ok(_) => {}
                        Err(_) => {
                            println!("damn vote time out, starting new one...");
                        }
                    };
                let task_two = async || {
                    // let cap = value.get_cluster_count().await as usize;
                    // let mut handles = Vec::with_capacity(cap);
                    // let barrier = Arc::new(cap);
                    //
                    // for i in 0..cap {
                    //     let c = barrier.clone();
                    //
                    //     handles.push(tokio::spawn(async move {
                    //         let new_handler = value.clone();
                    //         if new_handler.send_vote(i as i32).await {
                    //             new_handler.inc_ctr().await;
                    //         }
                    //         println!("gey")
                    //     }));
                    // }
                    let threads: Vec<_> = (0..actor_handler.get_cluster_count().await)
                        .map(|i| {
                            let new_handler = actor_handler.clone();
                            println!("muncul ok nih dari threadaldkfj: {:?}", i);
                            tokio::spawn(async move {
                                if new_handler.send_vote(i).await {
                                    new_handler.inc_ctr().await
                                }
                            })
                        })
                        .collect();
                    println!("lewat sini ga?");
                    for handle in threads {
                        println!("debug handle: {:?}", handle);
                        let _ = handle.await.unwrap();
                    }
                    println!("lewat sini ga2?");
                    if actor_handler.get_ctr().await > (actor_handler.get_cluster_count().await / 2)
                    {
                        actor_handler.change_type(NodeType::LEADER).await;
                    }
                    println!("lewat sini ga3?");
                };

                tokio::select! {
                    _ = task_one() => {
                        println!("one wins")
                    }
                    _ = task_two() => {
                        println!("two wins")
                    }

                }
            }
        };
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
        cluster_idx: -1,
    };

    if args.len() == 5 {
        let leader_address = Address {
            ip: args[3].clone(),
            port: args[4].clone(),
            cluster_idx: -1,
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
