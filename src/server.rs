use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration, Instant};
use tonic::{transport::Server, Request, Response, Status};

use raft_service::raft_service_client::RaftServiceClient;
use raft_service::raft_service_server::{RaftService, RaftServiceServer};

use raft_service::{
    AppendReply, AppendRequest, CommandType, ExecuteReply, ExecuteRequest, LogEntry,
    MembershipReply, MembershipRequest, RequestLogReply, RequestLogRequest, VoteReply, VoteRequest,
};

pub mod raft_service {
    tonic::include_proto!("raft_service");
}

#[derive(Clone, Debug, PartialEq)]
enum Role {
    Leader,
    Candidate,
    Follower,
}

#[derive(Clone, Debug, PartialEq)]
struct Addr {
    ip: String,
    port: String,
    cluster_idx: i32,
}

impl Addr {
    fn uri(&self) -> String {
        format!("http://{}:{}", self.ip, self.port)
    }

    fn to_proto(&self) -> raft_service::Address {
        raft_service::Address {
            ip: self.ip.clone(),
            port: self.port.clone(),
            cluster_idx: self.cluster_idx,
        }
    }

    fn equals_no_idx(&self, other: &Addr) -> bool {
        self.ip == other.ip && self.port == other.port
    }
}

fn cmd_from_i32(v: i32) -> CommandType {
    CommandType::try_from(v).unwrap_or(CommandType::CmdUnknown)
}

fn last_log_term(log: &[LogEntry]) -> i32 {
    log.last().map(|e| e.term).unwrap_or(-1)
}

fn last_log_index(log: &[LogEntry]) -> i32 {
    (log.len() as i32) - 1
}

#[derive(Debug)]
struct State {
    me: Addr,
    is_member: bool,

    role: Role,
    current_term: i32,
    voted_for: Option<i32>,
    leader: Option<Addr>,

    cluster: Vec<Addr>,

    log: Vec<LogEntry>,
    commit_index: i32,
    last_applied: i32,
    kv: HashMap<String, String>,

    next_index: Vec<i32>,
    match_index: Vec<i32>,

    last_heartbeat: Instant,
}

impl State {
    fn majority(&self) -> usize {
        (self.cluster.len() / 2) + 1
    }

    fn recompute_cluster_indices(&mut self) {
        for (i, a) in self.cluster.iter_mut().enumerate() {
            a.cluster_idx = i as i32;
        }
        if let Some(pos) = self.cluster.iter().position(|a| a.equals_no_idx(&self.me)) {
            self.me.cluster_idx = pos as i32;
            self.is_member = true;
        } else {
            self.is_member = false;
            self.me.cluster_idx = -1;
        }

        if let Some(ld) = self.leader.clone() {
            if let Some(pos) = self.cluster.iter().position(|a| a.equals_no_idx(&ld)) {
                let mut new_ld = ld.clone();
                new_ld.cluster_idx = pos as i32;
                self.leader = Some(new_ld);
            }
        }
    }

    fn ensure_leader_arrays(&mut self) {
        let n = self.cluster.len();
        if self.next_index.len() != n {
            self.next_index = vec![self.log.len() as i32; n];
        }
        if self.match_index.len() != n {
            self.match_index = vec![-1; n];
        }
        if self.me.cluster_idx >= 0 {
            let i = self.me.cluster_idx as usize;
            if i < self.match_index.len() {
                self.match_index[i] = last_log_index(&self.log);
                self.next_index[i] = self.log.len() as i32;
            }
        }
    }

    fn append_entry(&mut self, cmd: CommandType, key: String, value: String) -> i32 {
        let entry = LogEntry {
            cmd: cmd as i32,
            key,
            value,
            term: self.current_term,
        };
        self.log.push(entry);
        last_log_index(&self.log)
    }

    fn apply_commits(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let i = self.last_applied as usize;
            if i >= self.log.len() {
                break;
            }
            let e = self.log[i].clone();
            match cmd_from_i32(e.cmd) {
                CommandType::CmdSet => {
                    self.kv.insert(e.key, e.value);
                }
                CommandType::CmdAppend => {
                    let old = self
                        .kv
                        .get(&e.key)
                        .cloned()
                        .unwrap_or_else(|| "".to_string());
                    self.kv.insert(e.key, format!("{}{}", old, e.value));
                }
                CommandType::CmdDel => {
                    self.kv.remove(&e.key);
                }
                CommandType::CmdRemoveMember => {
                    if let Ok(node_id) = e.value.parse::<i32>() {
                        if node_id >= 0 && (node_id as usize) < self.cluster.len() {
                            let remove_addr = self.cluster[node_id as usize].clone();
                            self.cluster.remove(node_id as usize);

                            if let Some(ld) = self.leader.clone() {
                                if ld.equals_no_idx(&remove_addr) {
                                    self.leader = None;
                                    if self.role == Role::Leader {
                                        self.role = Role::Follower;
                                    }
                                }
                            }

                            self.recompute_cluster_indices();

                            if !self.is_member {
                                self.role = Role::Follower;
                                self.voted_for = None;
                            }

                            self.next_index.truncate(self.cluster.len());
                            self.match_index.truncate(self.cluster.len());
                            self.ensure_leader_arrays();
                        }
                    }
                }
                _ => {
                }
            }
        }
    }

    fn advance_commit_index(&mut self) {
        let mut n = self.commit_index + 1;
        let last = last_log_index(&self.log);
        while n <= last {
            let mut count = 0usize;
            for (i, _) in self.cluster.iter().enumerate() {
                if i < self.match_index.len() && self.match_index[i] >= n {
                    count += 1;
                }
            }
            if count >= self.majority() && self.log[n as usize].term == self.current_term {
                self.commit_index = n;
            }
            n += 1;
        }
    }
}

#[derive(Clone)]
struct Shared {
    st: Arc<Mutex<State>>,
    repl_lock: Arc<Mutex<()>>,
}

#[derive(Clone)]
struct MyRaftService {
    shared: Shared,
}

impl MyRaftService {
    async fn redirect_reply(&self) -> ExecuteReply {
        let st = self.shared.st.lock().await;
        let leader = st
            .leader
            .clone()
            .unwrap_or_else(|| st.cluster.first().cloned().unwrap_or(st.me.clone()));
        ExecuteReply {
            ok: false,
            message: "NOT_LEADER".to_string(),
            redirect: true,
            leader: Some(leader.to_proto()),
        }
    }

    async fn redirect_log_reply(&self) -> RequestLogReply {
        let st = self.shared.st.lock().await;
        let leader = st
            .leader
            .clone()
            .unwrap_or_else(|| st.cluster.first().cloned().unwrap_or(st.me.clone()));
        RequestLogReply {
            ok: false,
            redirect: true,
            leader: Some(leader.to_proto()),
            log: vec![],
        }
    }

    async fn replicate_peer_to_end(
        shared: Shared,
        peer_idx: usize,
        need_index: i32,
        force_one: bool,
    ) -> bool {
        let mut sent_once = false;

        loop {
            if !force_one || sent_once {
                let st = shared.st.lock().await;
                if peer_idx < st.match_index.len() && st.match_index[peer_idx] >= need_index {
                    return true;
                }
            }

            let (peer_addr, req) = {
                let st = shared.st.lock().await;

                if st.role != Role::Leader {
                    return false;
                }
                if peer_idx >= st.cluster.len() {
                    return true;
                }
                if st.me.cluster_idx >= 0 && peer_idx == st.me.cluster_idx as usize {
                    return true;
                }

                let peer = st.cluster[peer_idx].clone();

                let next_i = st.next_index.get(peer_idx).cloned().unwrap_or(0);
                let prev_i = next_i - 1;

                let prev_term = if prev_i >= 0 && (prev_i as usize) < st.log.len() {
                    st.log[prev_i as usize].term
                } else {
                    -1
                };

                let entries = if next_i >= 0 && (next_i as usize) <= st.log.len() {
                    st.log[(next_i as usize)..].to_vec()
                } else {
                    vec![]
                };

                let cluster_config = st.cluster.iter().map(|a| a.to_proto()).collect::<Vec<_>>();

                let req = AppendRequest {
                    term: st.current_term,
                    leader_id: st.me.cluster_idx,
                    prev_log_index: prev_i,
                    prev_log_term: prev_term,
                    leader_commit: st.commit_index,
                    entries,
                    cluster_config,
                };

                (peer, req)
            };

            let mut client = match RaftServiceClient::connect(peer_addr.uri()).await {
                Ok(c) => c,
                Err(_) => return false,
            };

            let reply = match client.append_entries(Request::new(req)).await {
                Ok(r) => r.into_inner(),
                Err(_) => return false,
            };

            sent_once = true;

            {
                let mut st = shared.st.lock().await;

                if reply.term > st.current_term {
                    st.current_term = reply.term;
                    st.role = Role::Follower;
                    st.voted_for = None;
                    st.leader = None;
                    return false;
                }

                if st.role != Role::Leader {
                    return false;
                }

                if peer_idx >= st.cluster.len() {
                    return false;
                }

                if reply.success {
                    let new_match = last_log_index(&st.log);
                    st.match_index[peer_idx] = new_match;
                    st.next_index[peer_idx] = st.log.len() as i32;
                } else {
                    let ni = st.next_index.get(peer_idx).cloned().unwrap_or(0);
                    st.next_index[peer_idx] = (ni - 1).max(0);
                }
            }
        }
    }

    async fn replicate_to_majority(&self, need_index: i32) -> bool {
        let _guard = self.shared.repl_lock.lock().await;

        let peers: Vec<usize> = {
            let mut st = self.shared.st.lock().await;
            st.ensure_leader_arrays();
            (0..st.cluster.len()).collect()
        };

        {
            let mut st = self.shared.st.lock().await;
            if st.cluster.len() <= 1 {
                st.commit_index = st.commit_index.max(need_index);
                st.apply_commits();
                return true;
            }
        }

        let mut tasks = Vec::new();
        for idx in peers {
            let shared = self.shared.clone();
            tasks.push(tokio::spawn(async move {
                MyRaftService::replicate_peer_to_end(shared, idx, need_index, false).await
            }));
        }

        let mut ack = 0usize;
        for t in tasks {
            if let Ok(true) = t.await {
                ack += 1;
            }
        }

        let majority = {
            let st = self.shared.st.lock().await;
            st.majority()
        };

        if ack < majority {
            return false;
        }

        {
            let mut st = self.shared.st.lock().await;
            if st.role != Role::Leader {
                return false;
            }
            st.advance_commit_index();
            if st.commit_index >= need_index {
                st.apply_commits();
                return true;
            }
        }

        false
    }

    async fn send_heartbeat_round(shared: Shared) {
        let _guard = shared.repl_lock.lock().await;

        let is_leader = {
            let st = shared.st.lock().await;
            st.role == Role::Leader
        };
        if !is_leader {
            return;
        }

        let peers: Vec<usize> = {
            let mut st = shared.st.lock().await;
            st.ensure_leader_arrays();
            (0..st.cluster.len()).collect()
        };

        let mut tasks = Vec::new();
        for idx in peers {
            let shared2 = shared.clone();
            let need = {
                let st = shared2.st.lock().await;
                last_log_index(&st.log)
            };
            tasks.push(tokio::spawn(async move {
                let _ = MyRaftService::replicate_peer_to_end(shared2, idx, need, true).await;
            }));
        }

        for t in tasks {
            let _ = t.await;
        }

        {
            let mut st = shared.st.lock().await;
            if st.role == Role::Leader {
                st.advance_commit_index();
                st.apply_commits();
            }
        }
    }
}

#[tonic::async_trait]
impl RaftService for MyRaftService {
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteReply>, Status> {
        let req = request.into_inner();

        {
            let st = self.shared.st.lock().await;
            if !st.is_member {
                let leader = st
                    .leader
                    .clone()
                    .unwrap_or_else(|| st.cluster.first().cloned().unwrap_or(st.me.clone()));
                return Ok(Response::new(ExecuteReply {
                    ok: false,
                    message: "NOT_MEMBER".to_string(),
                    redirect: true,
                    leader: Some(leader.to_proto()),
                }));
            }
        }

        {
            let st = self.shared.st.lock().await;
            if st.role != Role::Leader {
                drop(st);
                return Ok(Response::new(self.redirect_reply().await));
            }
        }

        let cmd = cmd_from_i32(req.cmd);
        match cmd {
            CommandType::CmdGet => {
                let st = self.shared.st.lock().await;
                let v = st
                    .kv
                    .get(&req.key)
                    .cloned()
                    .unwrap_or_else(|| "".to_string());
                return Ok(Response::new(ExecuteReply {
                    ok: true,
                    message: v,
                    redirect: false,
                    leader: None,
                }));
            }

            CommandType::CmdPing => {
                let idx = {
                    let mut st = self.shared.st.lock().await;
                    st.append_entry(CommandType::CmdPing, "".to_string(), "".to_string())
                };
                let ok = self.replicate_to_majority(idx).await;
                if !ok {
                    return Ok(Response::new(ExecuteReply {
                        ok: false,
                        message: "FAILED_TO_COMMIT".to_string(),
                        redirect: false,
                        leader: None,
                    }));
                }
                return Ok(Response::new(ExecuteReply {
                    ok: true,
                    message: "PONG".to_string(),
                    redirect: false,
                    leader: None,
                }));
            }

            CommandType::CmdSet => {
                let idx = {
                    let mut st = self.shared.st.lock().await;
                    st.append_entry(CommandType::CmdSet, req.key.clone(), req.value.clone())
                };
                let ok = self.replicate_to_majority(idx).await;
                let msg = if ok { "Success" } else { "FAILED_TO_COMMIT" };
                return Ok(Response::new(ExecuteReply {
                    ok,
                    message: msg.to_string(),
                    redirect: false,
                    leader: None,
                }));
            }

            CommandType::CmdAppend => {
                let idx = {
                    let mut st = self.shared.st.lock().await;
                    st.append_entry(CommandType::CmdAppend, req.key.clone(), req.value.clone())
                };
                let ok = self.replicate_to_majority(idx).await;
                let msg = if ok { "Success" } else { "FAILED_TO_COMMIT" };
                return Ok(Response::new(ExecuteReply {
                    ok,
                    message: msg.to_string(),
                    redirect: false,
                    leader: None,
                }));
            }

            CommandType::CmdDel => {
                let idx = {
                    let mut st = self.shared.st.lock().await;
                    st.append_entry(CommandType::CmdDel, req.key.clone(), "".to_string())
                };
                let ok = self.replicate_to_majority(idx).await;
                let msg = if ok { "Success" } else { "FAILED_TO_COMMIT" };
                return Ok(Response::new(ExecuteReply {
                    ok,
                    message: msg.to_string(),
                    redirect: false,
                    leader: None,
                }));
            }

            CommandType::CmdStrlen => {
                let idx = {
                    let mut st = self.shared.st.lock().await;
                    st.append_entry(CommandType::CmdStrlen, req.key.clone(), "".to_string())
                };
                let ok = self.replicate_to_majority(idx).await;
                if !ok {
                    return Ok(Response::new(ExecuteReply {
                        ok: false,
                        message: "FAILED_TO_COMMIT".to_string(),
                        redirect: false,
                        leader: None,
                    }));
                }
                let st = self.shared.st.lock().await;
                let v = st
                    .kv
                    .get(&req.key)
                    .cloned()
                    .unwrap_or_else(|| "".to_string());
                return Ok(Response::new(ExecuteReply {
                    ok: true,
                    message: format!("{}", v.len()),
                    redirect: false,
                    leader: None,
                }));
            }

            CommandType::CmdRemoveMember => {
                let node_id_str = format!("{}", req.node_id);

                {
                    let st = self.shared.st.lock().await;
                    if req.node_id == st.me.cluster_idx {
                        return Ok(Response::new(ExecuteReply {
                            ok: false,
                            message: "CANNOT_REMOVE_SELF".to_string(),
                            redirect: false,
                            leader: None,
                        }));
                    }
                }

                let idx = {
                    let mut st = self.shared.st.lock().await;
                    st.append_entry(CommandType::CmdRemoveMember, "".to_string(), node_id_str)
                };

                let ok = self.replicate_to_majority(idx).await;
                let msg = if ok { "Success" } else { "FAILED_TO_COMMIT" };
                return Ok(Response::new(ExecuteReply {
                    ok,
                    message: msg.to_string(),
                    redirect: false,
                    leader: None,
                }));
            }

            _ => {
                return Ok(Response::new(ExecuteReply {
                    ok: false,
                    message: "UNIMPLEMENTED".to_string(),
                    redirect: false,
                    leader: None,
                }));
            }
        }
    }

    async fn request_log(
        &self,
        _request: Request<RequestLogRequest>,
    ) -> Result<Response<RequestLogReply>, Status> {
        {
            let st = self.shared.st.lock().await;
            if !st.is_member {
                let leader = st
                    .leader
                    .clone()
                    .unwrap_or_else(|| st.cluster.first().cloned().unwrap_or(st.me.clone()));
                return Ok(Response::new(RequestLogReply {
                    ok: false,
                    redirect: true,
                    leader: Some(leader.to_proto()),
                    log: vec![],
                }));
            }
        }

        {
            let st = self.shared.st.lock().await;
            if st.role != Role::Leader {
                drop(st);
                return Ok(Response::new(self.redirect_log_reply().await));
            }
        }

        let idx = {
            let mut st = self.shared.st.lock().await;
            st.append_entry(CommandType::CmdRequestLog, "".to_string(), "".to_string())
        };
        let ok = self.replicate_to_majority(idx).await;
        if !ok {
            return Ok(Response::new(RequestLogReply {
                ok: false,
                redirect: false,
                leader: None,
                log: vec![],
            }));
        }

        let st = self.shared.st.lock().await;
        Ok(Response::new(RequestLogReply {
            ok: true,
            redirect: false,
            leader: None,
            log: st.log.clone(),
        }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendReply>, Status> {
        let req = request.into_inner();

        let mut st = self.shared.st.lock().await;

        if req.term < st.current_term {
            return Ok(Response::new(AppendReply {
                term: st.current_term,
                success: false,
            }));
        }

        if req.term > st.current_term {
            st.current_term = req.term;
            st.role = Role::Follower;
            st.voted_for = None;
        }

        if !req.cluster_config.is_empty() {
            st.cluster = req
                .cluster_config
                .iter()
                .enumerate()
                .map(|(i, a)| Addr {
                    ip: a.ip.clone(),
                    port: a.port.clone(),
                    cluster_idx: i as i32,
                })
                .collect();
            st.recompute_cluster_indices();
        }

        if req.leader_id >= 0 && (req.leader_id as usize) < st.cluster.len() {
            st.leader = Some(st.cluster[req.leader_id as usize].clone());
        }

        st.last_heartbeat = Instant::now();

        st.role = Role::Follower;

        if req.prev_log_index >= 0 {
            let pli = req.prev_log_index as usize;
            if pli >= st.log.len() {
                return Ok(Response::new(AppendReply {
                    term: st.current_term,
                    success: false,
                }));
            }
            if st.log[pli].term != req.prev_log_term {
                return Ok(Response::new(AppendReply {
                    term: st.current_term,
                    success: false,
                }));
            }
        }

        let mut insert_at = (req.prev_log_index + 1) as usize;

        for incoming in req.entries.iter() {
            if insert_at < st.log.len() {
                if st.log[insert_at].term != incoming.term {
                    st.log.truncate(insert_at);
                    st.log.push(incoming.clone());
                } else {
                }
            } else {
                st.log.push(incoming.clone());
            }
            insert_at += 1;
        }

        let last_idx = last_log_index(&st.log);
        if req.leader_commit > st.commit_index {
            st.commit_index = std::cmp::min(req.leader_commit, last_idx);
        }
        st.apply_commits();

        Ok(Response::new(AppendReply {
            term: st.current_term,
            success: true,
        }))
    }

    async fn vote_me(&self, request: Request<VoteRequest>) -> Result<Response<VoteReply>, Status> {
        let req = request.into_inner();
        let mut st = self.shared.st.lock().await;

        if !st.is_member {
            return Ok(Response::new(VoteReply {
                term: st.current_term,
                vote_granted: false,
            }));
        }

        if req.term < st.current_term {
            return Ok(Response::new(VoteReply {
                term: st.current_term,
                vote_granted: false,
            }));
        }

        if req.term > st.current_term {
            st.current_term = req.term;
            st.voted_for = None;
            st.role = Role::Follower;
        }

        st.last_heartbeat = Instant::now();

        let my_last_term = last_log_term(&st.log);
        let my_last_idx = last_log_index(&st.log);

        let up_to_date = (req.last_log_term > my_last_term)
            || (req.last_log_term == my_last_term && req.last_log_index >= my_last_idx);

        let voted_ok = st.voted_for.is_none() || st.voted_for == Some(req.candidate_id);

        let grant = voted_ok && up_to_date;
        if grant {
            st.voted_for = Some(req.candidate_id);
        }

        Ok(Response::new(VoteReply {
            term: st.current_term,
            vote_granted: grant,
        }))
    }

    async fn membership(
        &self,
        request: Request<MembershipRequest>,
    ) -> Result<Response<MembershipReply>, Status> {
        let req = request.into_inner();
        let mut st = self.shared.st.lock().await;

        if st.role != Role::Leader {
            let leader = st
                .leader
                .clone()
                .unwrap_or_else(|| st.cluster.first().cloned().unwrap_or(st.me.clone()));
            return Ok(Response::new(MembershipReply {
                ok: false,
                leader: Some(leader.to_proto()),
                assigned_id: -1,
                cluster_config: vec![],
            }));
        }

        let new_addr = Addr {
            ip: req.ip_addr.clone(),
            port: req.port.clone(),
            cluster_idx: st.cluster.len() as i32,
        };

        if st.cluster.iter().any(|a| a.equals_no_idx(&new_addr)) {
            let assigned = st
                .cluster
                .iter()
                .position(|a| a.equals_no_idx(&new_addr))
                .unwrap() as i32;
            let cfg = st.cluster.iter().map(|a| a.to_proto()).collect::<Vec<_>>();
            return Ok(Response::new(MembershipReply {
                ok: true,
                leader: Some(st.me.to_proto()),
                assigned_id: assigned,
                cluster_config: cfg,
            }));
        }

        st.cluster.push(new_addr);
        st.recompute_cluster_indices();
        st.ensure_leader_arrays();

        let assigned_id = (st.cluster.len() as i32) - 1;
        let cfg = st.cluster.iter().map(|a| a.to_proto()).collect::<Vec<_>>();

        Ok(Response::new(MembershipReply {
            ok: true,
            leader: Some(st.me.to_proto()),
            assigned_id,
            cluster_config: cfg,
        }))
    }
}

async fn join_cluster(shared: Shared, seed_leader: Addr) {
    loop {
        let me = {
            let st = shared.st.lock().await;
            st.me.clone()
        };

        let mut leader = seed_leader.clone();

        let conn = RaftServiceClient::connect(leader.uri()).await;
        let mut client = match conn {
            Ok(c) => c,
            Err(_) => {
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        let resp = client
            .membership(Request::new(MembershipRequest {
                ip_addr: me.ip.clone(),
                port: me.port.clone(),
            }))
            .await;

        let reply = match resp {
            Ok(r) => r.into_inner(),
            Err(_) => {
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        if !reply.ok {
            if let Some(ld) = reply.leader {
                leader = Addr {
                    ip: ld.ip,
                    port: ld.port,
                    cluster_idx: ld.cluster_idx,
                };
                sleep(Duration::from_millis(200)).await;
                continue;
            } else {
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        }

        {
            let mut st = shared.st.lock().await;
            st.cluster = reply
                .cluster_config
                .iter()
                .enumerate()
                .map(|(i, a)| Addr {
                    ip: a.ip.clone(),
                    port: a.port.clone(),
                    cluster_idx: i as i32,
                })
                .collect();

            st.recompute_cluster_indices();
            st.is_member = true;

            if let Some(ld) = reply.leader {
                st.leader = Some(Addr {
                    ip: ld.ip,
                    port: ld.port,
                    cluster_idx: ld.cluster_idx,
                });
            } else {
                st.leader = Some(leader.clone());
            }

            st.role = Role::Follower;
            st.last_heartbeat = Instant::now();
        }

        println!("Joined cluster successfully.");
        return;
    }
}

async fn election_task(shared: Shared) {
    let mut timeout_ms: u64 = rand::random_range(4500..5000);

    loop {
        sleep(Duration::from_millis(50)).await;

        let (me_idx, peers, last_term, last_idx, majority) = {
            let st = shared.st.lock().await;

            if !st.is_member {
                continue;
            }

            if st.role == Role::Leader {
                continue;
            }

            let elapsed = st.last_heartbeat.elapsed().as_millis() as u64;
            if elapsed < timeout_ms {
                continue;
            }

            (
                st.me.cluster_idx,
                st.cluster.clone(),
                last_log_term(&st.log),
                last_log_index(&st.log),
                st.majority(),
            )
        };

        {
            let mut st = shared.st.lock().await;
            st.role = Role::Candidate;
            st.current_term += 1;
            st.voted_for = Some(st.me.cluster_idx);
            st.last_heartbeat = Instant::now();
        }

        let my_term = {
            let st = shared.st.lock().await;
            st.current_term
        };

        let mut tasks = Vec::new();
        for p in peers.iter() {
            if me_idx >= 0 && p.cluster_idx == me_idx {
                continue;
            }
            let addr = p.clone();
            let shared2 = shared.clone();
            let req = VoteRequest {
                term: my_term,
                candidate_id: me_idx,
                last_log_index: last_idx,
                last_log_term: last_term,
            };

            tasks.push(tokio::spawn(async move {
                let mut client = RaftServiceClient::connect(addr.uri()).await.ok()?;
                let r = client.vote_me(Request::new(req)).await.ok()?.into_inner();

                {
                    let mut st = shared2.st.lock().await;
                    if r.term > st.current_term {
                        st.current_term = r.term;
                        st.role = Role::Follower;
                        st.voted_for = None;
                        st.leader = None;
                    }
                }

                Some(r.vote_granted)
            }));
        }

        let mut votes = 1usize;
        for t in tasks {
            if let Ok(Some(true)) = t.await {
                votes += 1;
            }
        }

        {
            let mut st = shared.st.lock().await;

            if st.role != Role::Candidate || st.current_term != my_term {
                timeout_ms = rand::random_range(4500..5000);
                continue;
            }

            if votes >= majority {
                st.role = Role::Leader;
                st.leader = Some(st.me.clone());
                st.ensure_leader_arrays();
                st.last_heartbeat = Instant::now();
                println!(
                    "Elected as LEADER. term={} votes={}",
                    st.current_term, votes
                );
            } else {
                st.role = Role::Follower;
                st.voted_for = None;
                st.last_heartbeat = Instant::now();
                println!("Election failed. term={} votes={}", st.current_term, votes);
            }
        }

        timeout_ms = rand::random_range(4500..5000);
    }
}

async fn heartbeat_task(shared: Shared) {
    loop {
        sleep(Duration::from_millis(1000)).await;
        MyRaftService::send_heartbeat_round(shared.clone()).await;
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 && args.len() != 5 {
        panic!("Usage:\n  server <ip> <port> [leader_ip leader_port]");
    }

    let ip = args[1].clone();
    let port = args[2].clone();

    let me = Addr {
        ip: ip.clone(),
        port: port.clone(),
        cluster_idx: -1,
    };

    let is_leader_start = args.len() == 3;
    let seed_leader = if !is_leader_start {
        Some(Addr {
            ip: args[3].clone(),
            port: args[4].clone(),
            cluster_idx: -1,
        })
    } else {
        None
    };

    let initial_cluster = if is_leader_start {
        vec![Addr {
            ip: ip.clone(),
            port: port.clone(),
            cluster_idx: 0,
        }]
    } else {
        vec![]
    };

    let state = State {
        me: if is_leader_start {
            Addr {
                ip: ip.clone(),
                port: port.clone(),
                cluster_idx: 0,
            }
        } else {
            me.clone()
        },
        is_member: is_leader_start,
        role: if is_leader_start {
            Role::Leader
        } else {
            Role::Follower
        },
        current_term: if is_leader_start { 1 } else { 0 },
        voted_for: if is_leader_start { Some(0) } else { None },
        leader: if is_leader_start {
            Some(Addr {
                ip: ip.clone(),
                port: port.clone(),
                cluster_idx: 0,
            })
        } else {
            seed_leader.clone()
        },
        cluster: initial_cluster,
        log: vec![],
        commit_index: -1,
        last_applied: -1,
        kv: HashMap::new(),
        next_index: vec![],
        match_index: vec![],
        last_heartbeat: Instant::now(),
    };

    let shared = Shared {
        st: Arc::new(Mutex::new(state)),
        repl_lock: Arc::new(Mutex::new(())),
    };

    if let Some(ld) = seed_leader {
        let shared2 = shared.clone();
        tokio::spawn(async move {
            join_cluster(shared2, ld).await;
        });
    }

    tokio::spawn(election_task(shared.clone()));
    tokio::spawn(heartbeat_task(shared.clone()));

    let svc = MyRaftService { shared };

    let addr = format!("{}:{}", ip, port).parse()?;
    tokio::spawn(async move {
        Server::builder()
            .add_service(RaftServiceServer::new(svc))
            .serve(addr)
            .await
            .unwrap();
    });

    println!("Server running on {}:{}", ip, port);
    println!("Press Ctrl+C to exit...");
    signal::ctrl_c().await?;
    println!("Ctrl+C received. Exiting...");
    Ok(())
}
