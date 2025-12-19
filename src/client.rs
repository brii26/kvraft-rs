use raft_service::raft_service_client::RaftServiceClient;
use raft_service::{CommandType, ExecuteRequest, RequestLogRequest};
use std::env;

pub mod raft_service {
    tonic::include_proto!("raft_service");
}

fn tokenize(input: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '"' => {
                if in_quotes {
                    out.push(cur.clone());
                    cur.clear();
                    in_quotes = false;
                } else {
                    in_quotes = true;
                }
            }
            '\\' if in_quotes => {
                if let Some(n) = chars.next() {
                    cur.push(n);
                }
            }
            c if c.is_whitespace() && !in_quotes => {
                if !cur.is_empty() {
                    out.push(cur.clone());
                    cur.clear();
                }
            }
            _ => cur.push(c),
        }
    }

    if in_quotes {
        return Err("Unclosed quote (\")".to_string());
    }

    if !cur.is_empty() {
        out.push(cur);
    }

    Ok(out)
}

async fn connect(addr: &str) -> Result<RaftServiceClient<tonic::transport::Channel>, String> {
    RaftServiceClient::connect(addr.to_string())
        .await
        .map_err(|e| format!("Cannot connect to server: {e}"))
}

async fn exec_with_redirect(
    client: &mut Option<RaftServiceClient<tonic::transport::Channel>>,
    target_addr: &mut String,
    req: ExecuteRequest,
) -> Result<String, String> {
    for _ in 0..5 {
        if client.is_none() {
            *client = Some(connect(target_addr).await?);
        }
        let c = client.as_mut().unwrap();

        let resp = c.execute(tonic::Request::new(req.clone())).await;
        match resp {
            Ok(r) => {
                let r = r.into_inner();
                if r.redirect {
                    let leader = r.leader.ok_or("Redirect reply missing leader")?;
                    *target_addr = format!("http://{}:{}", leader.ip, leader.port);
                    *client = Some(connect(target_addr).await?);
                    continue;
                }
                if r.ok {
                    return Ok(r.message);
                }
                return Err(r.message);
            }
            Err(e) => return Err(format!("RPC error: {}", e.message())),
        }
    }
    Err("Too many redirects".to_string())
}

async fn request_log_with_redirect(
    client: &mut Option<RaftServiceClient<tonic::transport::Channel>>,
    target_addr: &mut String,
) -> Result<Vec<raft_service::LogEntry>, String> {
    for _ in 0..5 {
        if client.is_none() {
            *client = Some(connect(target_addr).await?);
        }
        let c = client.as_mut().unwrap();

        let resp = c
            .request_log(tonic::Request::new(RequestLogRequest {}))
            .await;

        match resp {
            Ok(r) => {
                let r = r.into_inner();
                if r.redirect {
                    let leader = r.leader.ok_or("Redirect reply missing leader")?;
                    *target_addr = format!("http://{}:{}", leader.ip, leader.port);
                    *client = Some(connect(target_addr).await?);
                    continue;
                }
                if r.ok {
                    return Ok(r.log);
                }
                return Err("request_log failed".to_string());
            }
            Err(e) => return Err(format!("RPC error: {}", e.message())),
        }
    }
    Err("Too many redirects".to_string())
}

fn print_help() {
    println!("Commands:");
    println!("  ping");
    println!("  get <key>");
    println!("  set <key> <value>");
    println!("  append <key> <value>");
    println!("  del <key>");
    println!("  strln <key>   (or strlen <key>)");
    println!("  request_log");
    println!("  remove <node_id>");
    println!("  change <ip> <port>");
    println!("  help");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("Use: cargo run --bin client <ip-addr> <port>");
    }

    let mut target_addr = format!("http://{}:{}", args[1], args[2]);
    let mut client: Option<RaftServiceClient<tonic::transport::Channel>> = None;

    print_help();

    loop {
        use std::io::{self, Write};
        print!("❯ ");
        io::stdout().flush().ok();
        let mut line = String::new();
        io::stdin().read_line(&mut line).ok();
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let tokens = match tokenize(line) {
            Ok(t) => t,
            Err(e) => {
                println!("{e}");
                continue;
            }
        };

        let cmd = tokens[0].as_str();

        if cmd == "help" {
            print_help();
            continue;
        }

        if cmd == "change" {
            if tokens.len() != 3 {
                println!("Usage: change <ip> <port>");
                continue;
            }
            target_addr = format!("http://{}:{}", tokens[1], tokens[2]);
            client = Some(connect(&target_addr).await.map_err(|e| {
                println!("{e}");
                e
            })?);
            println!("OK (now targeting {target_addr})");
            continue;
        }

        if cmd == "request_log" {
            match request_log_with_redirect(&mut client, &mut target_addr).await {
                Ok(logs) => {
                    for (i, e) in logs.iter().enumerate() {
                        let cmd = raft_service::CommandType::try_from(e.cmd)
                            .unwrap_or(raft_service::CommandType::CmdUnknown);

                        println!(
                            "[{i}] term={} cmd={:?} key='{}' value='{}'",
                            e.term, cmd, e.key, e.value
                        );
                    }
                }
                Err(e) => println!("{e}"),
            }
            continue;
        }

        let mut req = ExecuteRequest {
            cmd: CommandType::CmdUnknown as i32,
            key: "".to_string(),
            value: "".to_string(),
            node_id: -1,
        };

        match cmd {
            "ping" => {
                if tokens.len() != 1 {
                    println!("Usage: ping");
                    continue;
                }
                req.cmd = CommandType::CmdPing as i32;
            }
            "get" => {
                if tokens.len() != 2 {
                    println!("Usage: get <key>");
                    continue;
                }
                req.cmd = CommandType::CmdGet as i32;
                req.key = tokens[1].clone();
            }
            "set" => {
                if tokens.len() != 3 {
                    println!("Usage: set <key> <value>");
                    continue;
                }
                req.cmd = CommandType::CmdSet as i32;
                req.key = tokens[1].clone();
                req.value = tokens[2].clone();
            }
            "append" => {
                if tokens.len() != 3 {
                    println!("Usage: append <key> <value>");
                    continue;
                }
                req.cmd = CommandType::CmdAppend as i32;
                req.key = tokens[1].clone();
                req.value = tokens[2].clone();
            }
            "del" => {
                if tokens.len() != 2 {
                    println!("Usage: del <key>");
                    continue;
                }
                req.cmd = CommandType::CmdDel as i32;
                req.key = tokens[1].clone();
            }
            "strln" | "strlen" => {
                if tokens.len() != 2 {
                    println!("Usage: strln <key>");
                    continue;
                }
                req.cmd = CommandType::CmdStrlen as i32;
                req.key = tokens[1].clone();
            }
            "remove" => {
                if tokens.len() != 2 {
                    println!("Usage: remove <node_id>");
                    continue;
                }
                let id: i32 = match tokens[1].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        println!("node_id must be an integer");
                        continue;
                    }
                };
                req.cmd = CommandType::CmdRemoveMember as i32;
                req.node_id = id;
            }
            _ => {
                println!("Unknown command. Type `help`.");
                continue;
            }
        }

        match exec_with_redirect(&mut client, &mut target_addr, req).await {
            Ok(msg) => println!("{msg}"),
            Err(e) => println!("{e}"),
        }
    }
}
