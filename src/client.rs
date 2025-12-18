use raft_service::raft_service_client::RaftServiceClient;
use raft_service::{ClientReply, ClientRequest};
use std::env;
use std::{collections::VecDeque, fmt::Display};
pub mod raft_service {
    tonic::include_proto!("raft_service");
}
use dialoguer::{Completion, History, Input};

struct MyCompletion {
    options: Vec<String>,
}

impl Default for MyCompletion {
    fn default() -> Self {
        MyCompletion {
            options: vec![
                "ping".to_string(),
                "get <nama>".to_string(),
                "set <nama> <value>".to_string(),
                "strln <nama>".to_string(),
                "change <ip> <port>".to_string(),
                "del <nama>".to_string(),
                "append <nama> <value>".to_string(),
            ],
        }
    }
}

impl Completion for MyCompletion {
    fn get(&self, input: &str) -> Option<String> {
        let matches = self
            .options
            .iter()
            .filter(|option| option.starts_with(input))
            .collect::<Vec<_>>();

        if matches.len() == 1 {
            Some(matches[0].to_string())
        } else {
            None
        }
    }
}

struct MyHistory {
    history: VecDeque<String>,
}

impl Default for MyHistory {
    fn default() -> Self {
        MyHistory {
            history: VecDeque::new(),
        }
    }
}

impl<T: ToString> History<T> for MyHistory {
    fn read(&self, pos: usize) -> Option<String> {
        self.history.get(pos).cloned()
    }

    fn write(&mut self, val: &T) {
        self.history.push_front(val.to_string());
    }
}

fn prompt(
    text: &str,
    history: &mut MyHistory,
    completion: &MyCompletion,
) -> Result<String, std::io::Error> {
    let name: String = Input::new()
        .history_with(history)
        .completion_with(completion)
        .with_prompt(text)
        .interact_text()
        .unwrap();
    Ok(name)
}

// Client Entries
// Client code:
// 1. ping
// 2. get <nama-key>
// 3. set <nama-key> <value>
// 4. strln <nama-key>
// 5. del <nama-key>
// 6. append <nama-key> <value>
// 7. change <ip-dadr> <port>

fn parse_req_str(req_string: String) -> Result<tonic::Request<ClientRequest>, String> {
    let parts = req_string.split(" ");

    let mut request = tonic::Request::new(ClientRequest {
        r#type: -1,
        val: None,
        ip: None,
        key: None,
        port: None,
    });
    let mut len = 0;
    for _ in parts.clone() {
        len += 1;
    }
    let mut ctr = 0;
    for part in parts {
        match ctr {
            0 => {
                match part {
                    "ping" => request.get_mut().r#type = 1,
                    "get" => request.get_mut().r#type = 2,
                    "set" => request.get_mut().r#type = 3,
                    "strln" => request.get_mut().r#type = 4,
                    "del" => request.get_mut().r#type = 5,
                    "append" => request.get_mut().r#type = 6,
                    "change" => request.get_mut().r#type = 7,
                    _default => return Err("Unexpected token".to_string()),
                }

                match request.get_ref().r#type {
                    1 => {
                        if len != 1 {
                            return Err("Unexpected token".to_string());
                        }
                    }

                    2 | 4..6 => {
                        if len != 2 {
                            return Err("Unexpected token".to_string());
                        }
                    }
                    3 | 6..8 => {
                        if len != 3 {
                            return Err("Unexpected token".to_string());
                        }
                    }
                    _default => {
                        return Err("Unexpected token".to_string());
                    }
                }
            }
            1 => match request.get_ref().r#type {
                2..7 => request.get_mut().key = Some(part.to_string()),
                7 => request.get_mut().ip = Some(part.to_string()),
                _default => return Err("Unexpected token".to_string()),
            },
            2 => match request.get_ref().r#type {
                3 | 6 => request.get_mut().val = Some(part.to_string()),
                7 => request.get_mut().port = Some(part.to_string()),
                _default => {
                    return Err("Unexpected token".to_string());
                }
            },
            _default => {
                return Err("Unexpected token".to_string());
            }
        }
        ctr += 1;
    }

    Ok(request)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("Brow aint no way salah input: liat readme pls")
    }
    let ip_addr = &args[1];
    let port = &args[2];
    let mut addr_str = String::from("http://");
    addr_str.push_str(ip_addr);
    addr_str.push(':');
    addr_str.push_str(port);
    let mut history = MyHistory::default();
    let completion = MyCompletion::default();

    let conn = RaftServiceClient::connect(addr_str.clone()).await;
    match conn {
        Ok(mut client) => loop {
            let input = prompt("❯", &mut history, &completion);
            match input {
                Ok(req_str) => {
                    let parse_res = parse_req_str(req_str);
                    match parse_res {
                        Ok(request) => {
                            let response = client.client(request).await;
                            match response {
                                Ok(res) => {
                                    println!("{}", res.get_ref().message)
                                }
                                Err(err) => {
                                    println!("Server went wrong: {}", err.message());
                                }
                            }
                        }
                        Err(msg) => println!("{}", msg),
                    }
                }
                Err(_) => {}
            }
        },
        Err(_) => {
            println!("Cannot connect to server");
        }
    }
    Ok(())
}
