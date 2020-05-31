use structopt::StructOpt;
use tokio::prelude::*;
use tokio::net::TcpStream;
use rustyline::{Editor, error::ReadlineError};
use std::error::Error;
use std::fmt;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long)]
    pub debug: bool,

    #[structopt(short, long, default_value = "127.0.0.1")]
    pub hostname: String,

    #[structopt(short("P"), long, default_value = "6379")]
    pub port: u16,

    #[structopt(short, long)]
    pub pipe: bool,

    pub cmds: Vec<String>,
}

#[derive(Debug)]
enum RedisValue {
    Str(String),
    Bulk(String),
    Array(Vec<RedisValue>),
    Int(i64),
    Nil,
    Error(String),
}

impl fmt::Display for RedisValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RedisValue::Str(s) => write!(f, "{}", s),
            RedisValue::Bulk(s) => write!(f, "{:?}", s),
            RedisValue::Int(i) => write!(f, "(integer) {}", i),
            RedisValue::Nil => write!(f, "(nil)"),
            RedisValue::Error(s) => write!(f, "(error) {}", s),
            RedisValue::Array(v) => {
                if v.len() == 0 {
                    write!(f, "(empty array)")
                } else {
                    for i in 1..=v.len() {
                        write!(f, "{}{}) {}", if i > 1 { "\n" } else { "" },
                            i, v[i-1])?;
                    }
                    Ok(())
                }
            },
        }
    }
}

impl RedisValue {
    fn from_vec(v: Vec<String>) -> RedisValue {
        match v.len() {
            0 => RedisValue::Nil,
            _ => RedisValue::Array(v.into_iter().map(RedisValue::Bulk).collect())
        }
    }

    fn to_wire(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        use std::io::Write;
        macro_rules! write_cmd {
            ($res:ident, $e:expr) => (Write::write(&mut $res, $e)?);
        }

        let mut res = vec![];
        match self {
            RedisValue::Str(s) => {
                write_cmd!(res, b"+");
                write_cmd!(res, b"+");
                write_cmd!(res, s.as_bytes());
                write_cmd!(res, b"\r\n");
            },
            RedisValue::Bulk(s) => {
                write_cmd!(res, b"$");
                write_cmd!(res, format!("{}", s.len()).as_bytes());
                write_cmd!(res, b"\r\n");
                write_cmd!(res, s.as_bytes());
                write_cmd!(res, b"\r\n");
            },
            RedisValue::Int(i) => {
                write_cmd!(res, b":");
                write_cmd!(res, format!("{}", i).as_bytes());
                write_cmd!(res, b"\r\n");
            },
            RedisValue::Nil => {
                write_cmd!(res, b"$");
                write_cmd!(res, format!("{}", -1).as_bytes());
                write_cmd!(res, b"\r\n");
            },
            RedisValue::Error(s) => {
                write_cmd!(res, b"-");
                write_cmd!(res, s.as_bytes());
                write_cmd!(res, b"\r\n");
            },
            RedisValue::Array(v) => {
                write_cmd!(res, b"*");
                write_cmd!(res, format!("{}", v.len()).as_bytes());
                write_cmd!(res, b"\r\n");
                for d in v {
                    write_cmd!(res, &d.to_wire()?);
                }
            },
        }

        Ok(res)
    }
}

fn parse_string(s: &str) -> (&str, &str) {
    let i = s.find("\r\n").expect("invalid");
    (&s[i+2..], &s[..i])
}

//TODO: use one pass LA(1) parsing
fn parse_redis_output(s: &str) -> (&str, RedisValue) {
    let tok = s.as_bytes()[0];
    let s = &s[1..];
    match tok {
        b'-' => {
            let (s, buf) = parse_string(s);
            (s, RedisValue::Error(buf.to_string()))
        },
        b'+' => {
            let (s, buf) = parse_string(s);
            (s, RedisValue::Str(buf.to_string()))
        },
        b'$' => {
            let i = s.find("\r\n").expect("invalid");
            let n = s[..i].parse::<i32>().unwrap_or(0);
            if n == -1 {
                (&s[i+2..], RedisValue::Nil)
            } else {
                (&s[i+2+n as usize+2..], RedisValue::Bulk(s[i+2..i+2+n as usize].to_string()))
            }
        },
        b':' => {
            let (s, buf) = parse_string(s);
            (s, RedisValue::Int(buf.parse::<i64>().unwrap_or(0)))
        },
        b'*' => {
            let mut v = Vec::new();
            let i = s.find("\r\n").expect("invalid");
            let n = s[..i].parse::<usize>().unwrap_or(0);
            let mut s = &s[i+2..];
            for _ in 0..n {
                let (t, r) = parse_redis_output(s);
                v.push(r);
                s = t;
            }

            (s, RedisValue::Array(v))
        },
        _ => (s, RedisValue::Error("unknown".to_string())),
    }
}

async fn read_redis_output(cli: &mut TcpStream) -> Result<String, Box<dyn Error>> {
    let mut res = String::new();
    loop {
        let mut buf = [0u8; 32];
        let n = cli.read(&mut buf[..]).await?;
        res += &String::from_utf8_lossy(&buf[..n]);
        if n < 32 { break }
    }
    Ok(res)
}

//TODO: incrementally displaying while reading and parsing
async fn consume_all_output(cli: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let res = read_redis_output(cli).await?;

    let mut s = res.as_str();
    while s.len() > 0 {
        let (left, value) = parse_redis_output(s);
        println!("{}", value);
        s = left;
    }

    Ok(())
}

async fn stream(args: Vec<String>, pipe: bool, cli: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let cmd = args[0].clone();
    let data = if pipe {
        args.into_iter().map(|a| a + "\r\n").collect::<String>().into_bytes()
    } else {
        let value = RedisValue::from_vec(args);
        value.to_wire()?
    };
    cli.write(data.as_slice()).await?;

    match cmd.as_str() {
        "monitor" | "subscribe" => loop {
            consume_all_output(cli).await?
        },
        _ => consume_all_output(cli).await
    }
}

async fn interactive<S: AsRef<str>>(prompt: S, cli: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline(prompt.as_ref());
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                let args = line.split_whitespace().map(|s| s.to_owned()).collect::<Vec<String>>();
                let cmd = args[0].clone();
                let value = RedisValue::from_vec(args);
                cli.write(value.to_wire()?.as_slice()).await?;

                match cmd.as_str() {
                    "monitor" | "subscribe" => loop {
                        consume_all_output(cli).await?
                    },
                    _ => {
                        let res = read_redis_output(cli).await?;
                        println!("{}", parse_redis_output(&res).1);
                    }
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break
            },
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break
            },
            Err(err) => {
                return Err(Box::new(err))
            }
        }
    }
    Ok(())
}

async fn run(args: Opt) -> Result<(), Box<dyn Error>> {
    let mut cli = TcpStream::connect((args.hostname.as_str(), args.port)).await?;
    let prompt = format!("{}:{}> ", args.hostname,args.port);

    if args.cmds.len() == 0 && !args.pipe {
        interactive(prompt, &mut cli).await
    } else {
        let cmds = if args.pipe {
            let mut buf = String::new();
            tokio::io::stdin().read_to_string(&mut buf).await?;
            buf.split('\n').map(|s| s.to_owned()).collect::<Vec<String>>()
        } else {
            args.cmds
        };
        stream(cmds, args.pipe, &mut cli).await
    }
}

#[tokio::main]
async fn main() {
    unsafe {
        signal_hook::register(signal_hook::SIGINT, || {
            println!("quit");
            std::process::exit(0);
        }).expect("hook sigint failed");
    }

    let args = Opt::from_args();

    if let Err(err) = run(args).await {
        eprintln!("error: {}", err);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_wire_error() {
        let r = RedisValue::Error("hello".to_string());
        assert_eq!(&r.to_wire().expect(""), b"-hello\r\n");
    }
    #[test]
    fn test_to_wire_nil() {
        let r = RedisValue::Nil;
        assert_eq!(&r.to_wire().expect(""), b"$-1\r\n");
    }
    #[test]
    fn test_to_wire_str() {
        let r = RedisValue::Str("hello".to_string());
        assert_eq!(&r.to_wire().expect(""), b"+hello\r\n");
    }
    #[test]
    fn test_to_wire_bulk() {
        let r = RedisValue::Bulk("hello".to_string());
        assert_eq!(&r.to_wire().expect(""), b"$5\r\nhello\r\n");
    }
    #[test]
    fn test_to_wire_int() {
        let r = RedisValue::Int(34);
        assert_eq!(&r.to_wire().expect(""), b":34\r\n");
    }
    #[test]
    fn test_to_wire_array() {
        let mut v = vec![];
        v.push(RedisValue::Error("hello".to_string()));
        v.push(RedisValue::Nil);
        v.push(RedisValue::Str("hello".to_string()));
        v.push(RedisValue::Bulk("hello".to_string()));
        v.push(RedisValue::Int(34));
        let r = RedisValue::Array(v);
        let r2 = b"*5\r\n-hello\r\n$-1\r\n+hello\r\n$5\r\nhello\r\n:34\r\n".iter().map(|&c| c).collect::<Vec<u8>>();
        assert_eq!(r.to_wire().expect(""), r2);
    }
}
 
