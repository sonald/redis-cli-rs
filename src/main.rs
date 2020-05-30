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

    #[structopt(short, long, default_value = "6379")]
    pub port: u16,

    pub cmds: Vec<String>,
}

#[derive(Debug)]
enum RedisValue {
    Str(String),
    Bulk(String),
    Array(Vec<RedisValue>),
    Int(i32),
    Nil,
    Error(String),
}

impl fmt::Display for RedisValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RedisValue::Str(s) => write!(f, "{}", s),
            RedisValue::Bulk(s) => write!(f, "{}", s),
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
    fn from_vec(v: &Vec<String>) -> RedisValue {
        if v.len() == 0 {
            RedisValue::Nil
        } else {
            let mut res = vec![];
            for s in v {
                res.push(RedisValue::Bulk(s.clone()));
            }
            RedisValue::Array(res)
        }
    }

    fn to_wire(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        use std::io::Write;

        let mut res = vec![];
        match self {
            RedisValue::Str(s) => {
                Write::write(&mut res, b"+")?;
                Write::write(&mut res, s.as_bytes())?;
                Write::write(&mut res, b"\r\n")?;
            },
            RedisValue::Bulk(s) => {
                Write::write(&mut res, b"$")?;
                Write::write(&mut res, format!("{}", s.len()).as_bytes())?;
                Write::write(&mut res, b"\r\n")?;
                Write::write(&mut res, s.as_bytes())?;
                Write::write(&mut res, b"\r\n")?;
            },
            RedisValue::Int(i) => {
                Write::write(&mut res, b":")?;
                Write::write(&mut res, format!("{}", i).as_bytes())?;
                Write::write(&mut res, b"\r\n")?;
            },
            RedisValue::Nil => {
                Write::write(&mut res, b"$")?;
                Write::write(&mut res, format!("{}", -1).as_bytes())?;
                Write::write(&mut res, b"\r\n")?;
            },
            RedisValue::Error(s) => {
                Write::write(&mut res, b"-")?;
                Write::write(&mut res, s.as_bytes())?;
                Write::write(&mut res, b"\r\n")?;
            },
            RedisValue::Array(v) => {
                Write::write(&mut res, b"*")?;
                Write::write(&mut res, format!("{}", v.len()).as_bytes())?;
                Write::write(&mut res, b"\r\n")?;
                for d in v {
                    Write::write(&mut res, &d.to_wire()?)?;
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
            (s, RedisValue::Int(buf.parse::<i32>().unwrap_or(0)))
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

async fn stream(args: &Vec<String>, cli: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let cmd = RedisValue::from_vec(args);
    cli.write(cmd.to_wire()?.as_slice()).await?;

    if args[0] == "monitor" {
        monitor(cli).await
    } else {
        let res = read_redis_output(cli).await?;
        println!("{}", parse_redis_output(&res).1);
        Ok(())
    }
}

async fn monitor(cli: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    loop {
        let mut buf = vec![];
        let n = cli.read_to_end(&mut buf).await?;
        let res = String::from_utf8_lossy(&buf[..n]);
        println!("---- {}", parse_redis_output(&res).1);
    }
}

async fn interactive(cli: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                cli.write(line.as_bytes()).await?;

                if line.trim_end() == "monitor" {
                    monitor(cli).await?;
                    continue;
                }
                let  res = read_redis_output(cli).await?;
                println!("{}", parse_redis_output(&res).1);
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

    match args.cmds.len() {
        0 => interactive(&mut cli).await,
        _ => stream(&args.cmds, &mut cli).await
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
 
