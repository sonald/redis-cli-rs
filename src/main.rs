use structopt::StructOpt;
use std::net::TcpStream;
use std::io::{Write, Read};
use rustyline::{Editor, error::ReadlineError};
use std::error::Error;
use std::fmt;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long)]
    pub debug: bool,

    #[structopt(short, long)]
    pub hostname: Option<String>,

    pub cmds: Vec<String>,
}

#[derive(Debug)]
enum RedisResult {
    Str(String),
    Bulk(String),
    Array(Vec<Box<RedisResult>>),
    Int(i32),
    Nil,
    Error(String),
}

impl fmt::Display for RedisResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RedisResult::Str(s) => write!(f, "{}", s),
            RedisResult::Bulk(s) => write!(f, "{}", s),
            RedisResult::Int(i) => write!(f, "(integer) {}", i),
            RedisResult::Nil => write!(f, "(nil)"),
            RedisResult::Error(s) => write!(f, "(error) {}", s),
            RedisResult::Array(v) => {
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

fn parse_string(s: &str) -> (&str, &str) {
    let i = s.find("\r\n").expect("invalid");
    (&s[i+2..], &s[..i])
}

fn parse_redis_output(s: &str) -> (&str, RedisResult) {
    let tok = s.as_bytes()[0];
    let s = &s[1..];
    match tok {
        b'-' => {
            let (s, buf) = parse_string(s);
            (s, RedisResult::Error(buf.to_string()))
        },
        b'+' => {
            let (s, buf) = parse_string(s);
            (s, RedisResult::Str(buf.to_string()))
        },
        b'$' => {
            let i = s.find("\r\n").expect("invalid");
            let n = s[..i].parse::<i32>().unwrap_or(0);
            if n == -1 {
                (&s[i+2..], RedisResult::Nil)
            } else {
                (&s[i+2+n as usize+2..], RedisResult::Bulk(s[i+2..i+2+n as usize].to_string()))
            }
        },
        b':' => {
            let (s, buf) = parse_string(s);
            (s, RedisResult::Int(buf.parse::<i32>().unwrap_or(0)))
        },
        b'*' => {
            let mut v = Vec::new();
            let i = s.find("\r\n").expect("invalid");
            let n = s[..i].parse::<usize>().unwrap_or(0);
            let mut s = &s[i+2..];
            for _ in 0..n {
                let (t, r) = parse_redis_output(s);
                v.push(Box::new(r));
                s = t;
            }

            (s, RedisResult::Array(v))
        },
        _ => (s, RedisResult::Error("unknown".to_string())),
    }
}

fn stream(args: &Vec<String>, cli: &mut TcpStream) -> Result<RedisResult, Box<dyn Error>> {
    let mut cmd = args.join(" ");
    cmd.push('\n');
    cli.write(cmd.as_bytes())?;

    let mut res = String::new();
    loop {
        let mut buf = [0u8; 32];
        let n = cli.read(&mut buf[..])?;
        res += &String::from_utf8_lossy(&buf[..n]);
        if n < 32 { break }
    }

    Ok(parse_redis_output(&res).1)
}

fn interactive(cli: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(mut line) => {
                rl.add_history_entry(line.as_str());
                line.push('\n');
                cli.write(line.as_bytes())?;

                let mut res = String::new();
                loop {
                    let mut buf = [0u8; 32];
                    let n = cli.read(&mut buf[..])?;
                    res += &String::from_utf8_lossy(&buf[..n]);
                    if n < 32 { break }
                }
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

fn run(args: Opt) -> Result<(), Box<dyn Error>> {
    let host = match args.hostname {
        Some(ref s) => &s,
        None => "127.0.0.1"
    };
    let mut cli = TcpStream::connect((host, 6379)).expect("connect failed");

    match args.cmds.len() {
        0 => interactive(&mut cli),
        _ => stream(&args.cmds, &mut cli).map(|res| println!("{}", res))
    }
}

fn main() {
    unsafe {
        signal_hook::register(signal_hook::SIGINT, || {
            println!("quit");
            std::process::exit(0);
        }).expect("hook sigint failed");
    }

    let args = Opt::from_args();

    if let Err(err) = run(args) {
        eprintln!("error: {}", err);
    }
}
 
