use structopt::StructOpt;
use tokio::prelude::*;
use tokio::net::TcpStream;
use rustyline::{Editor, error::ReadlineError};
use std::error::Error;

mod redis;
use self::redis::*;

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

async fn read_redis_output(cli: &mut TcpStream) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut res = vec![];
    let mut buf = [0u8; 64];

    loop {
        let n = cli.read(&mut buf[..]).await?;
        res.extend(&buf[..n]);
        if n < 32 { break }
    }
    Ok(res)
}

async fn consume_all_output(cli: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let res = read_redis_output(cli).await?;

    let mut start = 0;
    while let Some((value, left)) = RedisValue::deserialize(&res[start..]) {
        println!("{}", value);
        start += left;
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
                        println!("{}", RedisValue::deserialize(&res).expect("").0);
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

//TODO: add proxy mode
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
 
