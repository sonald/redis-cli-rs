use std::fmt;
use std::error::Error;
use bytes::Bytes;

#[derive(Debug)]
pub enum RedisValue {
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

#[derive(Debug)]
pub enum Command {
    Get(String),
    Set(String, String),
    Pipeline(Vec<Command>)
}

impl From<Command> for RedisValue {
    fn from(cmd: Command) -> RedisValue {
        match cmd {
            Command::Get(key) => RedisValue::from_vec(vec![key]),
            Command::Set(key, val) => RedisValue::from_vec(vec![key, val]),
            _ => {unimplemented!();}
        }
    }
}

impl RedisValue {
    pub fn from_vec(v: Vec<String>) -> RedisValue {
        match v.len() {
            0 => RedisValue::Nil,
            _ => RedisValue::Array(v.into_iter().map(RedisValue::Bulk).collect())
        }
    }

    pub fn is_valid<S: AsRef<[u8]>>(s: S) -> bool {
        if s.as_ref().len() == 0 { return false }

        fn match_string<'a>(ts: &mut impl DoubleEndedIterator<Item = &'a u8>) -> bool {
            ts.find(|&&c| c == '\n').is_some()
        }

        fn match_value<'a>(ts: &mut impl DoubleEndedIterator<Item = &'a u8>) -> bool {
            if let Some(ch) = ts.next() {
                match ch {
                    b'-' => {
                        match_string(ts)
                    },
                    b'+' => {
                        match_string(ts)
                    },
                    b'$' => {
                        let mut n = match_string(ts).parse::<i32>().unwrap_or(0);
                        if n == -1 {
                            Some(RedisValue::Nil)
                        } else {
                            let mut buf = vec![];
                            while n > 0 {
                                let ch = ts.next().expect("invlaid resp");
                                buf.push(*ch);
                                n -= 1;
                            }

                            ts.next();
                            ts.next();

                            Some(RedisValue::Bulk(String::from_utf8_lossy(&buf).to_string()))
                        }
                    },
                    b':' => {
                        Some(RedisValue::Int(match_string(ts).parse::<i64>().unwrap_or(0)))
                    },
                    b'*' => {
                        let n = match_string(ts).parse::<usize>().unwrap_or(0);
                        let res = (0..n).fold(vec![], |mut v, _| {
                            let value = match_value(ts).expect("invalid resp");
                            v.push(value);
                            v
                        });

                        Some(RedisValue::Array(res))
                    },
                    _ => panic!("invalid redis resp"),
                }
            } else {
                None
            }
        }

        let mut ts = s.iter();
        match_value(&mut ts).map(|v| (v, s.as_ref().len() - ts.size_hint().0))
    }

    /// deserialize a RedisValue from `s`, and return value with consumed bytes
    pub fn deserialize(s: &[u8]) -> Option<(RedisValue, usize)> {
        if s.as_ref().len() == 0 {
            return None
        }

        fn match_string<'a>(ts: &mut impl DoubleEndedIterator<Item = &'a u8>) -> String {
            let mut buf = vec![];
            while let Some(ch) = ts.next() {
                if *ch == b'\r' {
                    break
                }

                buf.push(*ch);
            }
            ts.next(); // eat \n
            String::from_utf8_lossy(&buf).to_string()
        }

        fn match_value<'a>(ts: &mut impl DoubleEndedIterator<Item = &'a u8>) -> Option<RedisValue> {
            if let Some(ch) = ts.next() {
                match ch {
                    b'-' => {
                        Some(RedisValue::Error(match_string(ts)))
                    },
                    b'+' => {
                        Some(RedisValue::Str(match_string(ts)))
                    },
                    b'$' => {
                        let mut n = match_string(ts).parse::<i32>().unwrap_or(0);
                        if n == -1 {
                            Some(RedisValue::Nil)
                        } else {
                            let mut buf = vec![];
                            while n > 0 {
                                let ch = ts.next().expect("invlaid resp");
                                buf.push(*ch);
                                n -= 1;
                            }

                            ts.next();
                            ts.next();

                            Some(RedisValue::Bulk(String::from_utf8_lossy(&buf).to_string()))
                        }
                    },
                    b':' => {
                        Some(RedisValue::Int(match_string(ts).parse::<i64>().unwrap_or(0)))
                    },
                    b'*' => {
                        let n = match_string(ts).parse::<usize>().unwrap_or(0);
                        let res = (0..n).fold(vec![], |mut v, _| {
                            let value = match_value(ts).expect("invalid resp");
                            v.push(value);
                            v
                        });

                        Some(RedisValue::Array(res))
                    },
                    _ => panic!("invalid redis resp"),
                }
            } else {
                None
            }
        }

        let mut ts = s.iter();
        match_value(&mut ts).map(|v| (v, s.as_ref().len() - ts.size_hint().0))
    }

    pub fn to_wire(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        use std::io::Write;
        macro_rules! write_cmd {
            ($res:ident, $e:expr) => (Write::write(&mut $res, $e)?);
        }

        let mut res = vec![];
        match self {
            RedisValue::Str(s) => {
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
    #[test]
    fn test_deserialize() {
        let data = "*5\r\n-hello\r\n$-1\r\n+hello\r\n$5\r\nhello\r\n:34\r\n";
        let value = RedisValue::deserialize(data).expect("");
        println!("{}", value.0);
        assert_eq!(value.0.to_wire().expect(""), data.as_bytes());
    }
    #[test]
    fn test_deserialize2() {
        let data = "*5\r\n-hello\r\n$-1\r\n+hello\r\n$5\r\nhello\r\n:34\r\n+another value";
        let value = RedisValue::deserialize(data).expect("");
        println!("{}, {}", value.0, value.1);
        assert_eq!(value.0.to_wire().expect(""), &data.as_bytes()[..value.1]);
    }
}
