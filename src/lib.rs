use std::{io::{BufRead, Cursor, Seek, SeekFrom, ErrorKind, Read, Write}, net::{TcpStream, ToSocketAddrs}};

///Simple implementation for redis client by tcp stream

const BULK_STRING_BYTE: u8 = 36;
const INTEGER_BYTE: u8 = 58;
const SIMPLE_STRING_BYTE: u8 = 43;
const ERROR_STRING_BYTE: u8 = 45;
const ARRAYS_BYTE: u8 = 42;

///Client for connect to redis by tcp
#[derive(Debug)]
pub struct Client {
    connect: TcpStream,
}

impl Client {
    ///Create redis client
    ///```no_run
    ///use redis_client::Client;
    ///let mut client = Client::new("127.0.0.1:6379");
    ///```
    pub fn new<A: ToSocketAddrs>(addr: A) -> std::io::Result<Self> {
        let connect = TcpStream::connect(addr)?;

        Ok(Client {connect})
    }

    fn execute(&mut self, bytes: Vec<u8>) -> std::io::Result<Vec<u8>> {
        self.connect.write_all(&bytes)?;
        let mut buff: Vec<u8> = vec![0; 1024];

        self.connect.read(&mut buff)?;

        Ok(buff)
    }
}

///Struct for create redis command
#[derive(Debug)]
pub struct Cmd {
    args: Vec<String>,
}

impl Cmd {
    ///Start create command
    ///```
    ///use redis_client::Cmd;
    ///
    ///let cmd = Cmd::cmd("PING");
    ///```
    pub fn cmd(cmd: &str) -> Self {
        let vec = vec![cmd.to_string()];
        Cmd { args: vec }
    }

    ///Set arguments to your command
    ///```
    ///use redis_client::Cmd;
    ///
    ///let cmd = Cmd::cmd("SET").arg("key").arg("value");
    ///```
    pub fn arg(mut self, arg: &str) -> Self {
        self.args.push(arg.to_string());

        self
    }

    ///Execute command
    ///```no_run
    ///use redis_client::{Cmd, Client};
    ///let mut client = Client::new("127.0.0.1:6379").unwrap();
    ///
    ///let cmd = Cmd::cmd("SET")
    ///    .arg("key")
    ///    .arg("value")
    ///    .execute(&mut client);
    ///```
    pub fn execute(self, conn: &mut Client) -> std::io::Result<Values> {
        if let Ok (result) = parse_response(&mut conn.execute(Self::create_command(&self.args))?) {
            Ok(result)
        } else {
            Err(std::io::Error::new(ErrorKind::Other, "Error"))
        }
    }

    fn create_command(args: &Vec<String>) -> Vec<u8> {
        if args.len() == 1 {
            format!("+{}\r\n{}", args[0].len(), args[0]).into_bytes()
        } else {

            let mut result = format!("*{}\r\n", args.len());

            for arg in args {
                result.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
            }

            result.into_bytes()
        }
    }
}

///Enum for represent redis responses
#[derive(Debug, Eq, PartialEq)]
pub enum Values {
    SimpleString(String),
    Errors(String),
    Integers(i64),
    BulkString(String),
    Arrays(Vec<Values>),
}

///Function for parse response redis response from tcp stream
fn parse_response(buff: &mut Vec<u8>) -> Result<Values, Box<dyn std::error::Error>> {
    let mut cursor = Cursor::new(buff);

    cursor.seek(SeekFrom::Start(0))?;
    let mut first_byte: [u8; 1] = [0];
    cursor.read(&mut first_byte)?;


    match first_byte[0] {
        INTEGER_BYTE => {
            let mut l = cursor.lines();
            if let Some(int_result) = l.next() {
                Ok(Values::Integers(int_result?.parse()?))
            } else {
                Err(Box::new(std::io::Error::new(ErrorKind::InvalidInput, "Error integer value")))
            }
        },
        BULK_STRING_BYTE => {
            let mut l = cursor.lines();

            let _size = if let Some(str_result) = l.next() {
                str_result?.parse()?
            } else {
                0
            };

            if let Some(str_result) = l.next() {
                Ok(Values::BulkString(str_result?))
            } else {
                Err(Box::new(std::io::Error::new(ErrorKind::InvalidInput, "Error integer value")))
            }
        },
        SIMPLE_STRING_BYTE => {
            let mut l = cursor.lines();

            if let Some(int_result) = l.next() {
                Ok(Values::SimpleString(int_result?))
            } else {
                Err(Box::new(std::io::Error::new(ErrorKind::InvalidInput, "Error integer value")))
            }
        },
        ERROR_STRING_BYTE => {
            let mut l = cursor.lines();

            if let Some(int_result) = l.next() {
                Ok(Values::Errors(int_result?))
            } else {
                Err(Box::new(std::io::Error::new(ErrorKind::InvalidInput, "Error integer value")))
            }
        },
        ARRAYS_BYTE => {
            let mut line = String::new();
            cursor.read_line(&mut line)?;
            let line_count: i64 = line.chars()
                .filter(|c| c.is_numeric())
                .collect::<String>()
                .parse()?;

            let mut v: Vec<Values> = Vec::new();

            let mut split = cursor.split(b'\n');
            for _ in 0..line_count {
                let mut first_line = split.next().unwrap()?;
                let mut second_line = split.next().unwrap()?;
                second_line.push(b'\n');

                first_line.push(b'\n');
                first_line.append(&mut second_line);
                v.push(parse_response(&mut first_line)?);
            }

            Ok(Values::Arrays(v))
        },
        _ => {
            Err(Box::new(std::io::Error::new(ErrorKind::InvalidInput, "hui")))
        },
    }

}

#[cfg(test)]
mod tests {
    use crate::parse_response;
    use crate::Values;

    #[test]
    fn test_set_value() {
        let mut client = crate::Client::new("127.0.0.1:6379").unwrap();
        let result = crate::Cmd::cmd("SET").arg("\ntest\n").arg("test\n").execute(&mut client);

        assert_eq!(true, result.is_ok());
    }

    #[test]
    fn empty_string() {
        let result = parse_response(&mut vec![0; 0]);

        assert_eq!(true, result.is_err());
    }

    #[test]
    fn simple_string() {
        let mut raw_str: Vec<u8> = vec![b'+', b'H', b'e', b'l', b'l', b'o', b'\r', b'\n'];
        let result = parse_response(&mut raw_str);

        assert_eq!(Values::SimpleString(String::from("Hello")), result.unwrap());
    }

    #[test]
    fn bulk_string() {
        let mut raw_str: Vec<u8> = vec![b'$', b'4', b'\r', b'\n', b'T', b'e', b's', b't'];
        let result = parse_response(&mut raw_str);

        assert_eq!(Values::BulkString(String::from("Test")), result.unwrap());
    }

    #[test]
    fn integer() {
        let mut raw_str: Vec<u8> = vec![b':', b'1', b'2', b'\r', b'\n'];
        let result = parse_response(&mut raw_str);

        assert_eq!(Values::Integers(12), result.unwrap());
    }

    #[test]
    fn negative_integer() {
        let mut raw_str: Vec<u8> = vec![b'$', b'3', b'\r', b'\n', b'-', b'1', b'2'];
        let result = parse_response(&mut raw_str);

        assert_eq!(Values::BulkString(String::from("-12")), result.unwrap());
    }

    #[test]
    fn error() {
        let mut raw_str: Vec<u8> = vec![b'-', b'E', b'r', b'r', b'o', b'r', b' ', b'm', b'e', b's', b's', b'a', b'g', b'e', b'\r', b'\n'];
        let result = parse_response(&mut raw_str);

        assert_eq!(Values::Errors(String::from("Error message")), result.unwrap());
    }

    #[test]
    fn array() {
        let mut rawData = vec![
            b'*', b'4',
            b'\r', b'\n',
            b'$', b'3',
            b'\r', b'\n',
            b'p', b'8', b'F',
            b'\r', b'\n',
            b'$', b'4',
            b'\r', b'\n',
            b't', b'e', b's', b't',
            b'\r', b'\n',
            b'$', b'2',
            b'\r', b'\n',
            b'9', b'm',
            b'\r', b'\n',
            b'$', b'1',
            b'\r', b'\n',
            b't',
            b'\r', b'\n'];

        assert_eq!(Values::Arrays(
                vec![
                Values::BulkString(String::from("p8F")),
                Values::BulkString(String::from("test")),
                Values::BulkString(String::from("9m")),
                Values::BulkString(String::from("t")),
                ]
        ),
        parse_response(&mut rawData).unwrap());
    }
}
