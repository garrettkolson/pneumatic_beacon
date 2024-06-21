use std::io::{BufReader};
use std::net::TcpStream;
use pneumatic_core::encoding;
use pneumatic_core::node::{NodeRegistryRequest};

pub struct Beacon {
    nodes: Vec<Vec<u8>>
}

impl Beacon {
    pub fn from_config() -> Beacon {
        Beacon {
            nodes: Vec::new()
        }
    }

    pub fn handle_request(&self, mut stream: TcpStream){
        // todo: have to make methods both for registering with beacon
        // todo: and for making a broadcast request

        let buf_reader = BufReader::new(&mut stream);
        let _ = encoding::deserialize_rmp_to::<NodeRegistryRequest>(buf_reader.buffer().to_vec());

        // let request_line = buf_reader.lines().next().unwrap().unwrap();
        //
        // let (status_line, filename) = match &request_line[..] {
        //     "GET / HTTP/1.1" => ("HTTP/1.1 200 OK", "hello.html"),
        //     "GET /sleep HTTP/1.1" => {
        //         thread::sleep(Duration::from_secs(5));
        //         ("HTTP/1.1 200 OK", "hello.html")
        //     }
        //     _ => ("HTTP/1.1 404 Not Found", "404.html"),
        // };
        //
        // let contents = fs::read_to_string(filename).unwrap();
        // let length = contents.len();
        //
        // let response = format!("{status_line}\r\nContent-Length: {length}\r\n\r\n{contents}");
        // stream.write_all(response.as_bytes()).unwrap();
    }
}