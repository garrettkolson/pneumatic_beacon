use std::net::TcpStream;

pub struct Beacon {
    //nodes:
}

impl Beacon {
    pub fn from_config() -> Beacon {
        Beacon {}
    }

    pub fn handle_broadcast_request(&self, stream: TcpStream){
        todo!()
    }
}