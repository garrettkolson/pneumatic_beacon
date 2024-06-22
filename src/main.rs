use std::io::BufReader;
use std::net::{TcpListener};
use std::sync::Arc;
use pneumatic_core::conns::TcpConnFactory;
use pneumatic_core::server;
use pneumatic_beacon::Beacon;

const THREAD_COUNT: usize = 100;

fn main() {
    // todo: make listener address, thread count configurable by loading from config.json
    let listener = TcpListener::bind("127.0.0.1:7878")
        .expect("Couldn't set up listener for beacon");

    let beacon = Arc::new(Beacon::with_factory(TcpConnFactory::new()));
    let pool = match server::ThreadPool::build(THREAD_COUNT) {
        Err(err) => panic!("{}", err.message),
        Ok(p) => p
    };

    for stream in listener.incoming() {
        let _ = match stream {
            Ok(mut stream) => {
                let safe_beacon = Arc::clone(&beacon);
                let _ = pool.execute(move || {
                    let buf_reader = BufReader::new(&mut stream);
                    let raw_data = buf_reader.buffer().to_vec();
                    safe_beacon.handle_request(raw_data);
                });
            },
            // TODO: log bad requests?
            Err(_) => continue
        };
    }
}