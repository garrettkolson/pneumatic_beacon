use std::net::{TcpListener};
use std::sync::Arc;
use pneumatic_core::server;
use pneumatic_beacon::Beacon;

const THREAD_COUNT: usize = 100;

fn main() {
    // todo: make listener address, thread count configurable by loading from config.json
    let listener = TcpListener::bind("127.0.0.1:7878")
        .expect("Couldn't set up listener for beacon");

    let beacon = Arc::new(Beacon::from_config());
    let pool = match server::ThreadPool::build(THREAD_COUNT) {
        Err(err) => panic!("{}", err.message),
        Ok(p) => p
    };

    for stream in listener.incoming() {
        let _ = match stream {
            Ok(stream) => {
                let safe_beacon = Arc::clone(&beacon);
                let _ = pool.execute(move || {
                    safe_beacon.handle_request(stream);
                });
            },
            Err(_) => continue
        };
    }
}