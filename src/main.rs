use std::net::TcpListener;
use pneumatic_beacon::ThreadPool;
use pneumatic_beacon::beacon::Beacon;

const THREAD_COUNT: usize = 100;

fn main() {
    // todo: make listener address, thread count configurable by loading from config.json
    let listener = TcpListener::bind("127.0.0.1:7878")
        .expect("Couldn't set up listener for beacon");

    let beacon = Beacon::from_config();
    let pool = match ThreadPool::build(THREAD_COUNT) {
        Err(err) => panic!("{}", err.message),
        Ok(p) => p
    };

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let _ = pool.execute(|| {
            beacon.handle_broadcast_request(stream);
        });
    }

    println!("Shutting down...")
}



