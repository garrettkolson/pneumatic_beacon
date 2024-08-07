use std::io::{BufReader, Write};
use std::net::{TcpListener};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};
use pneumatic_core::config::Config;
use pneumatic_core::conns::*;
use pneumatic_core::server;
use pneumatic_beacon::{Beacon, HeartbeatError, HeartbeatResult};

const SERVER_THREAD_COUNT: usize = 100;
const HEARTBEAT_DURATION_IN_SECS: u64 = 60;

fn main() {
    let config = Config::build()
        .expect("Couldn't build node config for beacon");

    let conn_factory = Box::new(TcpConnFactory::new());
    let beacon = Beacon::init(config, conn_factory);
    let beacon = Arc::new(beacon);

    let request_beacon = Arc::clone(&beacon);
    let request_thread = thread::spawn(move || {
        listen_for_requests(request_beacon);
    });

    let heartbeat_beacon = Arc::clone(&beacon);
    let heartbeat_thread = thread::spawn(move || {
        check_node_heartbeats(heartbeat_beacon);
    });

    request_thread.join().unwrap();
    heartbeat_thread.join().unwrap();
}

fn listen_for_requests(beacon: Arc<Beacon>) {
    // todo: make listener address, thread count configurable by loading from config.json
    let listener = TcpListener::bind(format!("127.0.0.1:{BEACON_PORT}"))
        .expect("Couldn't set up listener for beacon");

    let pool = match server::ThreadPool::build(SERVER_THREAD_COUNT) {
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
                    let response = safe_beacon.handle_request(raw_data);
                    stream.write_all(&response).unwrap()
                });
            },
            // TODO: log bad requests?
            Err(_) => continue
        };
    }
}

fn check_node_heartbeats(beacon: Arc<Beacon>) {
    loop {
        let start = SystemTime::now();
        if let HeartbeatResult::Err(HeartbeatError::ConnectionError) = beacon.check_heartbeats() {
            // TODO: log these errors
            eprintln!("fatal error in checking for node heartbeats");
            return;
        } else {
            let mut remaining_time: i64 = HEARTBEAT_DURATION_IN_SECS as i64;
            if let Ok(elapsed) = start.elapsed() {
                let elapsed_secs = elapsed.as_secs() as i64;
                remaining_time = remaining_time - elapsed_secs;
                if remaining_time <= 0 { continue; }
            }
            thread::sleep(Duration::from_secs(remaining_time as u64));
        }
    }
}