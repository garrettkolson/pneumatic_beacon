use std::io::BufReader;
use std::net::{TcpListener};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};
use pneumatic_core::conns::TcpConnFactory;
use pneumatic_core::server;
use pneumatic_beacon::{Beacon, HeartbeatError, HeartbeatResult};

const SERVER_THREAD_COUNT: usize = 100;
const HEARTBEAT_DURATION_IN_SECS: u64 = 60;

fn main() {
    let conn_factory = Box::new(TcpConnFactory::new());
    let beacon = Arc::new(Beacon::with_factory(conn_factory));

    // TODO: have to make a tx/rx to let either thread know if there's a problem with the other?
    // TODO: or do we just panic?
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
    let listener = TcpListener::bind("127.0.0.1:7878")
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
                    safe_beacon.handle_request(raw_data);
                });
            },
            // TODO: log bad requests?
            Err(_) => continue
        };
    }
}

fn check_node_heartbeats(beacon: Arc<Beacon>) {
    let mut checking = true;
    while checking {
        let start = SystemTime::now();
        match beacon.check_heartbeats() {
            HeartbeatResult::Err(HeartbeatError::ConnectionError) => {
                checking = false;
                // TODO: log these errors
                panic!("fatal error in checking for node heartbeats");
            },
            _ => {
                let mut remaining_time = HEARTBEAT_DURATION_IN_SECS;
                if let Ok(elapsed) = start.elapsed() {
                    let elapsed_secs = elapsed.as_secs();
                    remaining_time = HEARTBEAT_DURATION_IN_SECS - elapsed_secs;
                    if remaining_time < 0 { continue; }
                }
                thread::sleep(Duration::from_secs(remaining_time));
            }
        };
    }
}