use std::net::{Ipv6Addr, SocketAddrV6};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use dashmap::DashMap;
use pneumatic_core::{conns, encoding, node::*};
use pneumatic_core::conns::Sender;

pub struct Beacon {
    committers: Arc<DashMap<Vec<u8>, Ipv6Addr>>,
    sentinels: Arc<DashMap<Vec<u8>, Ipv6Addr>>,
    executors: Arc<DashMap<Vec<u8>, Ipv6Addr>>,
    finalizers: Arc<DashMap<Vec<u8>, Ipv6Addr>>,
    conn_factory: Arc<Box<dyn conns::ConnFactory>>
}

impl Beacon {
    pub fn with_factory(conn_factory: Box<dyn conns::ConnFactory>) -> Beacon {
        Beacon {
            committers: Arc::new(DashMap::new()),
            sentinels: Arc::new(DashMap::new()),
            executors: Arc::new(DashMap::new()),
            finalizers: Arc::new(DashMap::new()),
            conn_factory: Arc::new(conn_factory)
        }
    }

    pub fn check_heartbeats(&self) -> HeartbeatResult {

        // TODO: decide how to remove unresponsive nodes from collections
        let mut handles: Vec<JoinHandle<_>> = vec![];
        for committer in self.committers.iter() {
            let (conn, addr) = self.get_heartbeat_conn(committer.value().clone());

            handles.push(thread::spawn(move || {
                let response = conn.get_response_from_v6(addr, &[1u8]);
            }));
        }

        for sentinel in self.sentinels.iter() {
            let (conn, addr) = self.get_heartbeat_conn(sentinel.value().clone());

            handles.push(thread::spawn(move || {
                let response = conn.get_response_from_v6(addr, &[1u8]);
            }));
        }

        for executor in self.executors.iter() {
            let (conn, addr) = self.get_heartbeat_conn(executor.value().clone());

            handles.push(thread::spawn(move || {
                let response = conn.get_response_from_v6(addr, &[1u8]);
            }));
        }

        for finalizer in self.finalizers.iter() {
            let (conn, addr) = self.get_heartbeat_conn(finalizer.value().clone());

            handles.push(thread::spawn(move || {
                let response = conn.get_response_from_v6(addr, &[1u8]);
            }));
        }

        for handle in handles {
            let _ = handle.join();
        }

        // TODO: if more nodes are needed, send out requests

        HeartbeatResult::Ok
    }

    fn get_heartbeat_conn(&self, node_addr: Ipv6Addr) -> (Box<dyn Sender>, SocketAddrV6) {
        let factory = Arc::clone(&self.conn_factory);
        let conn = factory.get_sender();
        let addr = SocketAddrV6::new(node_addr, conns::HEARTBEAT_PORT, 0, 0);
        (conn, addr)
    }

    pub fn handle_request(&self, data: Vec<u8>){
        // TODO: log failed deserializations
        let request = match encoding::deserialize_rmp_to::<NodeRequest>(data) {
            Ok(req) => req,
            Err(_) => return
        };

        match request.request_type {
            NodeRequestType::Register => self.register_node(request),
            NodeRequestType::Request => self.request_node(request),
            _ => return
        }
    }

    // TODO: put this in pneumatic_node
    fn respond_with_heartbeat(&self, request: NodeRequest) {
        todo!()
    }

    fn register_node(&self, request: NodeRequest) {
        for node_type in request.requester_types {
            let nodes = match node_type {
                NodeRegistryType::Committer => Arc::clone(&self.committers),
                NodeRegistryType::Sentinel => Arc::clone(&self.sentinels),
                NodeRegistryType::Executor => Arc::clone(&self.executors),
                NodeRegistryType::Finalizer => Arc::clone(&self.finalizers),
                _ => continue
            };

            nodes.insert(request.requester_key.clone(), request.requester_ip.clone());
        }
    }

    fn request_node(&self, request: NodeRequest) {
        // TODO: log non-matched type requests
        let (nodes, port) = match request.requested_type {
            NodeRegistryType::Committer => (Arc::clone(&self.committers), conns::COMMITTER_PORT),
            NodeRegistryType::Sentinel => (Arc::clone(&self.sentinels), conns::SENTINEL_PORT),
            NodeRegistryType::Executor => (Arc::clone(&self.executors), conns::EXECUTOR_PORT),
            NodeRegistryType::Finalizer => (Arc::clone(&self.finalizers), conns::FINALIZER_PORT),
            _ => return
        };

        // TODO: log serialization errors
        let data = match encoding::serialize_to_bytes_rmp(request) {
            Ok(d) => d,
            Err(_) => return
        };

        for node in nodes.iter() {
            let addr = SocketAddrV6::new(node.value().clone(), port, 0, 0);
            let sender = self.conn_factory.get_faf_sender();
            sender.send_to_v6(addr, &*data);
        }
    }
}

pub enum HeartbeatResult {
    Ok,
    NodesNeeded(Vec<NodeRegistryType>),
    Err(HeartbeatError)
}

pub enum HeartbeatError {
    NoNodesConnected,
    ConnectionError
}

#[cfg(test)]
mod tests {
    use std::net::{SocketAddrV4, SocketAddrV6};
    use pneumatic_core::conns::{ConnFactory, Sender, FireAndForgetSender, ConnError};

    // TODO: write the tests

    pub struct SendFakeStuff {
        sent: Option<Vec<u8>>
    }

    impl FireAndForgetSender for SendFakeStuff {
        fn send_to_v4(&self, addr: SocketAddrV4, data: &[u8]) {
            todo!()
        }

        fn send_to_v6(&self, addr: SocketAddrV6, data: &[u8]) {
            todo!()
        }
    }

    pub struct SendStuff {
        sent: Option<Vec<u8>>
    }

    impl Sender for SendStuff {
        fn get_response_from_v4(&self, addr: SocketAddrV4, data: &[u8]) -> Result<Vec<u8>, ConnError> {
            todo!()
        }

        fn get_response_from_v6(&self, addr: SocketAddrV6, data: &[u8]) -> Result<Vec<u8>, ConnError> {
            todo!()
        }
    }

    pub struct FakeConnFactory { }

    impl ConnFactory for FakeConnFactory {
        fn get_sender(&self) -> SendStuff {
            SendStuff {
                sent: None
            }
        }

        fn get_faf_sender(&self) -> SendFakeStuff {
            SendFakeStuff {
                sent: None
            }
        }
    }
}