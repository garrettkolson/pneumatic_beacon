use std::net::{Ipv6Addr, SocketAddrV6};
use std::sync::Arc;
use dashmap::DashMap;
use pneumatic_core::{conns, encoding, node::*};

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
        HeartbeatResult::Ok
    }

    pub fn handle_request(&self, data: Vec<u8>){
        // TODO: log failed deserializations
        let request = match encoding::deserialize_rmp_to::<NodeRequest>(data) {
            Ok(req) => req,
            Err(_) => return
        };

        match request.request_type {
            NodeRequestType::Heartbeat => self.respond_with_heartbeat(request),
            NodeRequestType::Register => self.register_node(request),
            NodeRequestType::Request => self.request_node(request)
        }
    }

    // TODO: need to probably pass the TcpStream through here to properly respond
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
    Err(HeartbeatError)
}

pub enum HeartbeatError {
    NoNodesConnected,
    ConnectionError
}

#[cfg(test)]
mod tests {
    use std::net::{SocketAddrV4, SocketAddrV6};
    use pneumatic_core::conns::{ConnFactory, FireAndForgetSender};

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

    pub struct FakeConnFactory { }

    impl ConnFactory for FakeConnFactory {
        fn get_faf_sender(&self) -> SendFakeStuff {
            SendFakeStuff {
                sent: None
            }
        }
    }
}