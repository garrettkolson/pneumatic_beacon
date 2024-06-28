use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use dashmap::DashMap;
use pneumatic_core::{config, conns, encoding, node::*};
use pneumatic_core::conns::{FireAndForgetSender, Sender};
use strum::IntoEnumIterator;

pub struct Beacon {
    config: config::Config,
    committers: Arc<DashMap<Vec<u8>, IpAddr>>,
    sentinels: Arc<DashMap<Vec<u8>, IpAddr>>,
    executors: Arc<DashMap<Vec<u8>, IpAddr>>,
    finalizers: Arc<DashMap<Vec<u8>, IpAddr>>,
    conn_factory: Arc<Box<dyn conns::ConnFactory>>,
}

impl Beacon {

    ///////////// factory methods //////////////

    pub fn init(config: config::Config, conn_factory: Box<dyn conns::ConnFactory>) -> Beacon {
        Beacon {
            config,
            committers: Arc::new(DashMap::new()),
            sentinels: Arc::new(DashMap::new()),
            executors: Arc::new(DashMap::new()),
            finalizers: Arc::new(DashMap::new()),
            conn_factory: Arc::new(conn_factory),
        }
    }

    /////////////// heartbeats /////////////////

    pub fn check_heartbeats(&self) -> HeartbeatResult {
        for node_type in NodeRegistryType::iter() {
            if let HeartbeatResult::NodesNeeded(n) = self.check_nodes(node_type) {
                self.broadcast_node_request(&n);
            }
        }

        HeartbeatResult::Ok
    }

    fn check_nodes(&self, node_type: NodeRegistryType) -> HeartbeatResult {
        let nodes = match self.get_nodes(&node_type) {
            Some(n) => n,
            None => return HeartbeatResult::Ok
        };

        let mut handles: Vec<JoinHandle<Heartbeat>> = vec![];
        for node in nodes.iter() {
            let (conn, addr) = self.get_heartbeat_conn(node.value().clone());
            let node_key = node.key().clone();

            handles.push(thread::spawn(move || {
                let heartbeat = Self::get_heartbeat();
                match conn.get_response(addr, &heartbeat) {
                    Ok(_) => Heartbeat::Alive(node_key),
                    Err(_) => Heartbeat::Dead(node_key)
                }
            }));
        }

        for handle in handles {
            if let Ok(Heartbeat::Dead(node_key)) = handle.join() {
                nodes.remove(&node_key);
                // TODO: write something here to PUSH heartbeat failure to local processes
                // TODO: so they can remove that node from their local lists
            }
        }

        let min = self.get_min_node_number(&node_type);
        match nodes.len() {
            n if n >= min => HeartbeatResult::Ok,
            _ => HeartbeatResult::NodesNeeded(node_type)
        }
    }

    fn broadcast_node_request(&self, node_type: &NodeRegistryType) {
        let mut handles: Vec<JoinHandle<_>> = vec![];
        for n in NodeRegistryType::iter() {
            let nodes = match self.get_nodes(&n) {
                Some(nodes) => nodes,
                None => continue
            };

            for node in nodes.iter() {
                let (conn, addr) = self.get_broadcast_conn(node.value().clone());
                let request = self.get_broadcast_request(node_type);

                handles.push(thread::spawn(move || {
                    if let Ok(data) = encoding::serialize_to_bytes_rmp(request) {
                        // TODO: this needs to get the response back to add those nodes to the local list
                        conn.send(addr, &data);
                    }
                }));
            }
        }

        for handle in handles {
            let _ = handle.join();
        }
    }

    fn get_heartbeat_conn(&self, node_addr: IpAddr) -> (Box<dyn Sender>, SocketAddr) {
        let factory = Arc::clone(&self.conn_factory);
        let conn = factory.get_sender();
        let addr = SocketAddr::new(node_addr, conns::HEARTBEAT_PORT);
        (conn, addr)
    }

    fn get_broadcast_conn(&self, node_addr: IpAddr) -> (Box<dyn FireAndForgetSender>, SocketAddr) {
        let factory = Arc::clone(&self.conn_factory);
        let conn = factory.get_faf_sender();
        let addr = SocketAddr::new(node_addr, conns::BEACON_PORT);
        (conn, addr)
    }

    fn get_broadcast_request(&self, node_type: &NodeRegistryType) -> NodeRequest {
        NodeRequest {
            requested_type: node_type.clone(),
            requester_ip: self.config.ip_address.clone(),
            requester_key: self.config.public_key.clone(),
            requester_types: vec![],
            request_type: NodeRequestType::Request
        }
    }

    fn get_nodes(&self, node_registry_type: &NodeRegistryType) -> Option<Nodes> {
        match node_registry_type {
            NodeRegistryType::Committer => Some(Arc::clone(&self.committers)),
            NodeRegistryType::Sentinel => Some(Arc::clone(&self.sentinels)),
            NodeRegistryType::Executor => Some(Arc::clone(&self.executors)),
            NodeRegistryType::Finalizer => Some(Arc::clone(&self.finalizers)),
            _ => None
        }
    }

    fn get_min_node_number(&self, node_type: &NodeRegistryType) -> usize {
        match self.config.type_configs.get(node_type) {
            Some(node) => node.min,
            None => 0
        }
    }

    //////////// external requests //////////////

    pub fn handle_request(&self, data: Vec<u8>) -> Vec<u8> {
        let request = match encoding::deserialize_rmp_to::<NodeRequest>(data) {
            Ok(req) => req,
            Err(_) => return Self::get_heartbeat()
        };

        match request.request_type {
            NodeRequestType::Register => self.register_node(request),
            NodeRequestType::Request => self.request_node(request),
            NodeRequestType::Heartbeat => Self::get_heartbeat()
        }
    }

    fn register_node(&self, request: NodeRequest) -> Vec<u8> {
        // TODO: have to make these checks more robust before allowing the registration
        for node_type in request.requester_types {
            if let Some(nodes) = self.get_nodes(&node_type) {
                nodes.insert(request.requester_key.clone(), request.requester_ip.clone());
                // TODO: write something to PUSH this registration to local processes
            };
        }

        // TODO: fix this to return something meaningful
        vec![]
    }

    fn request_node(&self, request: NodeRequest) -> Vec<u8> {
        // TODO: need to redo this to just return serialized list of requested type
        // TODO: log non-matched type requests
        // let (nodes, port) = match request.requested_type {
        //     NodeRegistryType::Committer => (Arc::clone(&self.committers), conns::COMMITTER_PORT),
        //     NodeRegistryType::Sentinel => (Arc::clone(&self.sentinels), conns::SENTINEL_PORT),
        //     NodeRegistryType::Executor => (Arc::clone(&self.executors), conns::EXECUTOR_PORT),
        //     NodeRegistryType::Finalizer => (Arc::clone(&self.finalizers), conns::FINALIZER_PORT),
        //     _ => return
        // };
        //
        // // TODO: log serialization errors
        // let data = match encoding::serialize_to_bytes_rmp(request) {
        //     Ok(d) => d,
        //     Err(_) => return
        // };
        //
        // for node in nodes.iter() {
        //     let addr = SocketAddr::new(node.value().clone(), port);
        //     let sender = self.conn_factory.get_faf_sender();
        //     sender.send(addr, &*data);
        // }
        todo!()
    }

    fn get_heartbeat() -> Vec<u8> {
        vec![1u8]
    }
}

pub type Nodes = Arc<DashMap<Vec<u8>, IpAddr>>;

pub enum Heartbeat {
    Alive(Vec<u8>),
    Dead(Vec<u8>)
}

pub enum HeartbeatResult {
    Ok,
    NodesNeeded(NodeRegistryType),
    Err(HeartbeatError)
}

pub enum HeartbeatError {
    NoNodesConnected,
    ConnectionError,
    IncorrectNodeType
}

#[cfg(test)]
mod tests {
    use std::net::{SocketAddr};
    use pneumatic_core::conns::{ConnFactory, Sender, FireAndForgetSender, ConnError};

    // TODO: write the tests

    pub struct SendFakeStuff {
        sent: Option<Vec<u8>>
    }

    impl FireAndForgetSender for SendFakeStuff {
        fn send(&self, addr: SocketAddr, data: &[u8]) {
            todo!()
        }
    }

    pub struct SendStuff {
        sent: Option<Vec<u8>>
    }

    impl Sender for SendStuff {
        fn get_response(&self, addr: SocketAddr, data: &[u8]) -> Result<Vec<u8>, ConnError> {
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