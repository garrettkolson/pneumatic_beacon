use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::thread;
use std::thread::JoinHandle;
use dashmap::DashMap;
use pneumatic_core::{config, conns, encoding, node::*};
use pneumatic_core::conns::{Sender};
use pneumatic_core::data::DataProvider;
use strum::IntoEnumIterator;

pub struct Beacon {
    config: config::Config,
    committers: Arc<DashMap<Vec<u8>, IpAddr>>,
    sentinels: Arc<DashMap<Vec<u8>, IpAddr>>,
    executors: Arc<DashMap<Vec<u8>, IpAddr>>,
    finalizers: Arc<DashMap<Vec<u8>, IpAddr>>,
    conn_factory: Arc<Box<dyn conns::ConnFactory>>,
    data_provider: DataProvider
}

impl Beacon {

    ///////////// factory methods //////////////

    pub fn init(config: config::Config, conn_factory: Box<dyn conns::ConnFactory>) -> Beacon {
        let data_provider = DataProvider::from_config(&config);
        Beacon {
            config,
            committers: Arc::new(DashMap::new()),
            sentinels: Arc::new(DashMap::new()),
            executors: Arc::new(DashMap::new()),
            finalizers: Arc::new(DashMap::new()),
            conn_factory: Arc::new(conn_factory),
            data_provider
        }
    }

    ////////////// public methods //////////////

    pub fn handle_request(&self, data: Vec<u8>) -> Vec<u8> {
        let request = match encoding::deserialize_rmp_to::<NodeRequest>(data) {
            Ok(req) => req,
            Err(_) => return Self::get_heartbeat()
        };

        match request.request_type {
            NodeRequestType::Register => self.register_node(request),
            NodeRequestType::Request => self.request_nodes(request),
            NodeRequestType::Heartbeat => Self::get_heartbeat()
        }
    }

    pub fn check_heartbeats(&self) -> HeartbeatResult {
        for node_type in NodeRegistryType::iter() {
            if let HeartbeatResult::NodesNeeded(n) = self.check_nodes(node_type) {
                self.broadcast_node_request(&n);
            }
        }

        HeartbeatResult::Ok
    }

    /////////////// heartbeats /////////////////

    fn check_nodes(&self, node_type: NodeRegistryType) -> HeartbeatResult {
        let nodes = match self.get_nodes(&node_type) {
            Some(n) => n,
            None => return HeartbeatResult::Ok
        };

        let mut handles: Vec<JoinHandle<Heartbeat>> = vec![];
        for node in nodes.iter() {
            let (conn, addr) = self.get_conn_and_addr(node.value().clone(), conns::HEARTBEAT_PORT);
            let node_key = node.key().clone();

            handles.push(thread::spawn(move || {
                let heartbeat = Self::get_heartbeat();
                match conn.get_response(addr, &heartbeat) {
                    Ok(_) => Heartbeat::Alive(node_key),
                    Err(_) => Heartbeat::Dead(node_key)
                }
            }));
        }

        let mut dead_nodes: Vec<InternalRegistration> = vec![];
        for handle in handles {
            if let Ok(Heartbeat::Dead(node_key)) = handle.join() {
                nodes.remove(&node_key);
                dead_nodes.push(
                    InternalRegistration::for_removal(node_key, vec![ node_type.clone() ]));
            }
        }

        if dead_nodes.len() > 0 {
            let batch = InternalRegistrationBatch::Remove(dead_nodes);
            self.push_registration_update(batch);
        }

        let min = self.get_min_node_number(&node_type);
        match nodes.len() {
            n if n >= min => HeartbeatResult::Ok,
            _ => HeartbeatResult::NodesNeeded(node_type)
        }
    }

    fn broadcast_node_request(&self, node_type: &NodeRegistryType) {
        let request = self.get_broadcast_request(node_type);
        let data = Arc::new(RwLock::new(encoding::serialize_to_bytes_rmp(request)
            .expect("Fatal serialization error broadcasting node request")));

        let mut handles: Vec<JoinHandle<Vec<u8>>> = vec![];
        for n in NodeRegistryType::iter() {
            let nodes = match self.get_nodes(&n) {
                Some(nodes) => nodes,
                None => continue
            };

            for node in nodes.iter() {
                let cloned_data = Arc::clone(&data);
                let (conn, addr) = self.get_conn_and_addr(node.value().clone(), conns::BEACON_PORT);
                handles.push(conns::send_on_thread(cloned_data, conn, addr));
            }
        }

        for handle in handles {
            let response_data = match handle.join() {
                Ok(d) => d,
                Err(_) => continue
            };

            let response = match encoding::deserialize_rmp_to::<NodeRegistryResponse>(response_data) {
                Ok(r) => r,
                Err(_) => continue
            };

            self.handle_registry_response(node_type, response.entries);
        }
    }

    //////////// registration //////////////

    fn register_node(&self, request: NodeRequest) -> Vec<u8> {
        for node_type in request.requester_types {
            if let Some(nodes) = self.get_nodes(&node_type) {
                if !self.verify_node(&node_type, &request.requester_key) { continue; }
                nodes.insert(request.requester_key.clone(), request.requester_ip.clone());
                let reg = InternalRegistration::for_add(request.requester_key.clone(),
                                                        request.requester_ip.clone(),
                                                        vec![ node_type ]);
                let batch = InternalRegistrationBatch::Add(vec![reg]);
                self.push_registration_update(batch);
            };
        }

        Self::get_heartbeat()
    }

    fn handle_registry_response(&self, node_type: &NodeRegistryType, entries: Vec<NodeRegistryEntry>) {
        if let Some(nodes) = self.get_nodes(node_type) {
            let mut new_nodes: Vec<InternalRegistration> = vec![];
            for entry in entries {
                if !self.verify_node(node_type, &entry.node_key) { continue; }
                nodes.insert(entry.node_key.clone(), entry.node_ip.clone());
                new_nodes.push(
                    InternalRegistration::for_add(entry.node_key, entry.node_ip,
                                                  vec![ node_type.clone() ]));
            }

            if new_nodes.len() == 0 { return; }
            let batch = InternalRegistrationBatch::Add(new_nodes);
            self.push_registration_update(batch);
        }
    }

    // TODO: what about doing this with a validator trait?
    fn verify_node(&self, node_type: &NodeRegistryType, key: &Vec<u8>) -> bool {
        if self.node_is_already_registered(key, node_type) { return false; }
        if self.type_is_maxed_out(node_type) { return false; }

        // TODO: have to verify that this node has the proper minimum balance for this type?
        // TODO: or just have the node functions do this, because a DataProvider is required?

        true
    }

    fn request_nodes(&self, request: NodeRequest) -> Vec<u8> {
        let response = NodeRegistryResponse {
            responder_key: self.config.public_key.clone(),
            responder_ip: self.config.ip_address.clone(),
            registry_type: request.requested_type.clone(),
            entries: match self.get_nodes(&request.requested_type) {
                None => vec![],
                Some(nodes) => nodes.iter().map(|n| {
                    NodeRegistryEntry {
                        node_key: n.key().clone(),
                        node_ip: n.value().clone()
                    }
                }).collect()
            }
        };

        encoding::serialize_to_bytes_rmp(response)
            .unwrap_or_else(|_| Self::get_heartbeat())
    }

    fn push_registration_update(&self, batch: InternalRegistrationBatch) {
        let data = Arc::new(RwLock::new(encoding::serialize_to_bytes_rmp(&batch)
            .expect("Fatal serialization error pushing registration update")));

        let addr = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let mut handles: Vec<JoinHandle<Vec<u8>>> = vec![];
        for n_type in &self.config.node_registry_types {
            let cloned_data = Arc::clone(&data);
            let (conn, socket) = self.get_conn_and_addr(addr, conns::get_internal_port(n_type));
            handles.push(conns::send_on_thread(cloned_data, conn, socket));
        }

        for handle in handles {
            let _ = handle.join();
        }
    }

    ///////////////////// Common //////////////////////

    fn get_conn_and_addr(&self, node_addr: IpAddr, port: u16) -> (Box<dyn Sender>, SocketAddr) {
        let factory = Arc::clone(&self.conn_factory);
        let conn = factory.get_sender();
        let addr = SocketAddr::new(node_addr, port);
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

    fn node_is_already_registered(&self, key: &Vec<u8>, node_type: &NodeRegistryType) -> bool {
        match self.get_nodes(node_type) {
            Some(nodes) => nodes.contains_key(key),
            None => false
        }
    }

    fn get_min_node_number(&self, node_type: &NodeRegistryType) -> usize {
        match self.config.type_configs.get(node_type) {
            Some(node) => node.min,
            None => 0
        }
    }

    fn get_max_node_number(&self, node_type: &NodeRegistryType) -> usize {
        match self.config.type_configs.get(node_type) {
            Some(node) => node.max,
            None => 0
        }
    }

    fn type_is_maxed_out(&self, node_type: &NodeRegistryType) -> bool {
        match self.get_nodes(node_type) {
            Some(nodes) => nodes.len() >= self.get_max_node_number(node_type),
            None => true
        }
    }

    fn get_heartbeat() -> Vec<u8> {
        vec![1u8, 2u8, 3u8, 4u8]
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