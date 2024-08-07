use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::thread;
use std::thread::JoinHandle;
use dashmap::DashMap;
use pneumatic_core::{config, conns, encoding::*, node::*};
use pneumatic_core::conns::{Sender};
use pneumatic_core::data::DataProvider;
use pneumatic_core::user::User;
use strum::IntoEnumIterator;

pub struct Beacon {
    config: config::Config,
    registry: NodeRegistry,
    conn_factory: Arc<Box<dyn conns::ConnFactory>>,
}

impl Beacon {

    ///////////// factory methods //////////////

    pub fn init(config: config::Config, conn_factory: Box<dyn conns::ConnFactory>) -> Beacon {
        Beacon {
            config,
            registry: NodeRegistry::init(),
            conn_factory: Arc::new(conn_factory),
        }
    }

    ////////////// public methods //////////////

    pub fn handle_request(&self, data: Vec<u8>) -> Vec<u8> {
        let Ok(request) = deserialize_rmp_to::<NodeRequest>(&data)
            else { return Self::get_heartbeat() };

        match request.request_type {
            NodeRequestType::Register => self.register_node(request),
            NodeRequestType::Request => self.request_nodes(request),
            NodeRequestType::Heartbeat => Self::get_heartbeat()
        }
    }

    pub fn check_heartbeats(&self) -> HeartbeatResult {
        let _: Vec<_> = NodeRegistryType::iter().map(|t| -> () {
            if let HeartbeatResult::NodesNeeded(n) = self.check_nodes(t) {
                self.broadcast_node_request(&n);
            }
        }).collect();

        HeartbeatResult::Ok
    }

    /////////////// heartbeats /////////////////

    fn check_nodes(&self, node_type: NodeRegistryType) -> HeartbeatResult {
        let Some(nodes) = self.registry.get_nodes(&node_type)
            else { return HeartbeatResult::Ok };

        let threads: Vec<JoinHandle<Heartbeat>> = nodes.iter().map(|node| -> JoinHandle<Heartbeat> {
            let (conn, addr) = self.get_conn_and_addr(node.value().clone(), conns::HEARTBEAT_PORT);
            let node_key = node.key().clone();

            thread::spawn(move || {
                let heartbeat = Self::get_heartbeat();
                match conn.get_response(addr, &heartbeat) {
                    Ok(_) => Heartbeat::Alive(node_key),
                    Err(_) => Heartbeat::Dead(node_key)
                }
            })
        }).collect();

        let mut dead_nodes: Vec<Registration> = vec![];
        for thread in threads {
            if let Ok(Heartbeat::Dead(node_key)) = thread.join() {
                nodes.remove(&node_key);
                dead_nodes.push(
                    Registration::for_removal(node_key, vec![ node_type.clone() ]));
            }
        }

        if dead_nodes.len() > 0 {
            let batch = RegistrationBatch::Remove(dead_nodes);
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
        let data = Arc::new(RwLock::new(serialize_to_bytes_rmp(&request)
            .expect("Fatal serialization error broadcasting node request")));

        let mut threads: Vec<JoinHandle<Vec<u8>>> = vec![];
        for n in NodeRegistryType::iter() {
            let Some(nodes) = self.registry.get_nodes(&n) else { continue };

            for node in nodes.iter() {
                let cloned_data = Arc::clone(&data);
                let (conn, addr) = self.get_conn_and_addr(node.value().clone(), conns::BEACON_PORT);
                threads.push(conns::send_on_thread(cloned_data, conn, addr));
            }
        }

        threads.into_iter().for_each(|thread| -> () {
            let Ok(response_data) = thread.join() else { return };
            let Ok(response) = deserialize_rmp_to::<NodeRegistryResponse>(&response_data)
                else { return };

            self.handle_registry_response(node_type, response.entries);
        })
    }

    //////////// registration //////////////

    fn register_node(&self, request: NodeRequest) -> Vec<u8> {
        for node_type in request.requester_types {
            let Some(nodes) = self.registry.get_nodes(&node_type) else { continue };
            if !self.node_is_ok(&node_type, &request.requester_key) { continue; }
            let Ok(Some(registration)) = self.get_node_registration(nodes.clone(),
                                                                &node_type,
                                                                request.requester_key.clone(),
                                                                request.requester_ip).join()
                else { continue };
            let batch = RegistrationBatch::Add(vec![registration]);
            self.push_registration_update(batch);
        }

        Self::get_heartbeat()
    }

    fn handle_registry_response(&self, node_type: &NodeRegistryType, entries: Vec<NodeRegistryEntry>) {
        let Some(nodes) = self.registry.get_nodes(node_type) else { return };
        let threads: Vec<JoinHandle<Option<Registration>>> = entries.into_iter()
            .map(|entry| -> JoinHandle<Option<Registration>> {
                if !self.node_is_ok(node_type, &entry.node_key) { return thread::spawn(|| return None) };
                self.get_node_registration(nodes.clone(), node_type, entry.node_key, entry.node_ip)
            })
            .collect();

        let mut new_nodes: Vec<Registration> = vec![];
        for thread in threads {
            let Ok(Some(registration)) = thread.join() else { continue };
            new_nodes.push(registration);
        }

        if new_nodes.len() == 0 { return; }
        let batch = RegistrationBatch::Add(new_nodes);
        self.push_registration_update(batch);
    }

    fn node_is_ok(&self, node_type: &NodeRegistryType, key: &Vec<u8>) -> bool {
        !self.registry.node_is_already_registered(key, node_type) && !self.type_is_maxed_out(node_type)
    }

    fn get_node_registration(&self, nodes: Nodes, node_type: &NodeRegistryType, key: Vec<u8>, ip: IpAddr)
        -> JoinHandle<Option<Registration>> {
        let environment_id = self.config.main_environment_id.clone();
        let min_stake = self.get_min_type_stake(node_type);
        let cloned_type = node_type.clone();
        thread::spawn(move || {
            match Self::check_db_node_user(&key, &environment_id, min_stake) {
                false => None,
                true => {
                    nodes.insert(key.clone(), ip.clone());
                    let reg = Registration::for_add(key, ip, vec![ cloned_type ]);
                    Some(reg)
                }
            }
        })
    }

    fn check_db_node_user(user_key: &Vec<u8>, environment_id: &str, min_stake: u64) -> bool {
        let Ok(locked_token) = DataProvider::get_token(user_key, environment_id) else { return false };
        let Ok(token) = locked_token.read() else { return false };
        let Some(user) = token.get_asset::<User>() else { return false };
        user.fuel_balance > min_stake
    }

    fn request_nodes(&self, request: NodeRequest) -> Vec<u8> {
        let response = NodeRegistryResponse {
            responder_key: self.config.public_key.clone(),
            responder_ip: self.config.ip_address.clone(),
            registry_type: request.requested_type.clone(),
            entries: match self.registry.get_nodes(&request.requested_type) {
                None => vec![],
                Some(nodes) => nodes.iter().map(|n| {
                    NodeRegistryEntry {
                        node_key: n.key().clone(),
                        node_ip: n.value().clone()
                    }
                }).collect()
            }
        };

        serialize_to_bytes_rmp(&response)
            .unwrap_or_else(|_| Self::get_heartbeat())
    }

    fn push_registration_update(&self, batch: RegistrationBatch) {
        let data = Arc::new(RwLock::new(serialize_to_bytes_rmp(&batch)
            .expect("Fatal serialization error pushing registration update")));

        let addr = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let threads: Vec<JoinHandle<Vec<u8>>> = self.config.node_registry_types.iter()
            .map(|t| -> JoinHandle<Vec<u8>> {
                let cloned_data = Arc::clone(&data);
                let (conn, socket) = self.get_conn_and_addr(addr, conns::get_internal_port(t));
                conns::send_on_thread(cloned_data, conn, socket)
            })
            .collect();

        threads.into_iter().for_each(|h| -> () {
            let _ = h.join().unwrap_or_else(|_| vec![]);
        });
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
        match self.registry.get_nodes(node_type) {
            Some(nodes) => nodes.len() >= self.get_max_node_number(node_type),
            None => true
        }
    }

    fn get_min_type_stake(&self, node_type: &NodeRegistryType) -> u64 {
        match self.config.type_configs.get(node_type) {
            Some(config) => config.min_stake,
            None => u64::MAX
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