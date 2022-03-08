pub mod pythd;
pub mod solana;
pub mod store;

use std::sync::Arc;

use self::{
    pythd::{
        adapter::Adapter,
        api::rpc::{ConnectionFactory, Server},
    },
    store::{global, local},
};

pub struct Publisher {
    pythd_server: Server,
}

impl Publisher {
    pub fn new() -> Self {
        todo!();

        // // Create the stores
        // let local_store = Arc::new(local::Store::new());
        // let global_store = Arc::new(global::Store::new());

        // // Create the pythd API server and adapter
        // let pythd_adapter = Adapter::new(local_store, global_store);
        // let listen_port = 8900;
        // let protocol_factory = ConnectionFactory {
        //     protocol: Arc::new(Box::new(pythd_adapter)),
        // };
        // let pythd_server = Server::new(protocol_factory, listen_port);

        // TODO

        // Publisher { pythd_server }
    }

    pub async fn run(&self) {
        // TODO
        todo!();
    }
}
