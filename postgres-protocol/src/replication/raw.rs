use std::collections::HashMap;

use bytes::Bytes;

use crate::replication::DecodingPlugin;

#[derive(Clone)]
pub struct Raw {
    name: String,
    options: HashMap<String, String>,
}

impl Raw {
    pub fn new(name: String, options: HashMap<String, String>) -> Self {
        Self { name, options }
    }
}

impl DecodingPlugin for Raw {
    type Message = Bytes;

    fn name(&self) -> &str {
        &self.name
    }

    fn options(&self) -> HashMap<String, String> {
        self.options.clone()
    }
}
