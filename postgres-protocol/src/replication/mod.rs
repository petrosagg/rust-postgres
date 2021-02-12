use std::collections::HashMap;

use crate::message::backend::Parse;

pub trait DecodingPlugin {
    type Message: Parse;

    fn name(&self) -> &str;
    fn options(&self) -> HashMap<String, String>;
}
