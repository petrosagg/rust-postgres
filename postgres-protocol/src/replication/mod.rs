use std::collections::HashMap;

use crate::message::backend::Parse;

pub mod pgoutput;
pub mod raw;

pub use pgoutput::PgOutput;
pub use raw::Raw;

pub trait DecodingPlugin {
    type Message: Parse;

    fn name(&self) -> &str;
    fn options(&self) -> HashMap<String, String>;
}
