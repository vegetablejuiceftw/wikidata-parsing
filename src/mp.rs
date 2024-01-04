use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WikidataMessage {
    #[serde(rename = "type")]
    pub type_: String,
    pub id: String,
    pub labels: Vec<(String, String)>,
    pub descriptions: Vec<(String, String)>,
    pub aliases: Vec<(String, String)>,
    pub sitelinks: Vec<(String, String, Value)>,
    pub claims: Vec<HashMap<String, Value>>,
    pub title: Option<String>,
}
