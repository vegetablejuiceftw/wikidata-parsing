use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WikidataItem {
    #[serde(rename = "type")]
    pub type_: String,
    pub id: String,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_map")]
    pub labels: Vec<RegionValue>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_map")]
    pub descriptions: Vec<RegionValue>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_map_list")]
    pub aliases: Vec<RegionValue>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_map")]
    pub sitelinks: Vec<Sitelink>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_claims")]
    pub claims: Vec<HashMap<String, Value>>,
    // pub claims: Vec<Value>,
    // pageid: u64,
    // ns: u64,
    pub title: Option<String>,
    // lastrevid: u64,
    // modified: String,
}


fn deserialize_map<'de, T, D>(deserializer: D) -> Result<Vec<T>, D::Error>
    where
        T: for<'a> serde::Deserialize<'a>,
        D: serde::Deserializer<'de>,
{
    let aliases_map: HashMap<String, T> =
        serde::Deserialize::deserialize(deserializer)?;
    let aliases: Vec<T> = aliases_map.into_iter().map(|(_, alias)| alias).collect();
    Ok(aliases)
}

fn deserialize_map_list<'de, T, D>(deserializer: D) -> Result<Vec<T>, D::Error>
    where
        T: for<'a> serde::Deserialize<'a>,
        D: serde::Deserializer<'de>,
{
    let aliases_map: HashMap<String, Vec<T>> =
        serde::Deserialize::deserialize(deserializer)?;
    let aliases: Vec<T> = aliases_map
        .into_iter()
        .map(|(_, alias)| alias)
        .flatten()
        .collect();
    Ok(aliases)
}

fn deserialize_claims<'de, D>(deserializer: D) -> Result<Vec<HashMap<String, Value>>, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    let claims_map: HashMap<String, Vec<Claim>> =
        serde::Deserialize::deserialize(deserializer)?;

    let claims: Vec<Claim> = claims_map
        .into_iter()
        .map(|(_, claims)| claims)
        .flatten()
        .collect();
    //
    let mapped: Vec<Value> = claims
        .iter()
        .map(|claim| {
            // let mut map = match &claim.mainsnak.datavalue {
            //     Some(m) => {
            //         // match m.get("value") {
            //         //     None => HashMap::new(),
            //         //     Some(g) => match g {
            //         //         Value::Object(v) => v.clone().into(),
            //         //         Value::String(v) => HashMap::from([("value".to_string(), v)]),
            //         //         _ => HashMap::new()
            //         //     }
            //         // }
            //     },
            //     None => HashMap::new(),
            // };

            let mut map: Value = match &claim.mainsnak.datavalue {
                None => Value::Object(Default::default()),
                Some(v) => match v {
                    Value::Null => Value::Object(Default::default()),
                    Value::Object(v) => match v.get("value") {
                        None => v.clone().into(),
                        Some(value) => match value {
                            Value::Null => v.clone().into(),
                            Value::Bool(_) => v.clone().into(),
                            Value::Number(_) => v.clone().into(),
                            Value::String(_) => v.clone().into(),
                            Value::Array(_) => v.clone().into(),
                            Value::Object(o) => {
                                // Extract the serde_json::Map from each Value::Object
                                let mut merged_map = serde_json::Map::new();

                                if let Value::Object(map1) = o.clone().into() {
                                    merged_map.extend(map1);
                                }

                                if let Value::Object(mut map2) = v.clone().into() {
                                    map2.remove("value");
                                    merged_map.extend(map2);
                                }

                                // let merged_map: serde_json::Map<String, Value> = o
                                //     .into_iter()
                                //     .chain(v.into_iter())
                                //     .collect::<serde_json::Map<String, Value>>();

                                // Create a new Value::Object with the merged map
                                // let out = Value::Object(merged_map);

                                // let merged_map:serde_json::Map<String, Value> = o.into_iter().chain(v.into_iter()).collect();
                                // let out: Value = Value::Object(merged_map);
                                merged_map.clone().into()
                            },
                        }
                    },
                    _ => Value::Object(Default::default()),
                }
            };




            // // Convert the existing object to a Map
            // if let Value::Object(ref mut map) = map {
            //     // Insert the new key-value pair
            //     // map.extend(map.into_iter());
            //     map.insert("property".to_string(), claim.mainsnak.property.to_string().into());
            // };

            // Convert the existing object to a Map
            if let Value::Object(ref mut map) = map {
                // Insert the new key-value pair
                map.insert("property".to_string(), claim.mainsnak.property.to_string().into());
            };

            // let mut map = match map {
            //     Value::Object(mut m) => {
            //         m.insert("property".to_string(), claim.mainsnak.property.to_string().into());
            //         m
            //     },
            //     _ => Value::Object(Default::default()),
            // };
            map
        })
        .collect();

    let out = mapped.into_iter().filter_map(|value| {
        match value {
            Value::Object(map) => {
                let m: HashMap<String, Value> = map.into_iter().collect();
                Some(m)
            },
            _ => None,
        }
    }).collect();
    Ok(out)
}


#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Claim {
    // #[serde(rename = "type")]
    // type_: String,
    // id: String,
    // rank: String,
    pub mainsnak: Mainsnak,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Mainsnak {
    // snaktype: String,
    pub property: String,
    // datavalue: Option<HashMap<String, Value>>,
    pub datavalue: Option<Value>,
    // datatype: String,
}

// #[derive(Debug, Serialize, Deserialize)]
// pub(crate) struct Datavalue {
//     // value: Option<String>,
//     // #[serde(rename = "type")]
//     // datatype: String,
// }

#[derive(Debug, Deserialize)]
pub(crate) struct Sitelink {
    pub site: String,
    pub title: String,
    pub badges: Vec<String>,
}


#[derive(Debug, Deserialize)]
pub(crate) struct RegionValue {
    pub language: String,
    pub value: String,
}
impl Serialize for RegionValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        let tuple = (&self.language, &self.value);
        tuple.serialize(serializer)
    }
}
impl Serialize for Sitelink {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        let badges = if &self.badges.len() > &0 { Some(&self.badges) } else { None };
        let tuple = (&self.site, &self.title, badges);
        tuple.serialize(serializer)
    }
}

