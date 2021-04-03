use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "security_protocol")]
pub enum SecurityProtocol {
    Plaintext,
    SaslSsl {
        sasl_username: String,
        sasl_password: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsumerSettings {
    pub group_id: String,
    #[serde(deserialize_with = "vec_from_str")]
    pub input_topics: Vec<String>,
}

pub fn vec_from_str<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(s.split(',').map(From::from).collect())
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaSettings {
    pub bootstrap_servers: String,
    #[serde(flatten)]
    pub security_protocol: SecurityProtocol,
    #[serde(flatten)]
    pub consumer: Option<ConsumerSettings>,
}

impl KafkaSettings {
    pub(crate) fn config<'a>(
        &self,
        config: &'a mut rdkafka::ClientConfig,
    ) -> &'a mut rdkafka::ClientConfig {
        config.set("bootstrap.servers", &self.bootstrap_servers);
        match &self.security_protocol {
            SecurityProtocol::Plaintext => {
                config.set("security.protocol", "PLAINTEXT");
            }
            SecurityProtocol::SaslSsl {
                sasl_username,
                sasl_password,
            } => {
                config
                    .set("security.protocol", "SASL_SSL")
                    .set("sasl.mechanism", "PLAIN")
                    .set("sasl.username", sasl_username)
                    .set("sasl.password", sasl_password);
            }
        }
        config
    }
}
