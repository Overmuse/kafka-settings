use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "security_protocol")]
pub enum SecurityProtocol {
    Plaintext,
    SaslSsl {
        sasl_username: String,
        sasl_password: String,
    },
}

impl Default for SecurityProtocol {
    fn default() -> Self {
        Self::Plaintext
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaSettings {
    #[serde(default = "String::new")]
    pub bootstrap_servers: String,
    #[serde(default = "Default::default", flatten)]
    pub security_protocol: SecurityProtocol,
    #[serde(default = "String::new")]
    pub group_id: String,
    #[serde(default = "Vec::new")]
    pub input_topics: Vec<String>,
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
