use serde::{de, Deserialize, Deserializer};
use std::{fmt::Display, str::FromStr};

#[derive(Debug, Clone, Deserialize, PartialEq)]
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

fn from_str<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: FromStr,
    T::Err: Display,
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    T::from_str(&s).map_err(de::Error::custom)
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProducerSettings {
    #[serde(deserialize_with = "from_str")]
    pub acks: usize,
    #[serde(deserialize_with = "from_str")]
    pub retries: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaSettings {
    pub bootstrap_servers: String,
    #[serde(flatten)]
    pub security_protocol: SecurityProtocol,
    #[serde(flatten)]
    pub consumer: Option<ConsumerSettings>,
    // Doesn't technically need to be an option now, but future-proofing
    #[serde(flatten)]
    pub producer: Option<ProducerSettings>,
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

#[cfg(test)]
mod test {
    use super::*;
    use config::{Config, Environment};

    #[derive(Debug, Clone, Deserialize)]
    struct Test {
        kafka: KafkaSettings,
    }

    #[test]
    fn bare_settings() {
        std::env::set_var("KAFKA__BOOTSTRAP_SERVERS", "ADDRESS");
        std::env::set_var("KAFKA__SECURITY_PROTOCOL", "PLAINTEXT");

        let mut s = Config::new();
        s.merge(Environment::new().separator("__")).unwrap();
        let settings: Test = s.try_into().unwrap();
        assert_eq!(settings.kafka.bootstrap_servers, "ADDRESS".to_string());
        assert_eq!(
            settings.kafka.security_protocol,
            SecurityProtocol::Plaintext
        );
        assert!(settings.kafka.consumer.is_none());
        assert!(settings.kafka.producer.is_none());
    }

    #[test]
    fn sasl_ssl() {
        std::env::set_var("KAFKA__BOOTSTRAP_SERVERS", "ADDRESS");
        std::env::set_var("KAFKA__SECURITY_PROTOCOL", "SASL_SSL");
        std::env::set_var("KAFKA__SASL_USERNAME", "USER");
        std::env::set_var("KAFKA__SASL_PASSWORD", "PASS");

        let mut s = Config::new();
        s.merge(Environment::new().separator("__")).unwrap();
        let settings: Test = s.try_into().unwrap();
        assert_eq!(settings.kafka.bootstrap_servers, "ADDRESS".to_string());
        assert_eq!(
            settings.kafka.security_protocol,
            SecurityProtocol::SaslSsl {
                sasl_username: "USER".into(),
                sasl_password: "PASS".into()
            }
        );
    }

    #[test]
    fn consumer_settings() {
        std::env::set_var("KAFKA__BOOTSTRAP_SERVERS", "ADDRESS");
        std::env::set_var("KAFKA__SECURITY_PROTOCOL", "PLAINTEXT");
        std::env::set_var("KAFKA__GROUP_ID", "group");
        std::env::set_var("KAFKA__INPUT_TOPICS", "A,B,C");
        let mut s = Config::new();
        s.merge(Environment::new().separator("__")).unwrap();
        let settings: Test = s.try_into().unwrap();
        let consumer_settings = settings.kafka.consumer.unwrap();
        assert_eq!(consumer_settings.group_id, "group".to_string());
        assert_eq!(
            consumer_settings.input_topics,
            vec!["A".to_string(), "B".to_string(), "C".to_string()]
        );
    }

    #[test]
    fn producer_settings() {
        std::env::set_var("KAFKA__BOOTSTRAP_SERVERS", "ADDRESS");
        std::env::set_var("KAFKA__SECURITY_PROTOCOL", "PLAINTEXT");
        std::env::set_var("KAFKA__ACKS", "0");
        std::env::set_var("KAFKA__RETRIES", "0");
        let mut s = Config::new();
        s.merge(Environment::new().separator("__")).unwrap();
        let settings: Test = s.try_into().unwrap();
        let producer_settings = settings.kafka.producer.unwrap();
        assert_eq!(producer_settings.acks, 0);
        assert_eq!(producer_settings.retries, 0);
    }
}
