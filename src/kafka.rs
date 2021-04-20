use crate::settings::{Acks, KafkaSettings};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    producer::FutureProducer,
    ClientConfig,
};

pub fn consumer(settings: &KafkaSettings) -> Result<StreamConsumer, KafkaError> {
    let mut config = ClientConfig::new();
    let config = settings.config(&mut config);
    let consumer_settings = settings
        .consumer
        .clone()
        .expect("Consumer settings not specified");
    let consumer: StreamConsumer = config
        .set("group.id", &consumer_settings.group_id)
        // TODO: Figure out how to remove this setting
        .set("enable.ssl.certificate.verification", "false")
        .create()?;
    let subscription_topics: Vec<_> = consumer_settings
        .input_topics
        .iter()
        .map(String::as_str)
        .collect();
    consumer.subscribe(&subscription_topics)?;
    Ok(consumer)
}

pub fn producer(settings: &KafkaSettings) -> Result<FutureProducer, KafkaError> {
    let mut config = ClientConfig::new();
    let config = settings.config(&mut config);
    let producer_settings = settings
        .producer
        .clone()
        .expect("Producer settings not specified");
    let acks = match producer_settings.acks {
        Acks::All => "all".to_string(),
        Acks::Number(n) => format!("{}", n),
    };
    let producer: FutureProducer = config
        .set("acks", acks)
        .set("retries", format!("{}", producer_settings.retries))
        // TODO: Figure out how to remove this setting
        .set("enable.ssl.certificate.verification", "false")
        .create()?;
    Ok(producer)
}
