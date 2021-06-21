use crate::settings::KafkaSettings;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    producer::FutureProducer,
    ClientConfig,
};
use uuid::Uuid;

pub fn consumer(settings: &KafkaSettings) -> Result<StreamConsumer, KafkaError> {
    let mut config = ClientConfig::new();
    let config = settings.config(&mut config);
    let consumer_settings = settings
        .consumer
        .clone()
        .expect("Consumer settings not specified");
    let group_id = if consumer_settings.unique_id {
        format!(
            "{}_{}",
            consumer_settings.group_id,
            Uuid::new_v4().to_string(),
        )
    } else {
        consumer_settings.group_id
    };

    let consumer: StreamConsumer = config
        .set("group.id", &group_id)
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
    let producer: FutureProducer = config
        .set("acks", format!("{}", producer_settings.acks))
        .set("retries", format!("{}", producer_settings.retries))
        // TODO: Figure out how to remove this setting
        .set("enable.ssl.certificate.verification", "false")
        .create()?;
    Ok(producer)
}
