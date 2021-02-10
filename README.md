# Kafka Listener

Spring Boot app processing messages implementing @KafkaListener and publishing messages using KafkaTemplate.
Messages are serialized as JSON or String. Spring EmbeddedKafka server is used in testing Producer and Consumer logic.

### Scope
Upon starting, the App publishes 1000 JSON messages to a topic which has a listener that saves message to in memory database. \
A second listener handles String messages from a file-source topic that maps message to a Transaction object to be saved to DB.
- Confluent Kafka can be downloaded from [here](https://www.confluent.io/download/) 
- [These steps](https://docs.confluent.io/platform/current/quickstart/ce-quickstart.html#ce-quickstart) setup the Confluent Kafka Community edition in local environment
- [Confluent File Source setup](https://docs.confluent.io/platform/current/connect/quickstart.html)
