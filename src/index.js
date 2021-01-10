const Consumer = require("./Consumer");
const Producer = require("./Producer");

class KafkaConsumer {
  constructor(config, Logger, Helpers) {
    this.config = config;
    this.Logger = Logger;
    this.Helpers = Helpers;

    this.start();
  }

  validateConfig() {
    const { consumer, urls } = this.config;

    if (
      consumer == null ||
      consumer.initialize == null ||
      consumer.initialize.groupId == null ||
      consumer.initialize.groupId === ""
    ) {
      throw new Error("You need define a group");
    }

    if (!urls || typeof urls != "string") {
      throw new Error("You need define a kafka url");
    }
  }

  start() {
    this.validateConfig();

    // Initialize consumer & producter
    const { producer, consumer, ...generalConfig } = this.config;
    const consumerConfig = { ...generalConfig, ...consumer };
    const producerConfig = { ...generalConfig, ...producer };

    this.consumer = new Consumer(this.Logger, consumerConfig, this.Helpers);
    this.producer = new Producer(this.Logger, producerConfig, this.Helpers);

    this.consumer.start();
    this.producer.start();
  }

  on(topic, callback) {
    this.consumer.on(topic, callback);
  }

  send(topic, data) {
    this.producer.send(topic, data);
  }

  onCommit(err, topicPartitions) {
    if (err) {
      // There was an error committing
      this.Logger.error("There was an error commiting", err);
      throw new Error(err);
    }

    // Commit went through. Let's log the topic partitions
    this.Logger.info("commited", topicPartitions);
  }
}

module.exports = KafkaConsumer;
