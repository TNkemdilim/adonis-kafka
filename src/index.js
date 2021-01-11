const { Kafka } = require("kafkajs");
const Consumer = require("./Consumer");
const Producer = require("./Producer");
const { parseConfig } = require("./utils/validation");

class AdonisKafka {
  constructor(config, logger, helper) {
    this.logger = logger;
    this.helpers = helper;
    this.connected = false;
    this.config = parseConfig(config);
  }

  async ensureConnected() {
    if (!this.connected) await this.connect();
  }

  async connect() {
    const kafka = new Kafka(this.config.shared);

    const appRoot = this.helper.appRoot();
    this.consumer = new Consumer(
      kafka,
      this.logger,
      this.config.consumer,
      appRoot
    );

    this.producer = new Producer(kafka, this.logger, this.config.producer);

    try {
      await Promise.all([this.consumer.start(), this.producer.start()]);
      this.logger.info("Successfully initialized Adonis Kafka");
      this.connected = true;
    } catch (err) {
      this.logger.error("Failed to initialize Adonis Kafka: ", err);
      this.connected = false;
    }
  }

  on(topic, callback) {
    this.ensureConnected().then((_) => this.consumer.on(topic, callback));
  }

  send(topic, data) {
    this.ensureConnected().then((_) => this.producer.send(topic, data));
  }

  onCommit(err, topicPartitions) {
    if (err) {
      this.logger.error("Commit error: ", err);
      throw new Error(err);
    }

    this.logger.info("commited", topicPartitions);
  }
}

module.exports = AdonisKafka;
