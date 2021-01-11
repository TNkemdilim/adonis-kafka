const { Kafka } = require("kafkajs");

class Producer {
  /**
   * Create Kafka consumer.
   * @param {Kafka} kafka
   * @param {object} logger
   * @param {object} config
   */
  constructor(kafka, logger, config) {
    this.logger = logger;
    this.config = config;
    this.producer = kafka.producer(this.config.initialize || {});
  }

  async start() {
    await this.producer.connect(this.config.run || {});
  }

  async send(topic, data) {
    if (typeof data !== "object") {
      throw new Error("You need send a json object in data argument");
    }

    let messages = Array.isArray(data) ? data : [data];
    messages = messages.map((message) => {
      if (!message.value) {
        message = {
          value: JSON.stringify(message),
        };
      }

      if (typeof message.value !== "string") {
        message.value = JSON.stringify(message.value);
      }

      return message;
    });

    await this.producer.send({
      topic,
      messages,
    });

    this.logger.info("sent data to kafka.");
  }
}

module.exports = Producer;
