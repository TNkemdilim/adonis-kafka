const { Kafka } = require("kafkajs");

class Consumer {
  /**
   * Create Kafka consumer.
   * @param {Kafka} kafka
   * @param {object} logger
   * @param {object} config
   * @param {object} helper
   */
  constructor(kafka, logger, config, appDirectory) {
    this.topics = [];
    this.events = {};
    this.timeout = null;
    this.logger = logger;
    this.config = config;
    this.killContainer = false;
    this.appDirectory = appDirectory;
    this.consumer = kafka.consumer(this.config.initialize || {});
  }

  async start() {
    await this.consumer.connect();

    await this.consumer.run({
      ...(this.config.run || {}),
      eachMessage: this.execute.bind(this),
    });
  }

  async execute({ topic, partition, message }) {
    const result = JSON.parse(message.value.toString());

    const events = this.events[topic] || [];

    const promises = events.map((callback) => {
      return new Promise((resolve) => {
        callback(result, async () => {
          resolve();

          if (this.config.run && this.config.run.autoCommit) {
            return;
          }

          const offset = String(Number(message.offset) + 1);

          await this.consumer.commitOffsets([{ topic, partition, offset }]);
        });
      });
    });

    await Promise.all(promises);
  }

  async on(topic, callback) {
    let topicArray = topic;
    const callbackFunction = this.validateCallback(callback);

    if (typeof topic === "string") {
      topicArray = topic.split(",");
    }

    if (!callbackFunction) {
      throw new Error("We can't find your controller.");
    }

    topicArray.forEach(async (item) => {
      if (!item) return;

      const events = this.events[item] || [];
      events.push(callbackFunction);
      this.events[item] = events;
      this.topics.push(item);

      await this.consumer.subscribe({
        topic: item,
        fromBeginning: this.config.fromBeginning || true,
      });
    });
  }

  validateCallback(callback) {
    // In this case the service is a function
    if (typeof callback === "function") return callback;

    const [model, func] = callback.split(".");
    const Module = require(`${this.appDirectory}/app/Controllers/Kafka/${model}`);
    const controller = new Module();

    if (typeof controller[func] === "function") {
      return controller[func].bind(controller);
    }

    return null;
  }
}

module.exports = Consumer;
