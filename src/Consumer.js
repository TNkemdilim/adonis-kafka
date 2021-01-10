/* eslint-disable global-require */
/* eslint-disable import/no-dynamic-require */
const { Kafka } = require("kafkajs");

class Consumer {
  constructor(Logger, config, Helpers) {
    this.Logger = Logger;
    this.Helpers = Helpers;
    this.config = config;
    this.topics = [];
    this.events = {};
    this.killContainer = false;
    this.timeout = null;
    this.consumer = null;

    const { initialize: initializeConfig, run, ...kafkaConfig } = this.config;

    const kafka = new Kafka(kafkaConfig);
    this.consumer = kafka.consumer(initializeConfig || {});
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

  //

  async on(topic, callback) {
    const callbackFunction = this.validateCallback(callback);
    let topicArray = topic;

    if (typeof topic === "string") {
      topicArray = topic.split(",");
    }

    if (!callbackFunction) {
      throw new Error("We can'f found your controller");
    }

    topicArray.forEach(async (item) => {
      if (!item) {
        return;
      }
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
    if (typeof callback === "function") {
      return callback;
    }

    const splited = callback.split(".");

    const model = splited[0];
    const func = splited[1];

    const root = this.Helpers.appRoot();
    const route = `${root}/app/Controllers/Kafka/${model}`;

    const Module = require(route);
    const controller = new Module();

    if (typeof controller[func] === "function") {
      return controller[func].bind(controller);
    }

    return null;
  }
}

module.exports = Consumer;
