/** @type {import('@adonisjs/framework/src/Env')} */
const Env = use("Env");

module.exports = {
  clientId: "kafka",
  brokers: Env.get("KAFKA_URLS", "").split(","),
  connectionTimeout: 20000,
  ssl: true,

  sasl: {
    mechanism: "plain",
    username: Env.get("KAFKA_USERNAME"),
    password: Env.get("KAFKA_PASSWORD"),
  },

  producer: {
    initialize: {},
  },

  consumer: {
    fromBeginning: false,
    partitionsConcurrently: 1,

    initialize: {
      groupId: Env.get("KAFKA_GROUP", "stalls-core"),
    },

    run: {
      autoCommit: false,
      partitionsConsumedConcurrently: 1,
    },
  },
};
