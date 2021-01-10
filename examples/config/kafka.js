/** @type {import('@adonisjs/framework/src/Env')} */
const Env = use("Env");

module.exports = {
  brokers: Env.get("KAFKA_URLS", null),
  connectionTimeout: 3000,
  requestTimeout: 60000,

  producer: {
    initialize: {},
  },

  consumer: {
    fromBeginning: Env.get("KAFKA_CONSUMER_FROM_BEGINNING", true),
    partitionsConcurrently: 1,

    initialize: {
      groupId: Env.get("KAFKA_GROUP", "kafka"),
    },

    run: {
      autoCommit: false,
      partitionsConsumedConcurrently: 1,
    },
  },
};
