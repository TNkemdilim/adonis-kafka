/**
 * Parse configuration.
 *
 * @param {object} config
 */
const parseConfig = ({ consumer, producers, ...shared }) => {
  if (
    consumer == null ||
    consumer.initialize == null ||
    consumer.initialize.groupId == null ||
    consumer.initialize.groupId === ""
  ) {
    throw new Error("You need define a group");
  }

  const { brokers } = shared;
  if (!brokers || !Array.isArray(brokers)) {
    throw new Error("You need to define a kafka broker.");
  }

  return { consumer, producer, shared };
};

module.exports = { parseConfig };
