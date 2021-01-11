const { ServiceProvider } = require("@adonisjs/fold");

const AdonisKafka = require("../src");

class KafkaProvider extends ServiceProvider {
  /**
   * Register namespaces to the IoC container
   *
   * @method register
   *
   * @return {void}
   */
  register() {
    this.app.singleton("Kafka", async () => {
      const Config = this.app.use("Adonis/Src/Config");
      const Logger = this.app.use("Adonis/Src/Logger");
      const Helpers = this.app.use("Adonis/Src/Helpers");

      if (Helpers.isAceCommand()) return null;

      const kafka = new AdonisKafka(Config.get("kafka"), Logger, Helpers);
      await kafka.start();
      return kafka;
    });
  }

  /**
   * Attach context getter when all providers have
   * been registered
   *
   * @method boot
   *
   * @return {void}
   */
  boot() {
    //
  }
}

module.exports = KafkaProvider;
