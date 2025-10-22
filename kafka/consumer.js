const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'order-consumer',
  brokers: ['localhost:9092', 'localhost:9093'],
});

const consumer = kafka.consumer({ groupId: 'order-group' });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'orders', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(
        `Received from partition ${partition}: ${message.key.toString()} => ${message.value.toString()}`
      );
    },
  });
};

run().catch(console.error);
