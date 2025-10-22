const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'multi-topic-consumer',
  brokers: ['localhost:9092', 'localhost:9093', 'localhost:9094'],
});

const consumer = kafka.consumer({ groupId: 'multi-topic-group' });

const runConsumer = async () => {
  await consumer.connect();
  const topics = ['orders-topic', 'payments-topic', 'inventory-topic', 'emails-topic'];

  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: true });
  }

  console.log('Consumer connected');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`Topic: ${topic} | Partition: ${partition} | Value: ${message.value.toString()}`);
    },
  });
};

runConsumer().catch(console.error);
