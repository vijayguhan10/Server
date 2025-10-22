const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'multi-topic-producer',
  brokers: ['localhost:9092', 'localhost:9093', 'localhost:9094'],
});

const producer = kafka.producer();

const runProducer = async () => {
  await producer.connect();
  console.log('Producer connected');

  let counter = 0;
  const topics = ['orders-topic', 'payments-topic', 'inventory-topic', 'emails-topic'];

  setInterval(async () => {
    for (const topic of topics) {
      const message = { id: counter++, value: Math.random() };
      await producer.send({
        topic,
        messages: [
          { key: String(counter % 3), value: JSON.stringify(message) },
        ],
      });
      console.log(`Sent to ${topic}:`, message);
    }
  }, 1000);
};

runProducer().catch(console.error);
