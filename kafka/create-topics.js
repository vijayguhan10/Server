const { Kafka } = require('kafkajs');

//
const kafka = new Kafka({
  clientId: 'admin',
  brokers: ['localhost:9092'],
});

(async () => {
  const admin = kafka.admin();
  await admin.connect();

  const topics = [
    'orders-topic',
    'payments-topic',
    'inventory-topic',
    'emails-topic',
  ];

  await admin.createTopics({
    topics: topics.map((topic) => ({
      topic,
      numPartitions: 3, // multiple partitions allowed with single broker
      replicationFactor: 1,
    })),
    waitForLeaders: true,
  });

  console.log('Topics created/ensured with replicationFactor=1');
  await admin.disconnect();
  process.exit(0);
})().catch((err) => {
  console.error('Failed to create topics:', err);
  process.exit(1);
});
