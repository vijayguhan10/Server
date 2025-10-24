const { Kafka } = require('kafkajs');
const fs = require('fs/promises');
const path = require('path');

const kafka = new Kafka({
  clientId: 'multi-topic-consumer',
  // Single-broker setup based on docker-compose
  brokers: ['localhost:9093'],
});

const consumer = kafka.consumer({ groupId: 'multi-topic-group' });

const runConsumer = async () => {
  // Ensure logs directory exists
  const logsDir = path.join(__dirname, 'logs');
  await fs.mkdir(logsDir, { recursive: true });

  await consumer.connect();
  const topics = ['orders-topic', 'payments-topic', 'inventory-topic', 'emails-topic'];

  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: true });
  }

  console.log('Consumer connected');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const timestampMs = Number(message.timestamp || Date.now());
      const record = {
        topic,
        partition,
        offset: message.offset,
        timestamp: new Date(timestampMs).toISOString(),
        key: message.key ? message.key.toString() : null,
        value: (() => {
          try {
            return JSON.parse(message.value.toString());
          } catch {
            return message.value?.toString();
          }
        })(),
      };

      // Console log for visibility
      console.log('[Consumed]', record);

      // Persist as JSONL per-topic
      const logFile = path.join(logsDir, `consumed-${topic}.log`);
      await fs.appendFile(logFile, JSON.stringify(record) + '\n', 'utf8');
    },
  });
};

runConsumer().catch(console.error);
