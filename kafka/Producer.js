const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'multi-topic-producer',
  // Single-broker setup based on docker-compose
  brokers: ['localhost:9093'],
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
      const sendTime = Date.now();
      try {
        const result = await producer.send({
          topic,
          // acks: -1 // uncomment to require all ISR acks if needed
          messages: [
            { key: String(counter % 3), value: JSON.stringify(message) },
          ],
        });

        // KafkaJS returns metadata per topic/partition
        for (const r of result) {
          const partitions = r.partitions || r.partitionMetadata || [];
          for (const p of partitions) {
            const meta = {
              topic: r.topicName || topic,
              partition: p.partition,
              baseOffset: p.baseOffset,
              logAppendTime: p.logAppendTime,
              sentAt: new Date(sendTime).toISOString(),
              key: String(counter % 3),
              payload: message,
            };
            console.log('[Producer ACK]', meta);
          }
        }
      } catch (err) {
        console.error('[Producer ERROR]', { topic, err: err.message });
      }
    }
  }, 1000);
};

runProducer().catch(console.error);
