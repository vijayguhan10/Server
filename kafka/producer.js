const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"] // replace with your Kafka broker
});

const producer = kafka.producer();

const runProducer = async () => {
  await producer.connect();
  console.log("Producer connected");

  // send 10 sample messages
  for (let i = 1; i <= 10; i++) {
    await producer.send({
      topic: "test-topic",
      messages: [
        { key: `key-${i}`, value: `Message number ${i}` }
      ],
    });
    console.log(`Sent: Message number ${i}`);
  }

  await producer.disconnect();
  console.log("Producer disconnected");
};

runProducer().catch(console.error);
