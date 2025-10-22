const { Kafka } = require('kafkajs');

// Kafka setup
const kafka = new Kafka({
  clientId: 'order-service',
  brokers: ['localhost:9092', 'localhost:9093'],
});

const producer = kafka.producer();

// Sample order data
const orders = [
  { orderId: 101, customer: 'Alice', amount: 250 },
  { orderId: 102, customer: 'Bob', amount: 120 },
  { orderId: 103, customer: 'Charlie', amount: 450 },
  { orderId: 104, customer: 'David', amount: 300 },
];

const run = async () => {
  await producer.connect();
  console.log('Producer connected!');

  let count = 0;

  setInterval(async () => {
    const order = orders[count % orders.length];
    const message = {
      key: `order-${order.orderId}`, // key ensures same partition for same orderId
      value: JSON.stringify(order),
    };

    await producer.send({
      topic: 'orders',
      messages: [message],
    });

    console.log('Sent:', message);
    count++;
  }, 2000); // send an order every 2 seconds
};

run().catch(console.error);
