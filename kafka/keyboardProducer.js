const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "banking-app",
  brokers: ["localhost:9092", "localhost:9093", "localhost:9094"]
});

const producer = kafka.producer();

const simulateKeyboardInputs = async () => {
  await producer.connect();
  console.log("Keyboard Producer connected to all 3 brokers");

  let messageCount = 0;
  const keys = ['a', 'b', 'c', 'd', 'e', 'Enter', 'Backspace', 'Tab'];
  
  // Simulate keyboard inputs every second
  setInterval(async () => {
    const keyData = {
      timestamp: new Date().toISOString(),
      key: keys[Math.floor(Math.random() * keys.length)],
      eventType: "keypress",
      sessionId: "session-123",
      userId: "user-456"
    };

    try {
      await producer.send({
        topic: "keyboard-events",
        messages: [
          {
            key: `keyboard-${messageCount++}`,
            value: JSON.stringify(keyData),
            partition: messageCount % 3
          }
        ],
      });
      console.log(`Sent keyboard event: key=${keyData.key}`);
    } catch (error) {
      console.error("Error sending keyboard event:", error);
    }
  }, 1000); // Every 1 second
};

simulateKeyboardInputs().catch(console.error);

// Graceful shutdown
process.on('SIGINT', async () => {
  await producer.disconnect();
  console.log("Keyboard Producer disconnected");
  process.exit(0);
});