const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "banking-app",
  brokers: ["localhost:9092", "localhost:9093", "localhost:9094"]
});

const producer = kafka.producer();

const simulateTextInputs = async () => {
  await producer.connect();
  console.log("Text Input Producer connected to all 3 brokers");

  let messageCount = 0;
  const sampleTexts = [
    "Account balance",
    "Transfer funds",
    "Payment history",
    "Customer support",
    "Login credentials"
  ];
  
  // Simulate text inputs every second
  setInterval(async () => {
    const textData = {
      timestamp: new Date().toISOString(),
      text: sampleTexts[Math.floor(Math.random() * sampleTexts.length)],
      fieldName: "searchBox",
      eventType: "textinput",
      sessionId: "session-123",
      userId: "user-456"
    };

    try {
      await producer.send({
        topic: "textinput-events",
        messages: [
          {
            key: `textinput-${messageCount++}`,
            value: JSON.stringify(textData),
            partition: messageCount % 3
          }
        ],
      });
      console.log(`Sent text input event: text="${textData.text}"`);
    } catch (error) {
      console.error("Error sending text input event:", error);
    }
  }, 1000); // Every 1 second
};

simulateTextInputs().catch(console.error);

// Graceful shutdown
process.on('SIGINT', async () => {
  await producer.disconnect();
  console.log("Text Input Producer disconnected");
  process.exit(0);
});