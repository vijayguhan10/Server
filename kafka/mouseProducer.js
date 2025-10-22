const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "banking-app",
  brokers: ["localhost:9092", "localhost:9093", "localhost:9094"]
});

const producer = kafka.producer();

const simulateMouseMovements = async () => {
  await producer.connect();
  console.log("Mouse Producer connected to all 3 brokers");

  let messageCount = 0;
  
  // Simulate mouse movements every second
  setInterval(async () => {
    const mouseData = {
      timestamp: new Date().toISOString(),
      x: Math.floor(Math.random() * 1920),
      y: Math.floor(Math.random() * 1080),
      eventType: "mousemove",
      sessionId: "session-123",
      userId: "user-456"
    };

    try {
      await producer.send({
        topic: "mouse-events",
        messages: [
          {
            key: `mouse-${messageCount++}`,
            value: JSON.stringify(mouseData),
            partition: messageCount % 3 // Distribute across 3 partitions
          }
        ],
      });
      console.log(`Sent mouse event: x=${mouseData.x}, y=${mouseData.y}`);
    } catch (error) {
      console.error("Error sending mouse event:", error);
    }
  }, 1000); // Every 1 second
};

simulateMouseMovements().catch(console.error);

// Graceful shutdown
process.on('SIGINT', async () => {
  await producer.disconnect();
  console.log("Mouse Producer disconnected");
  process.exit(0);
});