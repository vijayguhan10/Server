const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "banking-app",
  brokers: ["localhost:9092", "localhost:9093", "localhost:9094"]
});

const consumer = kafka.consumer({ groupId: "banking-event-aggregator" });

const runAggregator = async () => {
  await consumer.connect();
  console.log("Event Aggregator connected to all 3 brokers");

  // Subscribe to all three topics
  await consumer.subscribe({ topics: ["mouse-events", "keyboard-events", "textinput-events"], fromBeginning: false });

  const eventStats = {
    mouse: 0,
    keyboard: 0,
    textinput: 0
  };

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      
      // Track events by type
      if (topic === "mouse-events") {
        eventStats.mouse++;
        console.log(`[MOUSE] Partition ${partition}: x=${data.x}, y=${data.y} | Total: ${eventStats.mouse}`);
      } else if (topic === "keyboard-events") {
        eventStats.keyboard++;
        console.log(`[KEYBOARD] Partition ${partition}: key=${data.key} | Total: ${eventStats.keyboard}`);
      } else if (topic === "textinput-events") {
        eventStats.textinput++;
        console.log(`[TEXTINPUT] Partition ${partition}: text="${data.text}" | Total: ${eventStats.textinput}`);
      }

      // Log aggregate stats every 10 events
      const totalEvents = eventStats.mouse + eventStats.keyboard + eventStats.textinput;
      if (totalEvents % 10 === 0) {
        console.log("\n=== EVENT STATISTICS ===");
        console.log(`Mouse Events: ${eventStats.mouse}`);
        console.log(`Keyboard Events: ${eventStats.keyboard}`);
        console.log(`Text Input Events: ${eventStats.textinput}`);
        console.log(`Total Events: ${totalEvents}`);
        console.log("========================\n");
      }
    },
  });
};

runAggregator().catch(console.error);

// Graceful shutdown
process.on('SIGINT', async () => {
  await consumer.disconnect();
  console.log("Event Aggregator disconnected");
  process.exit(0);
});