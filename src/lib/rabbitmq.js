// =============================================================================
// src/lib/rabbitmq.js — Shared RabbitMQ Connection & Channel Factory
// =============================================================================

const amqp = require("amqplib");

const QUEUE_NAME = "ticket_processing_queue";

async function connectToRabbitMQ(retries = 10) {
  const url = process.env.RABBITMQ_URL || "amqp://sss_rabbit:sss_rabbit_pw@localhost:5672";

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      console.log(`[RabbitMQ] Connection attempt ${attempt}/${retries}...`);
      const connection = await amqp.connect(url);
      const channel = await connection.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });

      console.log(`[RabbitMQ] ✅ Connected and queue "${QUEUE_NAME}" asserted.`);

      process.on("SIGTERM", async () => {
        console.log("[RabbitMQ] SIGTERM received. Closing channel and connection...");
        await channel.close();
        await connection.close();
      });

      return { connection, channel };

    } catch (err) {
      console.error(`[RabbitMQ] Attempt ${attempt} failed: ${err.message}`);
      if (attempt === retries) {
        throw new Error(`[RabbitMQ] Could not connect after ${retries} attempts. Aborting.`);
      }
      const backoffMs = Math.min(1000 * 2 ** attempt, 1000 * 60);
      console.log(`[RabbitMQ] Retrying in ${backoffMs / 1000}s...`);
      await new Promise((resolve) => setTimeout(resolve, backoffMs));
    }
  }
}

module.exports = { connectToRabbitMQ, QUEUE_NAME };