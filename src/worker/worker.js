// =============================================================================
// src/worker/worker.js — Background Worker Service
// =============================================================================

require("dotenv").config();
const { connectToRabbitMQ, QUEUE_NAME } = require("../lib/rabbitmq");

const PREFETCH_COUNT = 8;

async function saveTicketToDatabase(payload) {
  const latencyMs = Math.floor(Math.random() * 10) + 5;
  await new Promise((resolve) => setTimeout(resolve, latencyMs));

  if (Math.random() < 0.05) {
    throw new Error("Simulated transient DB error (connection timeout)");
  }

  console.log(`[Worker] 💾 MOCK DB write: tracking_id=${payload.tracking_id} (${latencyMs}ms)`);
  return { id: "mock-uuid-" + Date.now(), ...payload, status: "OPEN" };
}

async function startWorker() {
  const { channel } = await connectToRabbitMQ();

  await channel.prefetch(PREFETCH_COUNT);
  console.log(`[Worker] 🔧 Prefetch set to ${PREFETCH_COUNT} messages.`);

  console.log(`[Worker] 👂 Listening on queue: "${QUEUE_NAME}"...`);

  channel.consume(QUEUE_NAME, async (msg) => {
    if (!msg) {
      console.warn("[Worker] Received null message — consumer may have been cancelled.");
      return;
    }

    let payload;

    try {
      payload = JSON.parse(msg.content.toString());
    } catch (parseErr) {
      console.error("[Worker] ❌ Failed to parse message JSON. Sending to DLQ.", parseErr.message);
      channel.nack(msg, false, false);
      return;
    }

    const processingStart = Date.now();
    console.log(`[Worker] 📥 Received: tracking_id=${payload.tracking_id} category=${payload.category}`);

    const queueLatencyMs = Date.now() - new Date(payload.published_at).getTime();
    if (queueLatencyMs > 5000) {
      console.warn(`[Worker] ⚠️  High queue latency: ${queueLatencyMs}ms for ${payload.tracking_id}. Consider scaling workers.`);
    }

    const MAX_RETRIES = 3;
    let lastError;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        const savedTicket = await saveTicketToDatabase(payload);

        const processingMs = Date.now() - processingStart;
        console.log(
          `[Worker] ✅ Ticket saved: tracking_id=${savedTicket.tracking_id} ` +
          `attempt=${attempt} processing_time=${processingMs}ms queue_latency=${queueLatencyMs}ms`
        );

        channel.ack(msg);
        return;

      } catch (err) {
        lastError = err;
        console.error(`[Worker] ⚠️  DB write attempt ${attempt}/${MAX_RETRIES} failed: ${err.message}`);

        if (attempt < MAX_RETRIES) {
          await new Promise((resolve) => setTimeout(resolve, 500 * attempt));
        }
      }
    }

    console.error(
      `[Worker] ❌ Exhausted ${MAX_RETRIES} retries for tracking_id=${payload?.tracking_id}. ` +
      "Requeueing message for future retry."
    );
    channel.nack(msg, false, true);

  }, { noAck: false });
}

startWorker().catch((err) => {
  console.error("[Worker] Fatal startup error:", err);
  process.exit(1);
});
