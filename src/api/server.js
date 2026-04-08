// =============================================================================
// src/api/server.js — Ingestion API (Express)
// =============================================================================

require("dotenv").config();
const express = require("express");
const crypto = require("crypto");
const { connectToRabbitMQ, QUEUE_NAME } = require("../lib/rabbitmq");

const app = express();
app.use(express.json());

const idempotencyStore = new Map();

const CATEGORY_PREFIX = {
  FEE_REFUND: "REFUND",
  COURSE_CHANGE: "COURSE",
};

function generateTrackingId(category) {
  const prefix = CATEGORY_PREFIX[category];
  const suffix = crypto.randomInt(1000, 9999);
  return `${prefix}-${suffix}`;
}

const VALID_CATEGORIES = ["FEE_REFUND", "COURSE_CHANGE"];

function validateTicketPayload(body) {
  const errors = [];
  if (!body.student_id || typeof body.student_id !== "string") {
    errors.push("student_id is required and must be a string.");
  }
  if (!body.category || !VALID_CATEGORIES.includes(body.category)) {
    errors.push(`category must be one of: ${VALID_CATEGORIES.join(", ")}.`);
  }
  if (!body.description || typeof body.description !== "string" || body.description.trim().length < 10) {
    errors.push("description is required and must be at least 10 characters.");
  }
  return errors;
}

app.post("/tickets", async (req, res) => {
  const validationErrors = validateTicketPayload(req.body);
  if (validationErrors.length > 0) {
    return res.status(400).json({ success: false, errors: validationErrors });
  }

  const { student_id, category, description } = req.body;

  const clientKey = req.headers["idempotency-key"];
  const idempotencyKey = clientKey
    ? clientKey
    : crypto
        .createHash("sha256")
        .update(`${student_id}:${category}:${description.trim()}:${new Date().toDateString()}`)
        .digest("hex");

  if (idempotencyStore.has(idempotencyKey)) {
    const existingTrackingId = idempotencyStore.get(idempotencyKey);
    console.log(`[API] Duplicate detected for key ${idempotencyKey}. Returning existing: ${existingTrackingId}`);

    return res.status(200).json({
      success: true,
      duplicate: true,
      tracking_id: existingTrackingId,
      message: "A ticket with this request was already submitted. Here is your existing Tracking ID.",
    });
  }

  const trackingId = generateTrackingId(category);

  const queuePayload = {
    tracking_id: trackingId,
    idempotency_key: idempotencyKey,
    student_id,
    category,
    description: description.trim(),
    published_at: new Date().toISOString(),
  };

  try {
    app.locals.mqChannel.sendToQueue(
      QUEUE_NAME,
      Buffer.from(JSON.stringify(queuePayload)),
      { persistent: true }
    );
  } catch (err) {
    console.error("[API] ❌ Failed to publish to RabbitMQ:", err.message);
    return res.status(503).json({
      success: false,
      message: "Service temporarily unavailable. Please retry in a few moments.",
    });
  }

  idempotencyStore.set(idempotencyKey, trackingId);

  console.log(`[API] ✅ Ticket published. tracking_id=${trackingId} student=${student_id} category=${category}`);

  return res.status(202).json({
    success: true,
    tracking_id: trackingId,
    message: "Your request has been received and queued for processing.",
    estimated_processing: "Your ticket status will update within a few minutes.",
    status_check_url: `/tickets/${trackingId}`,
  });
});

app.get("/tickets/:tracking_id", async (req, res) => {
  res.status(200).json({
    message: "Status check endpoint — wire up Prisma query here.",
    tracking_id: req.params.tracking_id,
  });
});

app.get("/health", (req, res) => res.json({ status: "ok", service: "ingestion-api" }));

async function startServer() {
  const { channel } = await connectToRabbitMQ();
  app.locals.mqChannel = channel;

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => {
    console.log(`[API] 🚀 Ingestion API listening on port ${PORT}`);
    console.log(`[API] RabbitMQ Management UI → http://localhost:15672`);
  });
}

startServer().catch((err) => {
  console.error("[API] Fatal startup error:", err);
  process.exit(1);
});
