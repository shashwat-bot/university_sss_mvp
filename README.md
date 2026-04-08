# university-sss-mvp
University Student Services System — Async MVP Backend
# University Student Services System (MVP)

An async-first backend for handling student service requests with sub-5ms response times.

## Architecture

```
Student Browser
    ↓ POST /tickets
API Server (Express) → RabbitMQ → Worker → PostgreSQL
    ↓ ~2-5ms response
```

## Local Development

### Prerequisites
- Docker & Docker Compose
- Node.js 18+
- npm

### Setup

```bash
# 1. Clone
git clone https://github.com/shashwat-bot/university-sss-mvp.git
cd university-sss-mvp

# 2. Start infrastructure
docker-compose up -d

# 3. Install dependencies
npm install

# 4. Generate Prisma client
npm run db:generate

# 5. Run migrations
npm run db:migrate

# 6. Start API (Terminal 1)
npm run api

# 7. Start Worker (Terminal 2)
npm run worker
```

### Test the API

```bash
curl -X POST http://localhost:3000/tickets \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: unique-key-123" \
  -d '{"student_id":"STU-12345","category":"FEE_REFUND","description":"I was charged twice for tuition"}'
```

### RabbitMQ Management UI

Visit: http://localhost:15672

- Username: sss_rabbit
- Password: sss_rabbit_pw

### Cleanup

```bash
docker-compose down -v
```

## Key Features

- Sub-5ms API response times
- Handles 5x traffic spikes via message queue
- Durable message queue (survives restarts)
- At-least-once delivery guarantee
- Independent API and worker scaling

## Architecture Details

- `src/api/server.js` - API ingestion logic
- `src/worker/worker.js` - Async DB writer
- `src/lib/rabbitmq.js` - Connection & queue management
- `prisma/schema.prisma` - Database schema