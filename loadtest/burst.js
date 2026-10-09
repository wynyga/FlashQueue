// A flash-sale burst: virtual users ramp up fast, hold, then leave. This is a synthetic load test, not real
// visitors. Run it with:  docker compose run --rm k6
//
// Environment (all optional):
//   VUS    peak virtual users              (default 200)
//   STOCK  units for sale                  (default 500)
//   HOLD   how long to stay at the peak    (default 30s)
import http from "k6/http";
import { check, sleep } from "k6";

const BASE = __ENV.BASE_URL || "http://localhost:8081";
const ADMIN = __ENV.ADMIN_KEY || "dev-admin";
const VUS = Number(__ENV.VUS || 200);

export const options = {
  scenarios: {
    burst: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "10s", target: VUS }, // the rush
        { duration: __ENV.HOLD || "30s", target: VUS }, // the sale is on
        { duration: "5s", target: 0 }, // everyone leaves
      ],
      gracefulRampDown: "5s",
    },
  },
  thresholds: {
    // Fail the run (non-zero exit) if the API stops answering quickly or starts erroring.
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<1000"],
  },
};

// Start every run from a clean slate: full stock, no old orders, empty queues.
export function setup() {
  const res = http.post(`${BASE}/admin/reset?stock=${__ENV.STOCK || 500}`, null, {
    headers: { "X-Admin-Key": ADMIN },
  });
  check(res, { "reset worked": (r) => r.status === 200 });
}

export default function () {
  const res = http.post(`${BASE}/orders`, JSON.stringify({ user: `vu${__VU}` }), {
    headers: {
      "Content-Type": "application/json",
      // A fresh key per order, like a real buyer's first attempt.
      "Idempotency-Key": `${__VU}-${__ITER}-${Date.now()}`,
    },
    tags: { name: "POST /orders" },
  });
  check(res, { "accepted (202)": (r) => r.status === 202 });
  sleep(Math.random() * 0.4); // a buyer does not hammer the button with zero pause
}

// The queue is usually still draining when the burst ends, so this shows where things stood at that moment.
// Watch the rest in Grafana, or run `curl localhost:8081/stats` until "pending" reaches 0.
export function teardown() {
  const stats = http.get(`${BASE}/stats`).json();
  console.log(`when the burst ended: ${JSON.stringify(stats)}`);
}
