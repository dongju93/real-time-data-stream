import http from "k6/http";
import { check, sleep } from "k6";
import { Trend } from "k6/metrics";

const rawDuration = new Trend("raw_duration", true);
const aggregateDuration = new Trend("aggregate_duration", true);

export const options = {
  vus: Number(__ENV.VUS || 5),
  duration: __ENV.DURATION || "30s",
  thresholds: {
    checks: ["rate>0.99"],
    raw_duration: ["p(95)<2000"],
    aggregate_duration: ["p(95)<2000"],
  },
};

const baseUrl = (__ENV.BASE_URL || "http://localhost:8000").replace(/\/$/, "");
const startTime = __ENV.START_TIME;
const endTime = __ENV.END_TIME;

if (!startTime || !endTime) {
  throw new Error("START_TIME and END_TIME are required");
}

function stockUrl(granularity) {
  const query = [
    `startTime=${encodeURIComponent(startTime)}`,
    `endTime=${encodeURIComponent(endTime)}`,
    "limit=1000",
  ];
  if (granularity) {
    query.push(`granularity=${granularity}`);
  }
  return `${baseUrl}/api/v1/stock?${query.join("&")}`;
}

export default function () {
  const rawResponse = http.get(stockUrl(null), {
    tags: { endpoint: "raw" },
  });
  rawDuration.add(rawResponse.timings.duration);
  check(rawResponse, { "raw response is 200": (response) => response.status === 200 });

  const aggregateResponse = http.get(stockUrl("minute"), {
    tags: { endpoint: "aggregate" },
  });
  aggregateDuration.add(aggregateResponse.timings.duration);
  check(aggregateResponse, {
    "aggregate response is 200": (response) => response.status === 200,
  });

  sleep(0.1);
}
