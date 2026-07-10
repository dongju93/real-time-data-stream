\set ON_ERROR_STOP on
\timing on

\echo '=== Seed coverage ==='
SELECT
   COUNT(*) AS rows_in_range,
   MIN(event_time) AS first_event_time,
   MAX(event_time) AS last_event_time
FROM
   stock_trades
WHERE
   event_time BETWEEN :'start_time'::timestamptz AND :'end_time'::timestamptz;

ANALYZE stock_trades;

\echo '=== Hypertable ==='
SELECT
   hypertable_schema,
   hypertable_name,
   num_dimensions,
   num_chunks,
   compression_enabled
FROM
   timescaledb_information.hypertables
WHERE
   hypertable_name = 'stock_trades';

\echo '=== Chunks ==='
SELECT
   chunk_schema,
   chunk_name,
   range_start,
   range_end,
   is_compressed
FROM
   timescaledb_information.chunks
WHERE
   hypertable_name = 'stock_trades'
   AND range_end >= :'start_time'::timestamptz
   AND range_start <= :'end_time'::timestamptz
ORDER BY
   range_start;

\echo '=== Indexes ==='
SELECT
   indexname,
   indexdef
FROM
   pg_indexes
WHERE
   schemaname = 'public'
   AND tablename = 'stock_trades'
ORDER BY
   indexname;

\echo '=== Raw 30-day range page ==='
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
   *
FROM
   stock_trades
WHERE
   event_time BETWEEN :'start_time'::timestamptz AND :'end_time'::timestamptz
ORDER BY
   event_time DESC,
   event_id DESC
LIMIT
   1001;

\echo '=== Minute aggregation without continuous aggregate ==='
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
   *
FROM
   (
      SELECT
         time_bucket ('1 minute', event_time) AS event_time,
         ticker,
         FIRST (price, event_time) AS open,
         MAX(price) AS high,
         MIN(price) AS low,
         LAST (price, event_time) AS close,
         SUM(volume) AS volume,
         COUNT(*) AS trade_count
      FROM
         stock_trades
      WHERE
         event_time BETWEEN :'start_time'::timestamptz AND :'end_time'::timestamptz
      GROUP BY
         time_bucket ('1 minute', event_time),
         ticker
   ) AS aggregated_trades
ORDER BY
   event_time DESC,
   ticker DESC
LIMIT
   1001;

\echo '=== Refresh continuous aggregate for the seed range ==='
CREATE MATERIALIZED VIEW IF NOT EXISTS stock_trades_1min
WITH
   (timescaledb.continuous) AS
SELECT
   time_bucket ('1 minute', event_time) AS bucket,
   ticker,
   market_code,
   FIRST (price, event_time) AS open_price,
   MAX(price) AS high_price,
   MIN(price) AS low_price,
   LAST (price, event_time) AS close_price,
   SUM(volume) AS total_volume,
   COUNT(*) AS trade_count
FROM
   stock_trades
GROUP BY
   bucket,
   ticker,
   market_code
WITH
   NO DATA;

CALL refresh_continuous_aggregate (
   'stock_trades_1min',
   :'start_time'::timestamptz,
   :'end_time'::timestamptz
);

\echo '=== Minute aggregation with continuous aggregate ==='
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
   bucket AS event_time,
   ticker,
   open_price AS open,
   high_price AS high,
   low_price AS low,
   close_price AS close,
   total_volume AS volume,
   trade_count
FROM
   stock_trades_1min
WHERE
   bucket BETWEEN :'start_time'::timestamptz AND :'end_time'::timestamptz
ORDER BY
   bucket DESC,
   ticker DESC
LIMIT
   1001;
