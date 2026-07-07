-- 1. TimescaleDB 확장 활성화
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 2. 테이블 생성
CREATE TABLE IF NOT EXISTS stock_trades (
   event_time TIMESTAMPTZ NOT NULL,
   event_id UUID NOT NULL,
   ticker VARCHAR(10) NOT NULL,
   price DECIMAL(12, 2) NOT NULL,
   volume INTEGER NOT NULL,
   trade_type VARCHAR(10) NOT NULL,
   trade_id UUID NOT NULL,
   market_code VARCHAR(10),
   currency_code CHAR(3) DEFAULT 'USD',
   PRIMARY KEY (event_time, event_id)
);

-- 코멘트 추가
COMMENT ON TABLE stock_trades IS '주식 거래 데이터';
COMMENT ON COLUMN stock_trades.event_time IS 'UTC';
COMMENT ON COLUMN stock_trades.volume IS '거래량';

-- 3. 하이퍼테이블 생성
SELECT
   create_hypertable (
      'stock_trades',
      'event_time',
      chunk_time_interval => INTERVAL '7 days', -- Chunks 별 기간
      if_not_exists => TRUE
   );

-- 데이터 압축 정책
ALTER TABLE stock_trades
SET
   (
      timescaledb.compress,
      timescaledb.compress_segmentby = 'ticker, market_code', -- Segment group
      timescaledb.compress_orderby = 'event_time DESC, volume DESC'
   );

-- 자동 압축
SELECT
   add_compression_policy ('stock_trades', INTERVAL '30 days');

-- 4. 인덱스
-- 거래 심볼별
CREATE INDEX idx_stock_trades_ticker_time ON stock_trades (ticker, event_time DESC);
-- 거래 유형별
CREATE INDEX idx_stock_trades_type_time ON stock_trades (trade_type, event_time DESC);
-- 거래량 기반
CREATE INDEX idx_stock_trades_volume_time ON stock_trades (volume DESC, event_time DESC)
WHERE
   volume > 10000;
-- 시장별
CREATE INDEX idx_stock_trades_market_time ON stock_trades (market_code, event_time DESC);
-- 고유 거래 ID 보장
CREATE UNIQUE INDEX idx_stock_trades_trade_id_unique ON stock_trades (trade_id, event_time);

-- 여기까지 실행

-- 5. 데이터 보존 정책
-- 2년 이후 데이터 자동 삭제 정책
SELECT
   add_retention_policy ('stock_trades', INTERVAL '2 years');

-- 정책 확인
SELECT
   *
FROM
   timescaledb_information.policy_stats
WHERE
   hypertable_name = 'stock_trades';

-- 6. 연속 집계
-- 1분 단위 OHLCV 데이터 연속 집계
CREATE MATERIALIZED VIEW stock_trades_1min
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

-- 연속 집계 새로고침 정책
SELECT
   add_continuous_aggregate_policy (
      'stock_trades_1min',
      start_offset => INTERVAL '1 hour',
      end_offset => INTERVAL '1 minute',
      schedule_interval => INTERVAL '1 minute'
   );

-- 7. 테이블 설정 최적화
-- 테이블 레벨 최적화 설정
ALTER TABLE stock_trades
SET
   (
      fillfactor = 90, -- 삽입 성능 최적화
      autovacuum_vacuum_scale_factor = 0.1,
      autovacuum_analyze_scale_factor = 0.05
   );

-- 통계 정보 수집 최적화
ALTER TABLE stock_trades
ALTER COLUMN ticker
SET
   STATISTICS 1000;

ALTER TABLE stock_trades
ALTER COLUMN price
SET
   STATISTICS 1000;

ALTER TABLE stock_trades
ALTER COLUMN volume
SET
   STATISTICS 1000;

-- 8. 성능 모니터링
-- 하이퍼테이블 상태 확인
SELECT
   *
FROM
   timescaledb_information.hypertables
WHERE
   hypertable_name = 'stock_trades';

-- 청크 정보 확인
SELECT
   chunk_name,
   range_start,
   range_end,
   is_compressed
FROM
   timescaledb_information.chunks
WHERE
   hypertable_name = 'stock_trades'
ORDER BY
   range_start DESC
LIMIT
   10;

-- 압축률 확인
SELECT
   pg_size_pretty(before_compression_total_bytes) AS before_compression,
   pg_size_pretty(after_compression_total_bytes) AS after_compression,
   ROUND(
      100 - (
         after_compression_total_bytes::numeric / before_compression_total_bytes::numeric * 100
      ),
      2
   ) AS compression_ratio
FROM
   timescaledb_information.compression_stats
WHERE
   hypertable_name = 'stock_trades';
