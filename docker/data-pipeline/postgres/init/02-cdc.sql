-- 1. 복제용 Role 생성
CREATE ROLE <username> WITH REPLICATION LOGIN PASSWORD '<password>';

-- 2. 권한 부여
GRANT CONNECT ON DATABASE stock TO <username>;
GRANT USAGE ON SCHEMA public TO <username>;

-- 4. publication 생성, chunk 기반으로 모든 테이블 변경사항 추적 필요
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
ALTER PUBLICATION dbz_publication OWNER TO <username>;

-- 5. 복제 슬롯 생성, WAL 변경사항 추적
SELECT pg_create_logical_replication_slot('stock_trades_slot', 'pgoutput');