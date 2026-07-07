-- 현재 활성 연결 정보 조회
SELECT 
    pid,
    usename,
    application_name,
    client_addr,
    client_hostname,
    client_port,
    backend_start,
    query_start,
    state_change,
    state,
    query
FROM pg_stat_activity
WHERE state = 'active'
ORDER BY backend_start DESC;

-- 모든 연결 상태별 개수 확인
SELECT 
    state,
    COUNT(*) as connection_count
FROM pg_stat_activity
WHERE pid != pg_backend_pid()  -- 현재 세션 제외
GROUP BY state
ORDER BY connection_count DESC;

-- 데이터베이스별 연결 수 확인
SELECT 
    datname,
    COUNT(*) as connections,
    MAX(backend_start) as latest_connection
FROM pg_stat_activity
WHERE datname IS NOT NULL
GROUP BY datname
ORDER BY connections DESC;

-- 연결 제한 설정 확인
SELECT 
    name,
    setting,
    unit,
    context
FROM pg_settings 
WHERE name IN ('max_connections', 'superuser_reserved_connections');

-- 사용자별 연결 수 확인
SELECT 
    usename,
    COUNT(*) as connection_count,
    array_agg(DISTINCT state) as states
FROM pg_stat_activity
WHERE usename IS NOT NULL
GROUP BY usename
ORDER BY connection_count DESC;

-- 장시간 실행 중인 쿼리 확인 (1분 이상)
SELECT 
    pid,
    usename,
    client_addr,
    query_start,
    now() - query_start as duration,
    state,
    LEFT(query, 100) as query_preview
FROM pg_stat_activity
WHERE state != 'idle' 
    AND query_start < now() - interval '1 minute'
ORDER BY duration DESC;

-- Pool 관련 통계 (connection pooler 사용 시)
SELECT 
    application_name,
    COUNT(*) as pool_connections,
    array_agg(DISTINCT state) as connection_states
FROM pg_stat_activity
WHERE application_name LIKE '%pool%' OR application_name LIKE '%pgbouncer%'
GROUP BY application_name;

-- IP별 연결 상태 및 개수 확인
SELECT 
    client_addr,
    state,
    COUNT(*) as connection_count,
    array_agg(DISTINCT application_name) as applications
FROM pg_stat_activity
WHERE client_addr IS NOT NULL
GROUP BY client_addr, state
ORDER BY client_addr, connection_count DESC;

-- IP별 idle 상태 연결 (sleep 중인 연결) 상세 정보
SELECT 
    client_addr,
    COUNT(*) as idle_connections,
    MIN(state_change) as oldest_idle,
    MAX(state_change) as newest_idle,
    now() - MIN(state_change) as max_idle_duration
FROM pg_stat_activity
WHERE state = 'idle' 
    AND client_addr IS NOT NULL
GROUP BY client_addr
ORDER BY idle_connections DESC;

-- 전체 연결 풀 상태 요약 (IP별)
SELECT 
    client_addr,
    COUNT(*) as total_connections,
    COUNT(CASE WHEN state = 'idle' THEN 1 END) as idle_connections,
    COUNT(CASE WHEN state = 'active' THEN 1 END) as active_connections,
    COUNT(CASE WHEN state = 'idle in transaction' THEN 1 END) as idle_in_transaction
FROM pg_stat_activity
WHERE client_addr IS NOT NULL
GROUP BY client_addr
ORDER BY total_connections DESC;
