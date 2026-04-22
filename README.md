# 실시간 주식 거래 어플리케이션

## Goal

- 초당 수천건의 거래에 해당하는 주식거래 데이터를 사용자가 실시간 Tick 으로 볼 수 있음
- 실시간 거래 이상 내역을 확인 할 수 있음
- 지난 거래내역을 분/시간/일별로 조회 할 수 있음

## Architecture

### Overview

```mermaid
flowchart TD
    Source[Data Source] --Incoming--> PostgreSQL[PostgreSQL with TimescaleDB extension]
    App[Real-time Stock Application] --Fetch Data--> PostgreSQL
    Debezium --CDC--> PostgreSQL
    Debezium --producer--> Kafka[Apache Kafka]
    App --consumer--> Kafka
    Flink[Apache Flink] --consumer--> Kafka
    App --Table API--> Flink
    User <--Web Socket / SSE / RestAPI--> App
```

### Design Decisions

#### Why Event-Driven Architecture?

- 거래 데이터 생성, 저장, 분석, 알림이 각각 독립적으로 확장 가능하도록 구성
- 하나의 컴포넌트 장애가 전체 시스템에 미치는 영향을 최소화

#### Why Separate Real-time and Batch Processing?

- Real-time (WebSocket): 사용자가 즉시 거래 변화를 확인할 수 있도록
- Batch (REST API): 대용량 히스토리 데이터 조회 시 안정적인 성능 보장

#### Why CDC over Application-level Event Publishing?

- 데이터 정합성 보장: 트랜잭션 커밋과 동시에 이벤트 발행
- 기존 레거시 시스템 변경 최소화

## 성능 요구사항 (Performance Requirements)

### Production 환경 기준

| #   | 시나리오                           | HW 사양                       | 대상 메트릭                                 | 요구사항                                            |
| --- | ---------------------------------- | ----------------------------- | ------------------------------------------- | --------------------------------------------------- |
| 1   | **실시간 호가 전송 (WebSocket)**   | 4 CPU / 8GB RAM / 25Gbps NIC  | 1,000개 동시 연결, 초당 5,000건 거래 데이터 | End-to-End 지연시간 < 100ms (DB 커밋 → Client 수신) |
| 2   | **거래 데이터 삽입 (Bulk Insert)** | 8 CPU / 16GB RAM / SSD        | 배치당 10,000건 거래 데이터                 | 처리시간 < 500ms (asyncpg COPY 메서드 사용)         |
| 3   | **히스토리 조회 (REST API)**       | 4 CPU / 8GB RAM / TimescaleDB | 30일치 데이터 조회 (약 1,296,000건)         | 응답시간 < 2초 (TimescaleDB 파티셔닝 활용)          |

## Tech Stack

- `FastAPI` - 0.115.12
  - 비동기 처리로 수천 건의 동시 IO 연결을 효율적으로 처리
- `PostgreSQL with TimescaleDB extension` - 16.9
  - 시계열 데이터 압축과 파티셔닝으로 대용량 거래 데이터의 저장/조회 성능을 최적화
- `Debezium`: 3.0.0.Final
  - 데이터베이스 변경사항을 실시간으로 캐치하여 애플리케이션 로직 수정 없이 CDC를 구현
- `Apache Kafka` - 4.0.0
  - 초당 수천 건의 거래 데이터를 버퍼링하고 여러 컨슈머가 독립적으로 처리할 수 있는 확장성
- `Apache Flink` - Scala 2.12
  - 스트리밍 데이터에서 실시간 이상 거래 탐지를 위한 복잡한 윈도우 기반 집계 연산을 처리

## How It Works

### Data Pipeline

1. Data Source 에서 PostgreSQL 에 거래내역 데이터를 저장한다
   - Data Source 라고 지칭하는 거래 데이터는 서비스 레벨에서 구현하여 모킹 함
   - `POST http://<HOST>/api/v1/stock/generate`
2. 저장된 거래내역은 Change Data Capture 를 통해 Kafka 로 스트리밍된다
3. Flink 에서는 거래내역을 실시간으로 통계 및 연산을 수행하여 이상 거래 여부를 판단한다

## 구현 기술 상세

### TimescaleDB: 시계열 데이터베이스 최적화

- **Hypertable**: `stock_trades` 테이블을 `event_time` 기준으로 Hypertable로 변환하여 시계열 데이터에 최적화된 파티셔닝을 자동 적용합니다. 이를 통해 대규모 데이터셋에서도 빠른 데이터 삽입 및 범위 기반 조회(Time-based queries) 성능을 보장합니다.
- **고성능 삽입**: 데이터 생성 시 `asyncpg`의 `copy_records_to_table` 메서드를 사용하여 PostgreSQL의 `COPY` 명령을 실행합니다. 이는 `INSERT`를 반복하는 것보다 훨씬 빠른 속도로 대량의 데이터를 벌크 삽입할 수 있게 해주는 핵심 기능입니다.

### Debezium & Kafka: 변경 데이터 캡처(CDC) 및 이벤트 스트리밍

- **CDC 설정**: Debezium PostgreSQL 커넥터가 `stock_trades` 테이블의 변경 사항을 실시간으로 감지합니다. `publication.name`으로 `dbz_publication`을 사용하여 논리적 디코딩(Logical Decoding)을 통해 변경분을 스트림으로 변환합니다.
- **Kafka 토픽**: 감지된 모든 데이터 변경(INSERT, UPDATE, DELETE) 이벤트는 `stock.stock_trades`라는 Kafka 토픽으로 발행(Publish)됩니다. 이 토픽은 실시간 데이터 파이프라인의 중심 허브 역할을 합니다.
- **느슨한 결합**: 이 아키텍처를 통해 데이터베이스와 실시간 처리 시스템(FastAPI, Flink)이 분리됩니다. 데이터베이스는 데이터 저장에만 집중하고, 실시간 처리가 필요한 모든 애플리케이션은 Kafka 토픽을 구독(Subscribe)하여 독립적으로 확장 및 운영될 수 있습니다.

## API

### Tick

- `GET ws://<HOST>/api/v1/stock/real-time`
  - Description: 사용자의 실시간 거래내역(Tick) 을 요청 시 서비스에서 WebSocket 연결을 하여 Kafka 의 메시지를 전달한다
  - Features
    - 실시간 호가 시작
      - [ ] Client 가 WebSocket 연결을 통해 Ticker 이름과 Tick 주기를 Server 에 요청
      - [ ] Server 가 요청받은 Ticker 의 Kafka 토픽 구독 시작
      - [ ] Server 가 설정된 주기로 호가 데이터를 Client 에 전송
      - [ ] Client 가 실시간 호가 데이터를 수신 및 렌더링
    - 실시간 호가 변경
      - [ ] Server 가 기존 실시간 데이터 전송 중 새로운 Ticker 혹은 Tick 주기 변경 요청을 감지
      - [ ] Server 가 기존 Ticker 의 Kafka 구독 해제
      - [ ] Server 가 새로운 Ticker 의 Kafka 구독 시작
      - [ ] Server 가 갱신된 Tick 주기로 호가 데이터 전송 재개

### Anomaly

- `GET http://<HOST>/api/v1/stock/anomaly`
  - Description: 사용자의 거래 이상 거래 탐지 확인 요청 시 SSE 로 발생 내역을 전달한다
  - Features
    - [ ] Flink 이상 거래 결과를 Kafka 또는 조회 가능한 스트림으로 노출
    - [ ] 서비스가 이상 거래 이벤트를 subscribe 하여 SSE 형식으로 변환
    - [ ] 이벤트 타입, 발생 시각, ticker, 이상 탐지 근거를 포함한 응답 스키마 정의
    - [ ] 클라이언트 연결 종료 시 consumer 정리 및 재연결 처리
    - [ ] keep-alive 및 heartbeat 이벤트 처리
    - [ ] 이상 거래 없음 / 지연 / 처리 실패에 대한 예외 응답 정책 정의

### Trades

- `GET http://<HOST>/api/v1/stock`
  - Description: 지난 거래내역에 대한 조회 요청 시 RestAPI 로 제공한다
  - Features
    - [ ] duration, ticker, tradeType, marketCode 조건 조합 조회
    - [ ] 분/시간/일 단위 집계 조회 API 설계
    - [ ] 시작 시각, 종료 시각 기반 기간 조회 지원
    - [ ] 대용량 조회를 위한 pagination 또는 cursor 기반 응답 지원
    - [ ] count, filters, aggregate metadata 를 포함한 응답 형식 표준화
    - [ ] 잘못된 파라미터, 빈 결과, 최대 조회 범위 초과에 대한 검증 정책 정의

## 개발 항목

### 1. 실시간 Tick 스트리밍 완성

- [ ] Kafka consumer 를 애플리케이션 런타임 의존성으로 반영
- [ ] Debezium CDC 메시지 스키마 파싱 로직 구현
- [ ] WebSocket 연결별 ticker / tick 설정 상태 관리
- [ ] ticker 변경 시 기존 구독 해제 후 신규 구독 연결
- [ ] 수신 이벤트를 tick 주기 기준 candle/high-low 데이터로 집계
- [ ] 데이터 부재, 지연, consumer 오류에 대한 WebSocket 예외 처리

### 2. 이상 거래 SSE 완성

- [ ] Flink 출력 토픽 또는 결과 스트림 스키마 확정
- [ ] 이상 거래 이벤트 consumer 구현
- [ ] SSE event/data 포맷 표준화
- [ ] heartbeat, reconnect, graceful shutdown 처리
- [ ] 이상 거래 이벤트 필터링 및 직렬화

### 3. 히스토리 조회 고도화

- [ ] 현재 필터 조회 API를 range query 중심으로 확장
- [ ] 분/시간/일 aggregation 쿼리 추가
- [ ] pagination 또는 cursor 응답 도입
- [ ] 대량 데이터 조회 성능 검증
- [ ] 응답 모델 및 validation 정리

### 4. 운영 안정성 보강

- [ ] Kafka, PostgreSQL, Flink 설정을 `env.toml` 기반으로 통합
- [ ] 구조화된 로그와 장애 추적 포인트 추가
- [ ] 연결 종료, 재시도, backpressure 처리 정책 정리
- [ ] 통합 테스트 또는 최소한의 회귀 테스트 추가
- [ ] 성능 요구사항 기준 점검 시나리오 문서화
