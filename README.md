# 整数时间段房间预约服务

FastAPI + SQLite 实现。时间使用整数，区间采用半开区间 `[start_time, end_time)`，因此 `[a,b)` 与 `[b,c)` 不重叠。

## 关键语义

- 房间数固定为 `1..10`，默认 10；保留时长默认 5 个时钟单位。
- 申请成功先得到 `held` 限时保留，确认后成为 `confirmed`。
- 与当前 `held/confirmed` 容量冲突的申请进入 `waiting` 候补，`enqueue_seq` 持久化 FIFO 顺序。
- 保留过期、取消 `held/confirmed`、缩短已占用区间时，在**同一个 SQLite `BEGIN IMMEDIATE` 事务**中：
  1. 更新原预约并释放容量；
  2. 按候补入队顺序扫描一次；
  3. 仅当候选的完整区间可容纳时晋升为新的 `held`；
  4. 不拆分候补区间；当前不能完整容纳的候选不会阻止后面的独立可容纳候选。
- 时钟只能通过 `POST /clock/advance` 推进。若一次跨多个过期点，服务逐事件推进并返回过期/晋升事件。
- 所有 POST 都必须携带 `Idempotency-Key`：
  - 同键、同方法、同路径、同载荷重试：返回事务中保存的原始状态码和响应体；
  - 同键但载荷变化：返回 `409`。
- 所有状态、过期时刻、候补顺序和晋升结果都存放在 SQLite 中；服务重启不依赖内存状态。

## 启动

```bash
pip install -r requirements.txt
ROOM_COUNT=10 HOLD_TTL=5 DB_PATH=./reservations.db \
  uvicorn app.main:app --host 0.0.0.0 --port 8000
```

首次启动会创建数据库文件和表结构。

## API

### 申请

```bash
curl -X POST http://localhost:8000/reservations \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: apply-1' \
  -d '{"room_id":1,"start_time":10,"end_time":20}'
```

- `201`: 得到限时保留。
- `202`: 冲突，已进入候补。

### 确认保留

```bash
curl -X POST http://localhost:8000/reservations/1/confirm \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: confirm-1' \
  -d '{}'
```

### 取消

```bash
curl -X POST http://localhost:8000/reservations/1/cancel \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: cancel-1' \
  -d '{}'
```

取消 `held/confirmed` 会立即释放容量并扫描候补；取消 `waiting` 只移除候补项。

### 缩短已占用区间

```bash
curl -X POST http://localhost:8000/reservations/1/shorten \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: shorten-1' \
  -d '{"start_time":12,"end_time":20}'
```

新区间必须完整位于原区间内，并至少释放一个整数时间单位；该操作随后在同事务中扫描候补。

### 原子改期（换房/改时段）

```bash
curl -X POST http://localhost:8000/reservations/1/reschedule \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: reschedule-1' \
  -d '{
    "original_room_id": 1, "original_start_time": 10, "original_end_time": 20,
    "target_room_id": 2, "target_start_time": 30, "target_end_time": 40
  }'
```

- 仅 `held/confirmed` 预约可改期。原房间与原半开区间是并发校验依据：与当前状态不一致（已被另一请求移动、缩短、取消或确认）返回 `409`。
- 全部检查（按当前时钟判定到期、原状态/原区间、目标房间有效性与目标容量）与移动、候补晋升在**同一个 SQLite `BEGIN IMMEDIATE` 写事务**中完成；目标不可容纳、原状态已变化或请求已过期时，原预约与候补队列均不改变。
- 容量检查只排除预约自身，其余 `held/confirmed` 占用一律计入，半开区间相邻不冲突。
- 成功后一次性移动预约，再按现有 FIFO 规则扫描候补；只晋升真正释放出的旧容量（同房间交叠改期中仍被占用的部分不会释放，不产生重复释放）。
- `held` 保留**原到期时刻**，反复改期不能续期；`confirmed` 改期后仍为 `confirmed`。

### 推进受控时钟

```bash
curl -X POST http://localhost:8000/clock/advance \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: clock-5' \
  -d '{"target_time":5}'
```

### 查询

- `GET /health`
- `GET /reservations?status=waiting&room_id=1`
- `GET /reservations/{id}`

## 测试

```bash
python3 -m pytest -q
```

测试包含：

- 半开区间边界和多房间容量；
- 保留过期、FIFO 候补、完整区间晋升、不拆分；
- 缩短/取消释放容量后的同事务晋升；
- held/confirmed 原子改期：相邻区间、交叠改期不重复释放、到期边界、原区间并发校验、幂等重试与两个请求争用同一预约；
- 8 个并发冲突申请仅一个取得保留；
- 幂等重试返回原响应，载荷变化返回 `409`；
- 关闭并重新创建应用后时钟、保留、候补顺序和晋升结果保持不变；
- Python 参考模型与逐步推进的服务状态逐项核对。
