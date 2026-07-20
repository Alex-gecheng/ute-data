# 磨床数据采集与 API 服务

整合 **数据库查询（SCADA / MES）** 与 **OPC UA 实时机床状态采集** 的统一后端服务。

------

## 架构

```
Flask (Waitress, 32 threads)
 ├── 数据库 API（SCADA / MES，通过 SSH 隧道）
 │    ├── /api/device_status           设备运行状态
 │    ├── /api/process_data            工艺数据
 │    ├── /api/efficiency_data         效率数据
 │    ├── /api/home_online_inspection  首页在线检验
 │    ├── /api/detailed_online_inspection  详细页在线检验
 │    ├── /api/home_inspection         MES 首巡检
 │    └── /api/details_inspection      详情页首巡检（模拟）
 │
 └── OPC UA 实时采集（每机床 1 个 Subscription）
      ├── /api/machine/status          单机床加工状态
      ├── /api/machines/status         批量机床加工状态
      └── /api/machines/count          当前机床连接数

OPC UA 连接模型：
  Session
   └── Subscription × 1
         ├── MonitoredItem (aDbd[420])  工件计数
         ├── MonitoredItem (aDbw[428])  加工总时间
         ├── MonitoredItem (aDbw[430])  快进计时
         ├── MonitoredItem (aDbw[432])  快趋计时
         ├── MonitoredItem (aDbw[434])  粗磨1计时
         ├── MonitoredItem (aDbw[436])  粗磨2计时
         ├── MonitoredItem (aDbw[438])  精磨计时
         ├── MonitoredItem (aDbw[440])  光磨计时
         ├── MonitoredItem (aDbw[442])  退刀计时
         ├── MonitoredItem (aDbw[820])  生产状态（Bit0:生产 Bit1:空运行 Bit2:调整 Bit3:故障）
         └── MonitoredItem (aDbw[822])  等待状态（Bit0:等待缺料 Bit1:NC暂停）
```

------

## 启动

```bash
pip install -r requirements.txt
python app.py
```

服务地址：`http://0.0.0.0:5000`

### 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `OPCUA_USERNAME` | `OpcUaClient` | OPC UA 登录用户名 |
| `OPCUA_PASSWORD` | `OpcUaClient` | OPC UA 登录密码 |
| `OPCUA_PORT` | `4840` | OPC UA 服务端口 |
| `MAX_MACHINES` | `50` | 最大机床连接数 |
| `CORS_MODE` | `private` | 跨域模式：`private`（仅局域网）/ `all`（全部） |

------

## 通用说明

- **请求方式**：GET / POST（以各接口说明为准）
- **数据格式**：
  - 请求：`application/json`
  - 响应：`application/json`

- **通用返回字段说明**：

| 字段名     | 类型           | 说明                 |
| ---------- | -------------- | -------------------- |
| success    | boolean        | 请求是否成功         |
| elapsed_ms | number         | 接口处理耗时（毫秒） |
| data       | object / array | 实际业务数据         |

------

# 一、数据库 API（SCADA / MES）

> 数据库：iplant（SCADA） / ute_mes_qms_new（MES）

------

## 1. 工艺数据接口

### 接口地址

```
/api/process_data
```

### 请求方式

- `GET`
- `POST`

### 请求参数

| 参数名 | 类型   | 必填 | 说明     |
| ------ | ------ | ---- | -------- |
| code   | string | 是   | 工艺编号 |

### 请求示例

```
/api/process_data/?code=07_4_3mz2010
```

```bash
curl -X POST http://localhost:5000/api/process_data \
 -H "Content-Type: application/json" \
 -d '{"code":"07_4_3mz2010"}'
```

### 返回示例

```json
{
  "success": true,
  "elapsed_ms": 48.5,
  "data": {
    "加工方式": "仪表磨",
    "砂轮序号": 51,
    "砂轮线速度": 30.0,
    "粗磨1速度": 0.07,
    "粗磨1量": 0.05,
    "精磨速度": 0.03,
    "精磨量": 0.01,
    "光磨速度": 0.01,
    "光磨延时": 0.5
  }
}
```

------

## 2. 效率数据接口

### 接口地址

```
/api/efficiency_data
```

### 请求方式

- `GET`
- `POST`

### 请求参数

| 参数名 | 类型   | 必填 | 说明            |
| ------ | ------ | ---- | --------------- |
| code   | string | 是   | 工艺 / 设备编号 |

### 返回示例

```json
{
  "success": true,
  "elapsed_ms": 44.33,
  "data": {
    "光磨时长": 10.01,
    "快进时长": 2.81,
    "粗磨1时长": 7.93,
    "粗磨2时长": 0.04,
    "精磨时长": 4.93,
    "退刀时长": 2.37,
    "有效磨削时长": 23.05,
    "磨削总量": 3.44
  }
}
```

------

## 3. 首页 · 在线检验数据

> 数据来源与详细页在线巡检数据一致，主要用于首页汇总展示。

### 接口地址

```
/api/home_online_inspection
```

### 请求方式

- `POST`
- `GET`

### 请求参数

| 参数名 | 类型   | 必填 | 说明            |
| ------ | ------ | ---- | --------------- |
| code   | string | 是   | 设备 / 工艺编号 |

### 返回示例

```json
{
  "success": true,
  "elapsed_ms": 125.4,
  "data": {
    "抽检数": 5,
    "合格数": 5,
    "不合格数": 0,
    "预检不合格数": 0,
    "测量总数量": 247046,
    "合格总数量": 210278,
    "内径合格率": 85.0,
    "尺寸返工总数量": 17422,
    "尺寸报废总数量": 36506,
    "圆度返工总数量": 81,
    "锥度返工总数量": 6884
  }
}
```

------

## 4. 首页 · 首巡检数据（MES）

### 数据来源

MES 数据库（最近 3 条巡检记录）：

```sql
SELECT fqty_bad, fqty_good, type
FROM ute_mes_qms_new.t_qms_sj_taskiptitem
ORDER BY id DESC
LIMIT 3;
```

### 接口地址

```
/api/home_inspection
```

### 请求方式

- `GET`

### 请求参数

- 无

### 返回示例

```json
{
  "success": true,
  "elapsed_ms": 40.96,
  "data": [
    { "type": "巡检", "抽检数": 12, "合格数": 12, "不合格数": 0 },
    { "type": "巡检", "抽检数": 12, "合格数": 12, "不合格数": 0 },
    { "type": "巡检", "抽检数": 14, "合格数": 14, "不合格数": 0 }
  ]
}
```

------

## 5. 详细页 · 首巡检数据（模拟数据）

### 接口地址

```
/api/details_inspection
```

### 请求方式

- `GET` / `POST`

### 返回示例

```json
{
  "success": true,
  "data": {
    "内径尺寸标准": "φ17(-0.0005~-0.0035)",
    "内径尺寸结果": "合格",
    "垂直差标准": "0.002",
    "垂直差结果": "合格",
    "粗糙度标准": "Ra 0.2μm",
    "粗糙度结果": "合格",
    "表面质量标准": "无缺陷",
    "表面质量结果": "不合格",
    "表面质量备注": "2个生锈"
  }
}
```

------

## 6. 详细页 · 在线巡检数据

### 接口地址

```
/api/detailed_online_inspection
```

### 请求方式

- `GET`
- `POST`

### 请求频率

- **1 次 / 10 秒（0.1 QPS）**

### 请求参数

| 参数名 | 类型   | 必填 | 说明            |
| ------ | ------ | ---- | --------------- |
| code   | string | 是   | 设备 / 工艺编号 |

### 返回示例

```json
{
  "success": true,
  "elapsed_ms": 46.68,
  "data": {
    "上截面圆度": 2.0,
    "上截面圆度结果": "合格",
    "上截面尺寸": -14.1,
    "上截面尺寸结果": "返工",
    "内径测量总数量": 100562,
    "内径合格总数量": 88706,
    "内径合格率": 88.0,
    "尺寸返工总数量": 7877,
    "尺寸报废总数量": 11622
  }
}
```

------

## 7. 设备运行状态查询

### 接口地址

```
/api/device_status
```

### 请求参数

| 参数名 | 类型   | 必填 | 说明            |
| ------ | ------ | ---- | --------------- |
| code   | string | 是   | 可使用下划线 `_` |

### 请求示例

```bash
curl -X POST http://localhost:5000/api/device_status \
 -H "Content-Type: application/json" \
 -d '{"code":"01-1-3MZY1310"}'
```

### 返回示例

```json
{ "data": 0, "elapsed_ms": 70.02, "success": true }
```

### 状态码

| 状态码 | 状态     |
| ------ | -------- |
| 0      | 关机     |
| 1      | 开机     |
| 2      | 运行     |
| 3      | 故障报警 |
| 4      | 等待     |
| 5      | 设置     |
| 6      | 维护     |

------

# 二、OPC UA 实时机床状态 API

> 通过 OPC UA DataChange 订阅获取机床实时加工状态。
> 每台机床 1 个 Subscription、11 个 MonitoredItem。

------

## 8. 单机床加工状态

### 接口地址

```
GET /api/machine/status
```

### 请求参数

| 参数名 | 类型   | 必填 | 说明        |
| ------ | ------ | ---- | ----------- |
| ip     | string | 是   | 机床 IP 地址 |

### 请求示例

```
GET /api/machine/status?ip=192.168.11.206
```

### 返回示例

```json
{
  "ip": "192.168.11.206",
  "connected": true,
  "data": {
    "work_count": 54486,
    "work_time": 0,
    "fast_forward": 0,
    "fast_approach": 0,
    "rough1": 2000,
    "rough2": 0,
    "accurate": 500,
    "buffing": 0,
    "return_tool": 0,
    "stage": "精磨",
    "stage_time": 5.0,
    "machine_state": {
      "production": true,
      "standstill": false,
      "adjust": false,
      "malfunction": false,
      "wait_feed": false,
      "nc_suspend": false
    },
    "timestamp": "2026-07-20T17:30:15"
  }
}
```

### data 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `work_count` | int | 工件计数（累计） |
| `work_time` | int | 加工总时间 |
| `stage` | string | 当前阶段：快进 / 快趋 / 粗磨1 / 粗磨2 / 精磨 / 光磨 / 退刀 / 故障 / NC暂停 / 等待缺料 / 空闲 |
| `stage_time` | float | 当前阶段累计计时（秒） |
| `machine_state` | object | 机床运行状态位（见下表） |
| `timestamp` | string | 数据最后更新时间（ISO 8601） |

### machine_state 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `production` | bool | 生产中 |
| `standstill` | bool | 空运行 |
| `adjust` | bool | 调整中 |
| `malfunction` | bool | 故障 |
| `wait_feed` | bool | 等待缺料 |
| `nc_suspend` | bool | NC 暂停 |

### stage 优先级

```
故障 > NC暂停 > 等待缺料 > 正常加工阶段 > 空闲
```

当 `malfunction=true` 时，即使正在精磨，`stage` 也会返回 `"故障"`。

------

## 9. 批量机床加工状态

### 接口地址

```
GET /api/machines/status
```

### 请求参数

| 参数名 | 类型   | 必填 | 说明                      |
| ------ | ------ | ---- | ------------------------- |
| ips    | string | 是   | 机床 IP 列表，逗号分隔    |

### 请求示例

```
GET /api/machines/status?ips=192.168.11.206,192.168.11.207
```

### 返回示例

```json
{
  "machines": {
    "192.168.11.206": {
      "connected": true,
      "data": { "stage": "精磨", "stage_time": 5.0, "machine_state": { ... }, "timestamp": "..." }
    },
    "192.168.11.207": {
      "connected": false,
      "data": null
    }
  }
}
```

------

## 10. 机床连接统计

### 接口地址

```
GET /api/machines/count
```

### 返回示例

```json
{
  "count": 2,
  "max": 50
}
```

------

## 健康检查

### 接口地址

```
GET /health
```

### 返回示例

```json
{ "status": "ok" }
```

### 连接池状态

```
GET /pool/status
```

------

# 三、性能参考

## 数据库查询

SQL 查询使用索引取最新一条记录，时间复杂度 `O(1)`。

```
[查询成功] code=07_4_3mz2010, 耗时: 44.54ms
```

SSH 隧道下并发瓶颈约 **20 QPS**（瓶颈在 SSH 传输）。如果使用内网直连去掉 SSH，可大幅提升。

## OPC UA 采集

- 每机床 1 个 OPC UA Subscription，11 个 MonitoredItem
- DataChange 订阅间隔 1000ms
- 健康检查间隔 5s
- 断线自动重连（重连间隔 5s）
- 阶段空闲超时 3s（超过此时间无 DataChange 判定为空闲）
- 最大并发机床数：50（可通过 `MAX_MACHINES` 环境变量调整）

## 连接池配置

```python
# SCADA 数据库
db_pool_scada = PooledDB(
    creator=pymysql,
    maxconnections=30,
    mincached=10,
    maxcached=24,
    ...
)

# MES 数据库
db_pool_mes = PooledDB(
    creator=pymysql,
    maxconnections=10,
    mincached=2,
    maxcached=8,
    ...
)

# Waitress
serve(app, host='0.0.0.0', port=5000, threads=32)
```

可使用内网直连 + 调整连接池参数 + 调整线程数来优化性能。
