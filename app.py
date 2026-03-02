from flask import Flask, request, jsonify
from sshtunnel import SSHTunnelForwarder
import pymysql
from dbutils.pooled_db import PooledDB
import time
import atexit
import csv
import os
from waitress import serve
from flask_cors import CORS

app = Flask(__name__)

# 配置跨域（CORS）：默认仅允许局域网/私有网段与本机访问
# 通过环境变量 CORS_MODE 控制：'private'（默认）或 'all'
CORS_MODE = os.getenv('CORS_MODE', 'private').lower()
if CORS_MODE == 'all':
    # 允许所有来源（更开放，适用于快速联调）
    CORS(app, resources={r"/*": {"origins": "*", "methods": ["GET", "POST", "OPTIONS"]}})
else:
    # 仅允许常见私有网段与本机（局域网）
    PRIVATE_ORIGINS = [
        r"http://localhost(:\d+)?",
        r"http://127\.0\.0\.1(:\d+)?",
        r"http://192\.168\.\d{1,3}\.\d{1,3}(:\d+)?",
        r"http://10\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d+)?",
        r"http://172\.(1[6-9]|2[0-9]|3[0-1])\.\d{1,3}\.\d{1,3}(:\d+)?"
    ]
    CORS(app, resources={r"/*": {"origins": PRIVATE_ORIGINS, "methods": ["GET", "POST", "OPTIONS"]}})

# SSH 和数据库信息
SSH_CONFIG = {
    'host': '192.168.0.196',
    'port': 22,
    'user': 'root',
    'password': 'ute@2018'
}

#  数据库配置 - SCADA (
DB_CONFIG_SCADA = {
    'host': '192.168.10.251',
    'port': 3306,
    'user': 'bigdata',
    'password': 'bigdata@z6wRPj',
    'database': 'iplant'
}

# 数据库配置 - MES 
DB_CONFIG_SCADA_MES = {
    'host': '192.168.0.225',
    'port': 3306,
    'user': 'ute_view',
    'password': 'ute0126~!',
    'database': 'ute_mes_qms_new'  
}

# 全局 SSH 隧道和连接池 - 
tunnel_scada = None
db_pool_scada = None

tunnel_mes = None
db_pool_mes = None
variable_name_map = {}
code_name_map = {}  # Code → Name 映射


def load_variable_name_map():
    """从数据库读取 dms_device_parameter 表，加载映射到内存"""
    if not db_pool_scada:
        print("错误：连接池未初始化")
        return
    
    try:
        connection = db_pool_scada.connection()
        with connection.cursor() as cursor:
            # 查询设备参数表
            sql = "SELECT Code, VariableName, Name FROM iplant.dms_device_parameter"
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            var_count = 0
            code_count = 0
            
            for row in rows:
                code = row.get('Code')
                var_name = row.get('VariableName')
                name = row.get('Name')
                
                # Code → Name（Code 是唯一的）
                if code and name:
                    code_name_map[str(code)] = str(name)
                    code_count += 1
                
                # VariableName → Name（允许重复）
                if var_name and name:
                    if var_name not in variable_name_map:
                        var_count += 1
                    variable_name_map[str(var_name)] = str(name)
            
            print(f"已从数据库加载参数映射:")
            print(f"  Code 映射数: {code_count}")
            print(f"  VariableName 映射数: {var_count} (去重后)")
            
        connection.close()
        
    except Exception as e:
        print(f"加载映射失败: {str(e)}")

def init_connection_pool():
    """初始化 SSH 隧道和数据库连接池（两个数据库）"""
    global tunnel_scada, db_pool_scada, tunnel_mes, db_pool_mes
    
    print("=" * 60)
    
    # ========== SCADA系统 ==========
    print("正在建立 SSH 隧道 → 192.168.10.251...")
    tunnel_scada = SSHTunnelForwarder(
        (SSH_CONFIG['host'], SSH_CONFIG['port']),
        ssh_username=SSH_CONFIG['user'],
        ssh_password=SSH_CONFIG['password'],
        remote_bind_address=(DB_CONFIG_SCADA['host'], DB_CONFIG_SCADA['port']),
        local_bind_address=('127.0.0.1', 0)
    )
    tunnel_scada.start()
    print(f"SSH 隧道已建立 (SCADA)，本地端口: {tunnel_scada.local_bind_port}")
    
    print("正在创建数据库连接池 (SCADA)...")
    db_pool_scada = PooledDB(
        creator=pymysql,
        maxconnections=30,
        mincached=10,
        maxcached=24,
        maxshared=0,
        blocking=True,
        maxusage=0,
        setsession=[],
        ping=1,
        host='127.0.0.1',
        port=tunnel_scada.local_bind_port,
        user=DB_CONFIG_SCADA['user'],
        password=DB_CONFIG_SCADA['password'],
        database=DB_CONFIG_SCADA['database'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    print(f"连接池已创建 (SCADA) [初始连接: 10, 最大连接: 30]")
    
    # ========== MES系统 ==========
    print("-" * 60)
    print("正在建立 SSH 隧道 → 192.168.0.225...")
    tunnel_mes = SSHTunnelForwarder(
        (SSH_CONFIG['host'], SSH_CONFIG['port']),
        ssh_username=SSH_CONFIG['user'],
        ssh_password=SSH_CONFIG['password'],
        remote_bind_address=(DB_CONFIG_SCADA_MES['host'], DB_CONFIG_SCADA_MES['port']),
        local_bind_address=('127.0.0.1', 0)
    )
    tunnel_mes.start()
    print(f"SSH 隧道已建立 (MES)，本地端口: {tunnel_mes.local_bind_port}")
    
    print("正在创建数据库连接池 (MES)...")
    db_pool_mes = PooledDB(
        creator=pymysql,
        maxconnections=10,
        mincached=2,
        maxcached=8,
        maxshared=0,
        blocking=True,
        maxusage=0,
        setsession=[],
        ping=1,
        host='127.0.0.1',
        port=tunnel_mes.local_bind_port,
        user=DB_CONFIG_SCADA_MES['user'],
        password=DB_CONFIG_SCADA_MES['password'],
        database=DB_CONFIG_SCADA_MES['database'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    print(f"连接池已创建 (MES) [初始连接: 2, 最大连接: 10]")
    print("=" * 60)

def cleanup():
    """关闭连接池和 SSH 隧道"""
    global tunnel_scada, db_pool_scada, tunnel_mes, db_pool_mes
    print("\n正在关闭连接池和 SSH 隧道...")
    
    # 关闭 SCADA系统 资源
    if db_pool_scada:
        db_pool_scada.close()
    if tunnel_scada:
        tunnel_scada.stop()
    
    # 关闭 MES系统 资源
    if db_pool_mes:
        db_pool_mes.close()
    if tunnel_mes:
        tunnel_mes.stop()
    
    print("资源已释放")

# 注册退出时的清理函数
atexit.register(cleanup)

#设备运行状态    输入code 01-1-3MZY1310
@app.route('/api/device_status', methods=['GET', 'POST'])
def device_status():
    start_time = time.time()
    connection = None
    
    try:
        # 获取 code 参数
        if request.method == 'POST':
            code = request.json.get('code')
        else:
            code = request.args.get('code')
        
        if not code:
            elapsed = (time.time() - start_time) * 1000
            print(f"[请求失败] 耗时: {elapsed:.2f}ms - 缺少 code 参数")
            return jsonify({
                'success': False,
                'error': '缺少 code 参数'
            }), 400
        code = code.replace('_', '-')
        # 从连接池获取连接
        connection = db_pool_scada.connection()
        
        with connection.cursor() as cursor:
            # ===== Step 1：查 AssetNo（最优先）=====
            sql1 = """
            SELECT AssetNo
            FROM iplant.dms_device_ledger
            WHERE Code = %s
            LIMIT 1
            """

            cursor.execute(sql1, (code,))
            row = cursor.fetchone()

            if not row:
                elapsed = (time.time() - start_time) * 1000
                return jsonify({
                    'success': True,
                    'data': "Unknow",
                    'message': '未查询到数据',
                    'elapsed_ms': round(elapsed, 2)
                })

            asset_no = row[0] if not isinstance(row, dict) else row.get("AssetNo")

            # ===== Step 2：查设备状态（直接索引查找）=====
            sql2 = """
            SELECT DeviceStatus
            FROM iplant.dms_device_status
            WHERE DeviceID = %s
            LIMIT 1
            """

            cursor.execute(sql2, (int(asset_no),))
            row = cursor.fetchone()

            if not row:
                elapsed = (time.time() - start_time) * 1000
                return jsonify({
                    'success': True,
                    'data': "Unknow",
                    'message': '未查询到数据',
                    'elapsed_ms': round(elapsed, 2)
                })

            result = row[0] if not isinstance(row, dict) else row.get("DeviceStatus")

            if isinstance(result, dict):
                result = (
                    result.get("value")
                    or result.get("status")
                    or result.get("DeviceStatus")
                    or result.get("device_status")
                )

            if result is not None and not isinstance(result, int):
                try:
                    result = int(result)
                except (TypeError, ValueError):
                    elapsed = (time.time() - start_time) * 1000
                    return jsonify({
                        'success': False,
                        'error': f'DeviceStatus 类型异常: {type(result).__name__}',
                        'elapsed_ms': round(elapsed, 2)
                    }), 500
            
            elapsed = (time.time() - start_time) * 1000
            
            if result is not None:

                binary_str = format(result, '032b')
                reversed_bin = binary_str[::-1]

                # 状态映射：位 -> 状态码
                # 按优先级检查：故障报警 > 运行 > 等待 > 设置 > 维护 > 开机
                status_code = 0  # 默认为关机
                
                # bit 5: 故障报警 -> 3
                if len(reversed_bin) >= 5 and reversed_bin[4] == '1':
                    status_code = 3
                # bit 2, 20, 21: 运行相关 -> 2
                elif (len(reversed_bin) >= 2 and reversed_bin[1] == '1') or \
                     (len(reversed_bin) >= 20 and reversed_bin[19] == '1') or \
                     (len(reversed_bin) >= 21 and reversed_bin[20] == '1'):
                    status_code = 2
                # bit 9, 17, 18, 19: 等待相关 -> 4
                elif (len(reversed_bin) >= 9 and reversed_bin[8] == '1') or \
                     (len(reversed_bin) >= 17 and reversed_bin[16] == '1') or \
                     (len(reversed_bin) >= 18 and reversed_bin[17] == '1') or \
                     (len(reversed_bin) >= 19 and reversed_bin[18] == '1'):
                    status_code = 4
                # bit 10: 设置 -> 5
                elif len(reversed_bin) >= 10 and reversed_bin[9] == '1':
                    status_code = 5
                # bit 11: 维护 -> 6
                elif len(reversed_bin) >= 11 and reversed_bin[10] == '1':
                    status_code = 6
                # bit 1: 开机 -> 1
                elif len(reversed_bin) >= 1 and reversed_bin[0] == '1':
                    status_code = 1

                print(f"[查询成功] code={code}, status_code={status_code}, 耗时: {elapsed:.2f}ms")

                return jsonify({
                    'success': True,
                    'data': status_code,
                    'elapsed_ms': round(elapsed, 2)
                })
            else:
                print(f"[无数据] code={code}, 耗时: {elapsed:.2f}ms")
                return jsonify({
                    'success': True,
                    'data': "Unknow",
                    'message': '未查询到数据',
                    'elapsed_ms': round(elapsed, 2)
                })
                
    except Exception as e:
        elapsed = (time.time() - start_time) * 1000
        print(f"[查询异常] 耗时: {elapsed:.2f}ms - 错误: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'elapsed_ms': round(elapsed, 2)
        }), 500
    
    finally:
        # 归还连接到连接池
        if connection:
            connection.close()

# 工艺数据
@app.route('/api/process_data', methods=['GET', 'POST'])
def process_data():
    start_time = time.time()
    connection = None
    
    try:
        # 获取 code 参数
        if request.method == 'POST':
            code = request.json.get('code')
        else:
            code = request.args.get('code')
        
        if not code:
            elapsed = (time.time() - start_time) * 1000
            print(f"[请求失败] 耗时: {elapsed:.2f}ms - 缺少 code 参数")
            return jsonify({
                'success': False,
                'error': '缺少 code 参数'
            }), 400
        
        # 从连接池获取连接
        connection = db_pool_scada.connection()
        
        with connection.cursor() as cursor:
            # 执行查询
            table_name = f"dms_device_technology_{code}"
            sql = f"SELECT * FROM iplant.`{table_name}` ORDER BY ID DESC LIMIT 1"
            cursor.execute(sql)
            result = cursor.fetchone()
            
            elapsed = (time.time() - start_time) * 1000
            
            if result:
                # 尝试映射：优先用 Code，其次用 VariableName，都失败则跳过
                mapped = {}
                for k, v in result.items():
                    # 先尝试 Code 映射
                    mapped_key = code_name_map.get(str(k))
                    if mapped_key is None:
                        # 再尝试 VariableName 映射
                        mapped_key = variable_name_map.get(str(k))
                    
                    if mapped_key is not None:
                        mapped[mapped_key] = v

                # 加工方式值转换：101=仪表磨，100=定程磨
                if '加工方式' in mapped:
                    process_mode_map = {
                        101: '仪表磨',
                        100: '定程磨'
                    }
                    mode_value = mapped['加工方式']
                    try:
                        mode_key = int(float(mode_value))
                    except (TypeError, ValueError):
                        mode_key = None
                    if mode_key in process_mode_map:
                        mapped['加工方式'] = process_mode_map[mode_key]

                print(f"[查询成功] code={code}, 耗时: {elapsed:.2f}ms, 原始字段数={len(result)}, 映射字段数={len(mapped)}")
                return jsonify({
                    'success': True,
                    'data': mapped,
                    'elapsed_ms': round(elapsed, 2)
                })
            else:
                print(f"[无数据] code={code}, 耗时: {elapsed:.2f}ms")
                return jsonify({
                    'success': True,
                    'data': None,
                    'message': '未查询到数据',
                    'elapsed_ms': round(elapsed, 2)
                })
                
    except Exception as e:
        elapsed = (time.time() - start_time) * 1000
        print(f"[查询异常] 耗时: {elapsed:.2f}ms - 错误: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'elapsed_ms': round(elapsed, 2)
        }), 500
    
    finally:
        # 归还连接到连接池
        if connection:
            connection.close()

# 效率数据
@app.route('/api/efficiency_data', methods=['GET', 'POST'])
def efficiency_data():
    start_time = time.time()
    connection = None
    
    try:
        # 获取 code 参数
        if request.method == 'POST':
            code = request.json.get('code')
        else:
            code = request.args.get('code')
        
        if not code:
            elapsed = (time.time() - start_time) * 1000
            print(f"[请求失败] 耗时: {elapsed:.2f}ms - 缺少 code 参数")
            return jsonify({
                'success': False,
                'error': '缺少 code 参数'
            }), 400
        
        # 从连接池获取连接
        connection = db_pool_scada.connection()
        
        with connection.cursor() as cursor:
            # 执行查询
            table_name = f"dms_device_workparams_{code}"
            sql = f"SELECT * FROM iplant.`{table_name}` ORDER BY ID DESC LIMIT 1"
            cursor.execute(sql)
            result = cursor.fetchone()
            
            elapsed = (time.time() - start_time) * 1000
            
            if result:
                # 尝试映射：优先用 Code，其次用 VariableName，都失败则跳过
                mapped = {}
                for k, v in result.items():
                    # 先尝试 Code 映射
                    mapped_key = code_name_map.get(str(k))
                    if mapped_key is None:
                        # 再尝试 VariableName 映射
                        mapped_key = variable_name_map.get(str(k))
                    
                    if mapped_key is not None:
                        mapped[mapped_key] = v

                print(f"[查询成功] code={code}, 耗时: {elapsed:.2f}ms, 原始字段数={len(result)}, 映射字段数={len(mapped)}")
                return jsonify({
                    'success': True,
                    'data': mapped,
                    'elapsed_ms': round(elapsed, 2)
                })
            else:
                print(f"[无数据] code={code}, 耗时: {elapsed:.2f}ms")
                return jsonify({
                    'success': True,
                    'data': None,
                    'message': '未查询到数据',
                    'elapsed_ms': round(elapsed, 2)
                })
                
    except Exception as e:
        elapsed = (time.time() - start_time) * 1000
        print(f"[查询异常] 耗时: {elapsed:.2f}ms - 错误: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'elapsed_ms': round(elapsed, 2)
        }), 500
    
    finally:
        # 归还连接到连接池
        if connection:
            connection.close()

# 详细在线检验数据  
@app.route('/api/detailed_online_inspection', methods=['GET', 'POST'])
def detailed_online_inspection():
    start_time = time.time()
    connection = None
    
    try:
        # 获取 code 参数
        if request.method == 'POST':
            code = request.json.get('code')
        else:
            code = request.args.get('code')
        
        if not code:
            elapsed = (time.time() - start_time) * 1000
            print(f"[请求失败-225] 耗时: {elapsed:.2f}ms - 缺少 code 参数")
            return jsonify({
                'success': False,
                'error': '缺少 code 参数'
            }), 400
        
        # 从连接池获取连接
        connection = db_pool_scada.connection()
        
        with connection.cursor() as cursor:
            # 执行查询
            table_name = f"dms_device_qualityparams_{code}"
            sql = f"SELECT * FROM `{table_name}` ORDER BY ID DESC LIMIT 1"
            cursor.execute(sql)
            result = cursor.fetchone()
            
            elapsed = (time.time() - start_time) * 1000
            
            if result:
                # 尝试映射：优先用 Code，其次用 VariableName，都失败则跳过
                mapped = {}
                for k, v in result.items():
                    # 先尝试 Code 映射
                    mapped_key = code_name_map.get(str(k))
                    if mapped_key is None:
                        # 再尝试 VariableName 映射
                        mapped_key = variable_name_map.get(str(k))
                    
                    if mapped_key is not None:
                        mapped[mapped_key] = v

                print(f"[查询成功] code={code}, 耗时: {elapsed:.2f}ms, 原始字段数={len(result)}, 映射字段数={len(mapped)}")
                return jsonify({
                    'success': True,
                    'data': mapped,
                    'elapsed_ms': round(elapsed, 2)
                })
            else:
                print(f"[无数据] code={code}, 耗时: {elapsed:.2f}ms")
                return jsonify({
                    'success': True,
                    'data': None,
                    'message': '未查询到数据',
                    'elapsed_ms': round(elapsed, 2)
                })
                
    except Exception as e:
        elapsed = (time.time() - start_time) * 1000
        print(f"[查询异常-225] 耗时: {elapsed:.2f}ms - 错误: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'elapsed_ms': round(elapsed, 2)
        }), 500
    
    finally:
        # 归还连接到连接池
        if connection:
            connection.close()

# 首页在线检验数据    
@app.route('/api/home_online_inspection', methods=['GET','POST'])
def home_online_inspection():
    start_time = time.time()
    connection = None
    
    try:
        # 获取 code 参数
        if request.method == 'POST':
            code = request.json.get('code')
        else:
            code = request.args.get('code')
        
        if not code:
            elapsed = (time.time() - start_time) * 1000
            print(f"[请求失败-225] 耗时: {elapsed:.2f}ms - 缺少 code 参数")
            return jsonify({
                'success': False,
                'error': '缺少 code 参数'
            }), 400
        
        # 从连接池获取连接
        connection = db_pool_scada.connection()
        
        with connection.cursor() as cursor:
            # 执行查询
            table_name = f"dms_device_qualityparams_{code}"
            sql = f"SELECT * FROM `{table_name}` ORDER BY ID DESC LIMIT 1"
            cursor.execute(sql)
            result = cursor.fetchone()
            
            elapsed = (time.time() - start_time) * 1000
            
            if result:
                # 尝试映射：优先用 Code，其次用 VariableName，都失败则保留原key
                mapped = {}
                for k, v in result.items():
                    # 先尝试 Code 映射
                    mapped_key = code_name_map.get(str(k))
                    if mapped_key is None:
                        # 再尝试 VariableName 映射
                        mapped_key = variable_name_map.get(str(k))
                    
                    # 如果映射失败，保留原来的key
                    if mapped_key is None:
                        mapped_key = k
                    mapped[mapped_key] = v

                # 从 mapped 中提取所需字段
                result_values = [
                    v for k, v in mapped.items()
                    if k.endswith("结果") and isinstance(v, str)
                ]

                sample_count = len(result_values)
                qualified_count = sum(1 for v in result_values if v == "合格")
                unqualified_count = sample_count - qualified_count

                # 2. 直接读取的统计项（不存在则默认 0）
                total_measure_count = mapped.get("内径测量总数量", 0)
                total_qualified_count = mapped.get("内径合格总数量", 0)
                inner_diameter_pass_rate = mapped.get("内径合格率", 0)

                precheck_unqualified_count = mapped.get("预检不合格数量", 0)

                dimension_scrap_total = mapped.get("尺寸报废总数量", 0)
                dimension_rework_total = mapped.get("尺寸返工总数量", 0)
                roundness_rework_total = mapped.get("圆度返工总数量", 0)
                taper_rework_total = mapped.get("锥度返工总数量", 0)

                # 3. 机检（示例：存在设备状态即可认为有机检）
                # machine_inspection = 1 if "设备状态" in mapped else 0

                # 4. 汇总结果
                data = {
                    "抽检数": sample_count,
                    "合格数": qualified_count,
                    "不合格数": unqualified_count,

                    "测量总数量": total_measure_count,
                    "合格总数量": total_qualified_count,
                    "内径合格率": inner_diameter_pass_rate,

                    "预检不合格数": precheck_unqualified_count,

                    "尺寸报废总数量": dimension_scrap_total,
                    "尺寸返工总数量": dimension_rework_total,
                    "圆度返工总数量": roundness_rework_total,
                    "锥度返工总数量": taper_rework_total,
                }

                print(f"[查询成功] code={code}, 耗时: {elapsed:.2f}ms, 原始字段数={len(result)}, 映射字段数={len(mapped)}")
                return jsonify({
                    'success': True,
                    'data': data,
                    'elapsed_ms': round(elapsed, 2)
                })
            else:
                print(f"[无数据] code={code}, 耗时: {elapsed:.2f}ms")
                return jsonify({
                    'success': True,
                    'data': None,
                    'message': '未查询到数据',
                    'elapsed_ms': round(elapsed, 2)
                })
                
    except Exception as e:
        elapsed = (time.time() - start_time) * 1000
        print(f"[查询异常-home_online] 耗时: {elapsed:.2f}ms - 错误: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'elapsed_ms': round(elapsed, 2)
        }), 500
    
    finally:
        # 归还连接到连接池
        if connection:
            connection.close()


# 首页首巡检  MES系统数据
@app.route('/api/home_inspection', methods=['GET','POST'])
def home_inspection():
        start_time = time.time()
        connection = None
        
        try:
            
            
            # 从连接池获取连接
            connection = db_pool_mes.connection()
            
            with connection.cursor() as cursor:
                # 执行查询
                sql = f"SELECT fqty_bad,fqty_good,type FROM t_qms_sj_taskiptitem ORDER BY id DESC LIMIT 3;"
                cursor.execute(sql)
                result = cursor.fetchall()
                
                elapsed = (time.time() - start_time) * 1000
                
                if result:
                    # 每条记录分别返回统计
                    data = []
                    for row in result:
                        bad = row.get('fqty_bad', 0) or 0
                        good = row.get('fqty_good', 0) or 0
                        total = bad + good
                        data.append({
                            '不合格数': bad,         # 不良数
                            '合格数': good,       # 良品数
                            '抽检数': total,           # 总数
                            'type': row.get('type')   # 类型
                        })
                    
                    print(f"[查询成功-MES] , 耗时: {elapsed:.2f}ms, 记录数={len(result)}")
                    return jsonify({
                        'success': True,
                        'data': data,
                        'elapsed_ms': round(elapsed, 2)
                    })
                else:
                    print(f"[无数据] , 耗时: {elapsed:.2f}ms")
                    return jsonify({
                        'success': True,
                        'data': None,
                        'message': '未查询到数据',
                        'elapsed_ms': round(elapsed, 2)
                    })
                    
        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            print(f"[查询异常] 耗时: {elapsed:.2f}ms - 错误: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e),
                'elapsed_ms': round(elapsed, 2)
            }), 500
        
        finally:
            # 归还连接到连接池
            if connection:
                connection.close()

# 详情页首巡检    模拟数据
@app.route('/api/details_inspection', methods=['GET','POST'])
def details_inspection():
        # 模拟数据返回
        data = {
            "内径尺寸标准": "φ17(-0.0005~-0.0035)",
            "内径尺寸结果": "合格",
            "垂直差标准": "0.002",
            "垂直差结果": "合格",
            "壁厚差标准": "0.0015",
            "壁厚差结果": "合格",
            "椭圆标准": "0.001",
            "椭圆结果": "合格",
            "锥度标准": "0.001",
            "锥度结果": "合格",
            "粗糙度标准": "Ra 0.2μm",
            "粗糙度结果": "合格",
            "表面质量标准": "无缺陷",
            "表面质量结果": "不合格",
            "表面质量备注": "2个生锈"
        }
        return jsonify({
            'success': True,
            'data': data
        })

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok'})

@app.route('/pool/status', methods=['GET'])
def pool_status():
    """查看连接池状态"""
    status = {
        'pool_scada': 'running' if db_pool_scada else 'not_initialized',
        'pool_mes': 'running' if db_pool_mes else 'not_initialized'
    }
    if db_pool_scada and db_pool_mes:
        return jsonify({
            'status': 'running',
            'pool_info': status
        })
    else:
        return jsonify({
            'status': 'partial',
            'pool_info': status
        }), 503

if __name__ == '__main__':
    # 启动前初始化连接池
    init_connection_pool()
    # 从数据库加载映射（需要先初始化连接池）
    load_variable_name_map()
    
    # 使用 Waitress 启动生产级服务器
    print("正在启动 Waitress 服务器...")
    print("服务地址: http://0.0.0.0:5000")
    print("按 Ctrl+C 停止服务")
    try:
        serve(app, host='0.0.0.0', port=5000, threads=32)
    except KeyboardInterrupt:
        print("\n接收到终止信号...")
    finally:
        cleanup()



