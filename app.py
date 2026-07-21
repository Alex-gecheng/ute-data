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
import asyncio
import threading
import logging
from asyncua import Client

# ==================================================
# 日志配置
# ==================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# 屏蔽 asyncua 内部 Publish 保活日志
logging.getLogger("asyncua").setLevel(logging.WARNING)

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

#  数据库配置 - SCADA 
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


# ==================================================
# OPC UA 配置
# ==================================================
USERNAME = os.getenv("OPCUA_USERNAME", "OpcUaClient")
PASSWORD = os.getenv("OPCUA_PASSWORD", "OpcUaClient")
PORT = int(os.getenv("OPCUA_PORT", "4840"))
MAX_MACHINES = int(os.getenv("MAX_MACHINES", "50"))
HEALTH_CHECK_INTERVAL = 5  # 健康检查间隔（秒）
RECONNECT_DELAY = 5         # 重连间隔（秒）
STAGE_IDLE_TIMEOUT = 3      # 阶段超时空闲判断（秒）：超过此时间无 DataChange 则认为空闲

# ==================================================
# 加工阶段：数据key → 中文名称
# ==================================================
_STAGE_KEY_TO_NAME = {
    "fast_forward": "快进",
    "fast_approach": "快趋",
    "rough1": "磨削",
    "rough2": "磨削",
    "accurate": "磨削",
    "buffing": "磨削",
    "return_tool": "退刀",
}

# ==================================================
# OPC UA 订阅节点列表
# ==================================================
_SUBSCRIBE_NODES = [
    # --- 加工计数与时间 ---
    "ns=2;s=/Nck/State/aDbd[420]",   # 工件计数
    "ns=2;s=/Nck/State/aDbw[428]",   # 加工总时间
    # --- 各阶段累计计时 ---
    "ns=2;s=/Nck/State/aDbw[430]",   # 快进
    "ns=2;s=/Nck/State/aDbw[432]",   # 快趋
    "ns=2;s=/Nck/State/aDbw[434]",   # 磨削（粗磨1）
    "ns=2;s=/Nck/State/aDbw[436]",   # 磨削（粗磨2）
    "ns=2;s=/Nck/State/aDbw[438]",   # 磨削（精磨）
    "ns=2;s=/Nck/State/aDbw[440]",   # 磨削（光磨）
    "ns=2;s=/Nck/State/aDbw[442]",   # 退刀
    # --- 运行状态 ---
    "ns=2;s=/Nck/State/aDbw[820]",   # 生产状态（Bit0:生产 Bit1:空运行 Bit2:调整 Bit3:故障）
    "ns=2;s=/Nck/State/aDbw[822]",   # 等待状态（Bit0:等待缺料 Bit1:NC暂停）
]


# ==================================================
# 单台机床客户端
# ==================================================
class MachineClient:
    """管理单台机床的 OPC UA 连接、订阅、数据缓存（线程安全）"""

    def __init__(self, ip):
        self.ip = ip
        self.client = None
        self.subscriptions = []          # Subscription 列表（正常 len=1），用于 _disconnect 统一清理
        self.subscription_handles = []   # MonitoredItem handle 列表
        self.connected = False

        # 等待首次连接完成（重连时会被 clear，保证状态准确）
        self.ready = threading.Event()

        # 线程安全锁（保护所有共享数据）
        self.lock = threading.Lock()

        # ---- 数据缓存 ----
        self.data = {
            "work_count": 0,
            "work_time": 0,
            "fast_forward": 0,
            "fast_approach": 0,
            "rough1": 0,
            "rough2": 0,
            "accurate": 0,
            "buffing": 0,
            "return_tool": 0,
            "stage": "空闲",
            "stage_time": 0,
            "machine_state": {
                "production": False,
                "standstill": False,
                "adjust": False,
                "malfunction": False,
                "wait_feed": False,
                "nc_suspend": False,
            },
        }

        # ---- 加工阶段追踪 ----
        # current_stage: 最近一次收到 DataChange 的阶段 timer key（如 "accurate"）
        # 仅 OPC UA 订阅回调（DataChange 通知）会更新此字段，初始读取不会。
        self.current_stage = None
        self.stage_update_time = 0  # 最近一次阶段 DataChange 的时间戳

        # 初始同步标记：True 期间不触发 current_stage 更新
        self._initial_sync = False

        # 最后一次数据更新时间戳
        self._last_update_time = 0

        # 后台 asyncio 线程
        self.thread = None

    # =============================================
    # 启动后台线程
    # =============================================
    def start(self):
        """启动后台 asyncio 线程（幂等）"""
        if self.thread is None:
            self.thread = threading.Thread(
                target=self._run_loop,
                daemon=True,
                name=f"OPC-{self.ip}",
            )
            self.thread.start()
            logger.info("后台线程已启动: %s", self.ip)

    def _run_loop(self):
        """线程入口：创建独立 asyncio 事件循环"""
        asyncio.run(self.connect_loop())

    # =============================================
    # 自动重连循环
    # =============================================
    async def connect_loop(self):
        """主循环：连接 → 健康检查 → 断连 → 重连"""
        while True:
            try:
                # === 确保旧资源已清理 ===
                await self._disconnect()
                await self.connect()

                # 连接成功 → 周期健康检查
                while True:
                    with self.lock:
                        if not self.connected:
                            break
                    await asyncio.sleep(HEALTH_CHECK_INTERVAL)

                    if not await self._health_check():
                        logger.warning("健康检查失败: %s，主动断开重连", self.ip)
                        await self._disconnect()
                        break  # 跳出内层 while，进入外层重连

            except Exception as e:
                logger.error("连接异常: %s - %s", self.ip, e)
                await self._disconnect()

            # 统一重连等待
            logger.info("%s — %d 秒后重连...", self.ip, RECONNECT_DELAY)
            await asyncio.sleep(RECONNECT_DELAY)

    # =============================================
    # OPC UA 连接
    # =============================================
    async def connect(self):
        """建立 OPC UA 连接并创建订阅。
        顺序：connect → connected=True → create_subscription → ready.set()
        ready.set() 必须在 Subscription + MonitoredItem 全部就绪之后，
        确保 HTTP 层拿到的是真正可用的数据。"""
        url = f"opc.tcp://{self.ip}:{PORT}"
        self.client = Client(url)
        self.client.set_user(USERNAME)
        self.client.set_password(PASSWORD)

        await self.client.connect()
        logger.info("OPC UA 连接成功: %s", self.ip)

        with self.lock:
            self.connected = True

        await self.create_subscription()

        # Subscription + MonitoredItem 全部就绪后才标记 ready
        self.ready.set()

    # =============================================
    # 创建订阅（1 机床 = 1 Subscription + 11 MonitoredItem）
    # =============================================
    async def create_subscription(self):
        """
        创建一个 OPC UA Subscription，将所有 _SUBSCRIBE_NODES 挂载为 MonitoredItem。

        架构：
            Session
             └── Subscription × 1
                   ├── MonitoredItem (node 1)
                   ├── MonitoredItem (node 2)
                   └── ... (共 11 个)

        时序：
            1. _initial_sync = True   （阻止阶段误判）
            2. 创建 Subscription
            3. 创建所有 MonitoredItem（先建立订阅通道）
            4. 读取初始值填充缓存
            5. _initial_sync = False  （后续 DataChange 正常更新阶段）

        异常回滚：任何步骤失败都会删除已创建的 Subscription，清理列表。
        """
        machine = self  # 闭包引用

        class SubscriptionHandler:
            def datachange_notification(self, node, value, data):
                machine._on_data_change(node.nodeid.Identifier, value)

        sub = None

        try:
            # ====== Step 1: 标记初始同步 ======
            with self.lock:
                self._initial_sync = True

            # ====== Step 2: 收集所有节点 ======
            nodes = []
            for node_str in _SUBSCRIBE_NODES:
                node = self.client.get_node(node_str)
                nodes.append(node)

            # ====== Step 3: 创建 1 个 Subscription ======
            logger.info("%s 创建 OPC UA Subscription", self.ip)
            sub = await self.client.create_subscription(1000, SubscriptionHandler())
            self.subscriptions.append(sub)

            # ====== Step 4: 创建 MonitoredItem（先建立通道，再读初始值） ======
            handles = await sub.subscribe_data_change(nodes)
            self.subscription_handles = handles if isinstance(handles, list) else [handles]

            # ====== Step 5: 读取初始值（Subscription 已建立，无数据丢失窗口） ======
            for node in nodes:
                try:
                    value = await node.read_value()
                    self._on_data_change(node.nodeid.Identifier, value)
                    logger.info("%s 初始值 %s = %s", self.ip, node.nodeid.Identifier, value)
                except Exception as e:
                    logger.warning("读取初始值失败 %s - %s: %s", self.ip, node.nodeid.Identifier, e)

            # ====== Step 6: 初始同步完成 ======
            with self.lock:
                self._initial_sync = False

            logger.info("%s 订阅完成: MonitoredItems=%d, Subscriptions=%d",
                        self.ip, len(nodes), len(self.subscriptions))

        except Exception:
            # ====== 异常回滚：删除已创建的 Subscription ======
            if sub is not None:
                try:
                    await sub.delete()
                except Exception as cleanup_err:
                    logger.warning("回滚删除 Subscription 失败 %s: %s", self.ip, cleanup_err)
            self.subscriptions.clear()
            self.subscription_handles.clear()
            raise

    # =============================================
    # 健康检查
    # =============================================
    async def _health_check(self):
        """
        通过读取 OPC UA ServerState 节点探测连接是否存活。
        返回值：True=健康, False=异常
        """
        if self.client is None:
            return False
        try:
            node = self.client.get_node("i=2259")
            await asyncio.wait_for(node.read_value(), timeout=3)
            return True
        except Exception:
            return False

    # =============================================
    # 断开连接（完整清理，避免残留订阅）
    # =============================================
    async def _disconnect(self):
        """
        清理断开连接。幂等，可安全重复调用。

        清理顺序：
        1. 标记 connected=False + ready.clear()
        2. 删除所有 Subscription
        3. 断开 client
        4. 清空 handles、重置阶段追踪
        """
        was_connected = self.connected
        with self.lock:
            self.connected = False
        self.ready.clear()

        # ---- 删除所有订阅（先于 client 断开，避免 BadNoSubscription）----
        for sub in list(self.subscriptions):
            try:
                await sub.delete()
                logger.info("订阅已删除: %s", self.ip)
            except Exception as e:
                logger.warning("删除 Subscription 失败 %s: %s", self.ip, e)
        self.subscriptions.clear()
        self.subscription_handles.clear()

        # ---- 断开 client ----
        if self.client is not None:
            try:
                await self.client.disconnect()
                logger.info("OPC 客户端已断开: %s", self.ip)
            except Exception as e:
                logger.warning("断开 Client 失败 %s: %s", self.ip, e)
            self.client = None

        # ---- 重置阶段追踪 ----
        with self.lock:
            self.current_stage = None
            self.stage_update_time = 0

        if was_connected:
            logger.info("完整断开清理完成: %s", self.ip)

    # =============================================
    # 数据变更回调（OPC UA 订阅线程调用）
    # =============================================
    def _on_data_change(self, nid, value):
        """
        处理 DataChange 通知 & 初始值读取。

        阶段判断原则：
        - OPC UA DataChange 通知到达 = 该计时器正在累加 = 当前加工阶段
        - 不根据累计值 > 0 判断（加工完成后历史值仍然 >0）
        - _initial_sync=True 时只记录基准值，不触发阶段判断

        参数：
            nid: 节点标识符字符串（如 "aDbd[420]", "aDbw[438]"）
            value: 节点当前值
        """
        with self.lock:
            self._last_update_time = time.time()

            # --- 工件计数 ---
            if "aDbd[420]" in nid:
                self.data["work_count"] = value

            # --- 加工总时间 ---
            elif "aDbw[428]" in nid:
                self.data["work_time"] = value

            # --- 各阶段累计计时 ---
            # DataChange 到达 → 该计时器正在变化 → 当前阶段
            elif "aDbw[430]" in nid:
                self.data["fast_forward"] = value
                if not self._initial_sync:
                    self.current_stage = "fast_forward"
                    self.stage_update_time = time.time()
            elif "aDbw[432]" in nid:
                self.data["fast_approach"] = value
                if not self._initial_sync:
                    self.current_stage = "fast_approach"
                    self.stage_update_time = time.time()
            elif "aDbw[434]" in nid:
                self.data["rough1"] = value
                if not self._initial_sync:
                    self.current_stage = "rough1"
                    self.stage_update_time = time.time()
            elif "aDbw[436]" in nid:
                self.data["rough2"] = value
                if not self._initial_sync:
                    self.current_stage = "rough2"
                    self.stage_update_time = time.time()
            elif "aDbw[438]" in nid:
                self.data["accurate"] = value
                if not self._initial_sync:
                    self.current_stage = "accurate"
                    self.stage_update_time = time.time()
            elif "aDbw[440]" in nid:
                self.data["buffing"] = value
                if not self._initial_sync:
                    self.current_stage = "buffing"
                    self.stage_update_time = time.time()
            elif "aDbw[442]" in nid:
                self.data["return_tool"] = value
                if not self._initial_sync:
                    self.current_stage = "return_tool"
                    self.stage_update_time = time.time()

            # --- 生产状态 aDbw[820] ---
            elif "aDbw[820]" in nid:
                try:
                    v = int(value)
                    self.data["machine_state"]["production"] = bool(v & 0x01)
                    self.data["machine_state"]["standstill"] = bool(v & 0x02)
                    self.data["machine_state"]["adjust"] = bool(v & 0x04)
                    self.data["machine_state"]["malfunction"] = bool(v & 0x08)
                except (TypeError, ValueError):
                    logger.warning("无法解析 aDbw[820] 值: %s", value)

            # --- 等待状态 aDbw[822] ---
            elif "aDbw[822]" in nid:
                try:
                    v = int(value)
                    self.data["machine_state"]["wait_feed"] = bool(v & 0x01)
                    self.data["machine_state"]["nc_suspend"] = bool(v & 0x02)
                except (TypeError, ValueError):
                    logger.warning("无法解析 aDbw[822] 值: %s", value)

    # =============================================
    # 阶段解析（供 get_data 调用，在锁内执行）
    # =============================================
    def _parse_stage(self):
        """
        根据 DataChange 最近变化 + 异常状态优先级 + 超时判断当前阶段。

        优先级（从高到低）：
        1. 故障（machine_state.malfunction）
        2. NC暂停（machine_state.nc_suspend）
        3. 等待缺料（machine_state.wait_feed）
        4. 正常加工阶段（快进/快趋/磨削/退刀，基于最近 DataChange 的计时器）
       5. 空闲（超时或无 DataChange）
        """
        ms = self.data["machine_state"]

        # Priority 1: 故障
        if ms["malfunction"]:
            self.data["stage"] = "故障"
            self.data["stage_time"] = 0
            return

        # Priority 2: NC暂停
        if ms["nc_suspend"]:
            self.data["stage"] = "NC暂停"
            self.data["stage_time"] = 0
            return

        # Priority 3: 等待缺料
        if ms["wait_feed"]:
            self.data["stage"] = "等待缺料"
            self.data["stage_time"] = 0
            return

        # ---- 正常加工阶段判断 ----

        # 超时检测：超过 STAGE_IDLE_TIMEOUT 无阶段 DataChange → 空闲
        if self.stage_update_time > 0:
            elapsed = time.time() - self.stage_update_time
            if elapsed > STAGE_IDLE_TIMEOUT:
                self.data["stage"] = "空闲"
                self.data["stage_time"] = 0
                return

        # 从未收到过阶段 DataChange → 空闲
        if self.current_stage is None:
            self.data["stage"] = "空闲"
            self.data["stage_time"] = 0
            return

        # 根据 current_stage 查找阶段名称
        stage_name = _STAGE_KEY_TO_NAME.get(self.current_stage)
        if stage_name is None:
            self.data["stage"] = "空闲"
            self.data["stage_time"] = 0
            return

        # 取对应计时变量的当前值
        current_value = self.data.get(self.current_stage, 0)
        if current_value > 0:
            self.data["stage"] = stage_name
            self.data["stage_time"] = round(current_value * 0.01, 2)
        else:
            self.data["stage"] = "空闲"
            self.data["stage_time"] = 0

    # =============================================
    # 线程安全的数据读取（供 HTTP 层调用）
    # =============================================
    def get_data(self):
        """
        返回数据副本。在锁保护下执行阶段解析和数据拷贝。

        Flask 请求线程调用此方法，与 OPC UA 回调线程互斥。
        """
        with self.lock:
            # 先解析阶段，再返回数据
            self._parse_stage()

            # 浅拷贝顶层 + 独立拷贝 machine_state 嵌套 dict
            result = self.data.copy()
            result["machine_state"] = dict(self.data["machine_state"])

            # 附加时间戳
            if self._last_update_time > 0:
                result["timestamp"] = time.strftime(
                    "%Y-%m-%dT%H:%M:%S",
                    time.localtime(self._last_update_time),
                )
            else:
                result["timestamp"] = ""

            return result


# ==================================================
# 多机床管理器
# ==================================================
class MachineManager:
    """管理所有机床客户端，限制最大连接数，提供查询接口"""

    def __init__(self):
        self.machines = {}  # ip → MachineClient
        self.lock = threading.Lock()

    def get(self, ip):
        """
        获取或创建机床客户端。
        首次创建时启动后台线程并等待连接就绪。
        """
        with self.lock:
            if ip not in self.machines:
                if len(self.machines) >= MAX_MACHINES:
                    raise RuntimeError(
                        f"too many machines: {len(self.machines)} >= {MAX_MACHINES}"
                    )
                machine = MachineClient(ip)
                self.machines[ip] = machine
                machine.start()
            else:
                machine = self.machines[ip]

        if not machine.ready.is_set():
            machine.ready.wait(timeout=5)

        return machine

    def get_all(self, ips):
        """批量获取机床客户端"""
        result = {}
        for ip in ips:
            try:
                result[ip] = self.get(ip)
            except RuntimeError:
                raise
        return result

    def get_count(self):
        """返回当前管理的机床数量"""
        with self.lock:
            return len(self.machines)


# ==================================================
# 全局实例
# ==================================================
manager = MachineManager()


# ==================================================
# Flask API — 单机床查询
# ==================================================
@app.route("/api/machine/status", methods=["GET"])
def machine_status():
    """
    GET /api/machine/status?ip=192.168.11.206
    """
    ip = request.args.get("ip")
    if not ip:
        return jsonify({"error": "missing ip"}), 400

    try:
        machine = manager.get(ip)
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 429

    data = machine.get_data()
    return jsonify({
        "ip": ip,
        "connected": machine.connected,
        "data": data,
    })


# ==================================================
# Flask API — 批量查询
# ==================================================
@app.route("/api/machines/status", methods=["GET"])
def machines_status():
    """
    GET /api/machines/status?ips=192.168.11.206,192.168.11.207
    """
    ips_raw = request.args.get("ips", "")
    if not ips_raw:
        return jsonify({"error": "missing ips"}), 400

    ip_list = [ip.strip() for ip in ips_raw.split(",") if ip.strip()]
    if not ip_list:
        return jsonify({"error": "empty ips"}), 400

    try:
        machines = manager.get_all(ip_list)
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 429

    result = {}
    for ip in ip_list:
        try:
            machine = machines.get(ip)
            if machine is None:
                result[ip] = {"connected": False, "data": None, "error": "unknown"}
            else:
                data = machine.get_data()
                result[ip] = {
                    "connected": machine.connected,
                    "data": data,
                }
        except RuntimeError as e:
            result[ip] = {"connected": False, "data": None, "error": str(e)}

    return jsonify({"machines": result})


# ==================================================
# 健康检查 & 状态接口
# ==================================================
@app.route("/api/machines/count", methods=["GET"])
def machines_count():
    return jsonify({
        "count": manager.get_count(),
        "max": MAX_MACHINES,
    })


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



