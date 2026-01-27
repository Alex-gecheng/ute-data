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

DB_CONFIG = {
    'host': '192.168.10.251',
    'port': 3306,
    'user': 'bigdata',
    'password': 'bigdata@z6wRPj',
    'database': 'iplantute'
}

# 全局 SSH 隧道和连接池
tunnel = None
db_pool = None
variable_name_map = {}
code_name_map = {}  # Code → Name 映射


def load_variable_name_map():
    """从数据库读取 dms_device_parameter 表，加载映射到内存"""
    if not db_pool:
        print("错误：连接池未初始化")
        return
    
    try:
        connection = db_pool.connection()
        with connection.cursor() as cursor:
            # 查询设备参数表
            sql = "SELECT Code, VariableName, Name FROM iplantute.dms_device_parameter"
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
    """初始化 SSH 隧道和数据库连接池"""
    global tunnel, db_pool
    
    print("=" * 60)
    print("正在建立 SSH 隧道...")
    tunnel = SSHTunnelForwarder(
        (SSH_CONFIG['host'], SSH_CONFIG['port']),
        ssh_username=SSH_CONFIG['user'],
        ssh_password=SSH_CONFIG['password'],
        remote_bind_address=(DB_CONFIG['host'], DB_CONFIG['port']),
        local_bind_address=('127.0.0.1', 0)
    )
    tunnel.start()
    print(f"SSH 隧道已建立，本地端口: {tunnel.local_bind_port}")
    
    print("正在创建数据库连接池...")
    db_pool = PooledDB(
        creator=pymysql,
        maxconnections=30,        # 最大连接数
        mincached=10,              # 初始化时至少创建的空闲连接
        maxcached=24,             # 连接池中最多空闲连接数
        maxshared=0,              # 共享连接数（0 表示不共享）
        blocking=True,            # 连接数达到最大时，新连接是否阻塞
        maxusage=0,               # 单个连接最多被使用次数（0 表示无限制）
        setsession=[],
        ping=1,                   # 连接前检测连接是否可用（0=不检测，1=默认检测）
        host='127.0.0.1',
        port=tunnel.local_bind_port,
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    print(f"连接池已创建 [初始连接: 5, 最大连接: 20]")
    print("=" * 60)

def cleanup():
    """关闭连接池和 SSH 隧道"""
    global tunnel, db_pool
    print("\n正在关闭连接池和 SSH 隧道...")
    if db_pool:
        db_pool.close()
    if tunnel:
        tunnel.stop()
    print("资源已释放")

# 注册退出时的清理函数
atexit.register(cleanup)


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
        connection = db_pool.connection()
        
        with connection.cursor() as cursor:
            # 执行查询
            table_name = f"dms_device_technology_{code}"
            sql = f"SELECT * FROM iplantute.{table_name} ORDER BY ID DESC LIMIT 1"
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
        connection = db_pool.connection()
        
        with connection.cursor() as cursor:
            # 执行查询
            table_name = f"dms_device_workparams_{code}"
            sql = f"SELECT * FROM iplantute.{table_name} ORDER BY ID DESC LIMIT 1"
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
            print(f"[请求失败] 耗时: {elapsed:.2f}ms - 缺少 code 参数")
            return jsonify({
                'success': False,
                'error': '缺少 code 参数'
            }), 400
        
        # 从连接池获取连接
        connection = db_pool.connection()
        
        with connection.cursor() as cursor:
            # 执行查询
            table_name = f"dms_device_qualityparams_{code}"
            sql = f"SELECT * FROM iplantute.{table_name} ORDER BY ID DESC LIMIT 1"
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






@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok'})

@app.route('/pool/status', methods=['GET'])
def pool_status():
    """查看连接池状态"""
    if db_pool:
        return jsonify({
            'status': 'running',
            'pool_info': '连接池运行中'
        })
    else:
        return jsonify({
            'status': 'not_initialized',
            'pool_info': '连接池未初始化'
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



