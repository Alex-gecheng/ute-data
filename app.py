from flask import Flask, request, jsonify
from sshtunnel import SSHTunnelForwarder
import pymysql
from dbutils.pooled_db import PooledDB
import time
import atexit
import csv
import os
from waitress import serve

app = Flask(__name__)

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


def load_variable_name_map():
    """将 CSV 中的 VariableName→Name 映射加载到内存"""
    csv_path = os.path.join(os.path.dirname(__file__), 'device_parameter.csv')
    if not os.path.exists(csv_path):
        print(f"未找到映射文件: {csv_path}")
        return

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            var_name = row.get('VariableName')
            name = row.get('Name')
            if var_name and name:
                variable_name_map[str(var_name)] = str(name)

    print(f"已加载映射条目数: {len(variable_name_map)}")

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
        maxconnections=20,        # 最大连接数
        mincached=5,              # 初始化时至少创建的空闲连接
        maxcached=10,             # 连接池中最多空闲连接数
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
            table_name = f"dms_device_workparams_{code}"
            sql = f"SELECT * FROM iplantute.{table_name} ORDER BY ID DESC LIMIT 1"
            cursor.execute(sql)
            result = cursor.fetchone()
            
            elapsed = (time.time() - start_time) * 1000
            
            if result:
                # 将 VariableName 映射为 Name
                mapped = {}
                for k, v in result.items():
                    mapped_key = variable_name_map.get(str(k), k)
                    mapped[mapped_key] = v

                print(f"[查询成功] code={code}, 耗时: {elapsed:.2f}ms")
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
    # 预加载 CSV 映射
    load_variable_name_map()
    # 启动前初始化连接池
    init_connection_pool()
    
    # 使用 Waitress 启动生产级服务器
    print("正在启动 Waitress 服务器...")
    print("服务地址: http://0.0.0.0:5000")
    print("按 Ctrl+C 停止服务")
    try:
        serve(app, host='0.0.0.0', port=5000, threads=16)
    except KeyboardInterrupt:
        print("\n接收到终止信号...")
    finally:
        cleanup()
