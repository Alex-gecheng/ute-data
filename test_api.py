import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置
BASE_URL = "http://localhost:5000"
CODE = "07_4_3mz2010"
REQUESTS_PER_API = 10

# 测试数据
test_data = {
    'code': CODE
}

def test_api(api_name, url, request_num):
    """发送单个请求"""
    try:
        start = time.time()
        response = requests.post(url, json=test_data, timeout=30)
        elapsed = time.time() - start
        
        if response.status_code == 200:
            data = response.json()
            return {
                'api': api_name,
                'request_num': request_num,
                'status': 'success',
                'status_code': response.status_code,
                'elapsed': elapsed,
                'data_fields': len(data.get('data', {})) if data.get('data') else 0,
                'api_elapsed_ms': data.get('elapsed_ms', 'N/A')
            }
        else:
            return {
                'api': api_name,
                'request_num': request_num,
                'status': 'error',
                'status_code': response.status_code,
                'elapsed': elapsed,
                'error': response.text
            }
    except Exception as e:
        return {
            'api': api_name,
            'request_num': request_num,
            'status': 'exception',
            'elapsed': time.time() - start,
            'error': str(e)
        }

def main():
    """并发测试"""
    print("=" * 80)
    print("API 压力测试")
    print("=" * 80)
    print(f"目标 URL: {BASE_URL}")
    print(f"Code: {CODE}")
    print(f"每个接口请求数: {REQUESTS_PER_API}")
    print(f"总请求数: {REQUESTS_PER_API * 2}")
    print("=" * 80)
    print()
    
    # 构建请求列表
    tasks = []
    
    # /api/process_data 接口
    for i in range(1, REQUESTS_PER_API + 1):
        tasks.append(('process_data', f"{BASE_URL}/api/process_data", i))
    
    # /api/efficiency_data 接口
    for i in range(1, REQUESTS_PER_API + 1):
        tasks.append(('efficiency_data', f"{BASE_URL}/api/efficiency_data", i))
    
    # 并发执行
    results = []
    total_start = time.time()
    
    print(f"开始发送 {len(tasks)} 个请求...\n")
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(test_api, api_name, url, req_num): (api_name, req_num)
            for api_name, url, req_num in tasks
        }
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            api_name, req_num = futures[future]
            try:
                result = future.result()
                results.append(result)
                print(f"[{completed}/{len(tasks)}] {api_name} #{req_num}: {result['status']} ({result['elapsed']:.3f}s)")
            except Exception as e:
                print(f"[{completed}/{len(tasks)}] {api_name} #{req_num}: 执行失败 - {str(e)}")
    
    total_elapsed = time.time() - total_start
    
    # 统计结果
    print("\n" + "=" * 80)
    print("测试结果统计")
    print("=" * 80)
    
    process_results = [r for r in results if r['api'] == 'process_data']
    efficiency_results = [r for r in results if r['api'] == 'efficiency_data']
    
    successful = sum(1 for r in results if r['status'] == 'success')
    failed = sum(1 for r in results if r['status'] != 'success')
    
    print(f"总请求数: {len(results)}")
    print(f"成功: {successful}")
    print(f"失败: {failed}")
    print(f"总耗时: {total_elapsed:.3f}s")
    print()
    
    if process_results:
        print("process_data 接口结果:")
        print(f"  请求数: {len(process_results)}")
        process_success = [r for r in process_results if r['status'] == 'success']
        if process_success:
            process_elapsed = [r['elapsed'] for r in process_success]
            process_api_elapsed = [float(r['api_elapsed_ms']) for r in process_success if isinstance(r.get('api_elapsed_ms'), (int, float)) or (isinstance(r.get('api_elapsed_ms'), str) and r.get('api_elapsed_ms') != 'N/A')]
            print(f"  成功: {len(process_success)}")
            print(f"  平均响应时间: {sum(process_elapsed) / len(process_elapsed):.3f}s")
            print(f"  最快: {min(process_elapsed):.3f}s")
            print(f"  最慢: {max(process_elapsed):.3f}s")
            if process_api_elapsed:
                print(f"  平均查询时间(API内部): {sum(process_api_elapsed) / len(process_api_elapsed):.2f}ms")
            if process_success[0].get('data_fields'):
                print(f"  返回字段数: {process_success[0]['data_fields']}")
    
    print()
    
    if efficiency_results:
        print("efficiency_data 接口结果:")
        print(f"  请求数: {len(efficiency_results)}")
        efficiency_success = [r for r in efficiency_results if r['status'] == 'success']
        if efficiency_success:
            efficiency_elapsed = [r['elapsed'] for r in efficiency_success]
            efficiency_api_elapsed = [float(r['api_elapsed_ms']) for r in efficiency_success if isinstance(r.get('api_elapsed_ms'), (int, float)) or (isinstance(r.get('api_elapsed_ms'), str) and r.get('api_elapsed_ms') != 'N/A')]
            print(f"  成功: {len(efficiency_success)}")
            print(f"  平均响应时间: {sum(efficiency_elapsed) / len(efficiency_elapsed):.3f}s")
            print(f"  最快: {min(efficiency_elapsed):.3f}s")
            print(f"  最慢: {max(efficiency_elapsed):.3f}s")
            if efficiency_api_elapsed:
                print(f"  平均查询时间(API内部): {sum(efficiency_api_elapsed) / len(efficiency_api_elapsed):.2f}ms")
            if efficiency_success[0].get('data_fields'):
                print(f"  返回字段数: {efficiency_success[0]['data_fields']}")
    
    print("\n" + "=" * 80)
    
    # 详细结果
    print("\n详细请求结果:")
    print("-" * 80)
    for i, result in enumerate(sorted(results, key=lambda x: (x['api'], x['request_num'])), 1):
        if result['status'] == 'success':
            print(f"{i:2}. {result['api']:15} #{result['request_num']:2}: {result['elapsed']:.3f}s | "
                  f"API耗时: {result['api_elapsed_ms']}ms | 字段数: {result['data_fields']}")
        else:
            print(f"{i:2}. {result['api']:15} #{result['request_num']:2}: {result['status']:10} | {result.get('error', '')[:50]}")

if __name__ == '__main__':
    main()
