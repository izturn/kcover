import itertools
import subprocess
import statistics
import sys
import concurrent.futures
from datetime import datetime
import csv

# used for MACA version >=2.33

def read_ips(filename):
    with open(filename, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def parse_output_old(output):
    for line in output.split('\n'):
        if line.startswith('#') and "1073741824" in line:
            parts = line.strip().split()
            return float(parts[6]) if len(parts) >= 9 else None
    return None

def parse_output(output):
    for line in output.split('\n'):
        if not line.startswith('#'):
            if "1073741824" in line and "nThread" not in line:
                parts = line.strip().split()
                if len(parts) >= 8:
                    try:
                        if "sum" in line:
                            return float(parts[7])
                        else:
                            return float(parts[6])
                    except (IndexError, ValueError):
                        continue
    return None

def write_log(group, result, value):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("group_test.log", "a") as f:
        f.write(f"\n[{timestamp}] {'+'.join(group)}\n")
        f.write(f"命令输出:\n{result.stdout}")
        if result.stderr:
            f.write(f"\n错误信息:\n{result.stderr}")
        f.write(f"\n返回值: {result.returncode} | 解析值: {value}\n")
        f.write("-"*60)

def batch_generator(all_groups):
    remaining = all_groups.copy()
    while remaining:
        batch = []
        used_ips = set()
        for group in list(remaining):
            if not any(ip in used_ips for ip in group):
                batch.append(group)
                used_ips.update(group)
                remaining.remove(group)
        yield batch

def run_test(group, num_procs, test_bin):
    try:
        host_arg = ",".join([f"{ip}:8" for ip in group])
         
        cmd = [
            '/opt/maca/ompi/bin/mpirun',
            '-np', str(num_procs),
            '--allow-run-as-root',
            '-host', host_arg,
            '-mca', 'btl_tcp_if_include', '10.200.0.0/16',
            '-mca', 'oob_tcp_if_include', '10.200.0.0/16',
            '-mca', 'pml', '^ucx',
            '-mca', 'osc', '^ucx',
            '-mca', 'btl', '^openib',
            '-x', 'MX_TRACER_ENABLED_MCPTI=OFF',
            '-x', 'MXLOG_LEVEL=error',
            '-x', 'MCCL_IB_HCA=mlx5_bond_2,mlx5_bond_3,mlx5_bond_4,mlx5_bond_5',
            '-x', 'MCCL_SOCKET_IFNAME=bond0',
            '-x', 'LD_LIBRARY_PATH=/opt/maca/lib:/opt/maca/ompi/lib',
            f'/opt/maca/samples/mccl_tests/perf/mccl_perf/{test_bin}',
            '-b', '1K', '-e', '1G', '-d', 'float', '-f', '2', '-g', '1', '-n', '1'
        ]

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10
        )
        value = parse_output(result.stdout)
        write_log(group, result, value)
        return group, value
    except subprocess.TimeoutExpired:
        print(f"{'+'.join(group)} 执行超时")
        return group, None
    except Exception as e:
        print(f"{'+'.join(group)} 执行异常: {str(e)}")
        return group, None

def main(ip_file, threshold, test_bin):
    # 固定每组 IP 数量为 2
    group_size = 2
    max_batches = 5  # 只跑前 5 个批次

    ips = read_ips(ip_file)
    if len(ips) < group_size:
        print(f"需要至少{group_size}个IP地址")
        return

    all_groups = list(itertools.combinations(ips, group_size))
    results = []

    #print(f"总测试组数: {len(all_groups)}")
    #print(f"每组IP数: {group_size} | 每节点进程数: 8 | 总进程数: {group_size*8}")
    print(f"成功阈值: {threshold}")
    print(f"测试工具: {test_bin}")
    #print(f"开始分批执行（最多 {max_batches} 批次）...")

    # 用于慢节点检测：记录每个批次中出现于任一失败组的 IP 集合
    batch_failed_ips_list = []

    batch_count = 0
    for batch_idx, batch in enumerate(batch_generator(all_groups), 1):
        if batch_count >= max_batches:
            break
        batch_count += 1

        print(f"\n=== 批次 {batch_idx} (包含 {len(batch)} 组) ===")

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(batch)) as executor:
            futures = {executor.submit(run_test, group, group_size*8, test_bin): group for group in batch}

            batch_results = []
            for future in concurrent.futures.as_completed(futures):
                group = futures[future]
                try:
                    group, value = future.result()
                    results.append((group, value))
                    batch_results.append((group, value))
                    status = "✅" if value is not None and value >= threshold else "❌"
                    print(f"{status} {'+'.join(group):<40} : {value if value is not None else '执行失败'}")
                except Exception as e:
                    print(f"⚠️ {'+'.join(group)} 处理异常: {str(e)}")
                    batch_results.append((group, None))
                    results.append((group, None))

        # 统计本批次失败的 IP（任一失败组中包含的 IP 都视为本批次失败）
        failed_ips = set()
        for group, value in batch_results:
            if value is None or value < threshold:
                failed_ips.update(group)
        batch_failed_ips_list.append(failed_ips)

        print(f"\n批次 {batch_idx} 失败 IP 列表:")
        if failed_ips:
            for ip in sorted(failed_ips):
                print(f"  - {ip}")
        else:
            print("  无")

    # 运行结束（或达到 max_batches）
    print("\n测试完成，生成统计信息:")

    total_executed_groups = len(results)
    success = []
    failure = []
    for group, value in results:
        if value is not None and value >= threshold:
            success.append((group, value))
        else:
            failure.append((group, value))

    total = len(all_groups)
    success_count = len(success)
    failure_count = len(failure)

    print(f"已执行组数: {total_executed_groups}（原总组数 {total}）")
    print(f"成功组数: {success_count}/{total_executed_groups} ({(success_count/total_executed_groups*100) if total_executed_groups else 0:.1f}%)")
    print(f"失败组数: {failure_count}/{total_executed_groups} ({(failure_count/total_executed_groups*100) if total_executed_groups else 0:.1f}%)")

    no_value = sum(1 for _, v in failure if v is None)
    below_threshold = failure_count - no_value
    print("\n失败原因细分:")
    print(f"  - 执行失败: {no_value} 组")
    print(f"  - 值低于阈值: {below_threshold} 组")

    if success:
        values = [v for _, v in success]
        print("\n成功组统计:")
        print(f"平均值  : {statistics.mean(values):.2f}")
        print(f"中位数  : {statistics.median(values):.2f}")
        print(f"最小值  : {min(values):.2f}")
        print(f"最大值  : {max(values):.2f}")
        if len(values) > 1:
            print(f"标准差  : {statistics.stdev(values):.2f}")

    def write_csv(filename, data):
        headers = [f'ip{i+1}' for i in range(group_size)] + ['value']
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for group, value in data:
                writer.writerow(list(group) + [value])

    if success:
        write_csv('success.csv', success)
        print("\n成功结果已保存至 success.csv")
    if failure:
        write_csv('failure.csv', failure)
        print("失败结果已保存至 failure.csv")

    # 慢节点判定：取各批次失败 IP 集合的交集（只在我们执行过的批次上判定）
    print("\n========== 慢节点检测结果（出现在每个已执行批次的失败集合中） ==========")
    if batch_failed_ips_list:
        # 交集计算：只有在所有已执行批次中都出现的 IP 才被判为慢节点
        slow_nodes = set.intersection(*batch_failed_ips_list) if all(batch_failed_ips_list) else set()
    else:
        slow_nodes = set()

    if slow_nodes:
        print("检测到慢节点:")
        for ip in sorted(slow_nodes):
            print(f"  ❌ {ip}")
    else:
        print("未检测到慢节点")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("使用方法: python script.py <IP列表文件> <成功阈值> <测试工具>")
        print("示例: python script.py ips.txt 50.0 all_gather_perf")
        sys.exit(1)

    try:
        threshold = float(sys.argv[2])
        test_bin = sys.argv[3]
    except ValueError:
        print("阈值参数错误")
        sys.exit(1)

    main(sys.argv[1], threshold, test_bin)
