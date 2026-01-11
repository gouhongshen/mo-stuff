#!/usr/bin/env python3
"""
MatrixOne Memory Analyzer (Pro)
分析 mo-service 进程的内存使用情况，结合 Go Runtime、MatrixOne Mpool 和 Off-Heap Allocator 统计。
兼容 Linux 和 macOS。
"""

import subprocess
import sys
import re
import platform
import argparse
import time
import os
from urllib.request import urlopen

# --- 颜色与样式配置 ---
class Style:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

def bar(percent, width=20, color=Style.GREEN):
    """生成一个视觉进度条"""
    filled = int(width * percent / 100)
    bar_str = color + "█" * filled + Style.END + "░" * (width - filled)
    return f"[{bar_str}]"

# --- 核心逻辑 ---
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 6060
_MISSING = object()

def parse_args():
    parser = argparse.ArgumentParser(description="MatrixOne Memory Analyzer")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"MO service host (default: {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"MO debug port (default: {DEFAULT_PORT})")
    parser.add_argument("--metrics-port", type=int, default=7001, help="MO metrics/status port (default: 7001)")
    parser.add_argument("--pid", type=int, help="Specific PID to analyze (optional)")
    parser.add_argument("--dump", action="store_true", help="Dump heap profile to file")
    parser.add_argument("--watch", action="store_true", help="Continuously sample and report summary")
    parser.add_argument("--interval", type=int, default=10, help="Seconds between samples in watch mode (default: 10)")
    parser.add_argument("--samples", type=int, default=6, help="Number of samples in watch mode, 0 = infinite (default: 6)")
    parser.add_argument("--oom-threshold", type=float, default=85.0, help="Warn when RSS exceeds this percent of memory limit (default: 85)")
    parser.add_argument("--growth-threshold-mb", type=float, default=512.0, help="Warn if RSS/Heap/OffHeap grows by this many MB in watch window (default: 512)")
    return parser.parse_args()

def get_pids(specific_pid=None):
    if specific_pid: return [specific_pid]
    pids = []
    try:
        result = subprocess.run(['pgrep', '-f', 'mo-service'], capture_output=True, text=True)
        if result.returncode == 0:
            pids = [int(p) for p in result.stdout.strip().split()]
    except: pass
    return list(set(pids))

def _find_listen_inodes(port):
    inodes = set()
    for path in ['/proc/net/tcp', '/proc/net/tcp6']:
        try:
            with open(path, 'r') as f:
                next(f, None)
                for line in f:
                    parts = line.split()
                    if len(parts) < 10:
                        continue
                    local_addr = parts[1]
                    state = parts[3]
                    inode = parts[9]
                    if state != '0A':
                        continue
                    _, port_hex = local_addr.split(':')
                    if port_hex.upper() == f"{port:04X}":
                        inodes.add(inode)
        except:
            continue
    return inodes

def _find_pid_by_inodes(inodes):
    if not inodes:
        return None
    for pid in os.listdir('/proc'):
        if not pid.isdigit():
            continue
        fd_dir = os.path.join('/proc', pid, 'fd')
        try:
            for fd in os.listdir(fd_dir):
                try:
                    target = os.readlink(os.path.join(fd_dir, fd))
                except:
                    continue
                if target.startswith('socket:['):
                    inode = target[8:-1]
                    if inode in inodes:
                        return int(pid)
        except:
            continue
    return None

def find_pid_by_listen_port(port):
    inodes = _find_listen_inodes(port)
    return _find_pid_by_inodes(inodes)

def _read_int_from_file(path):
    try:
        with open(path, 'r') as f:
            val = f.read().strip()
        if val == 'max':
            return None
        return int(val)
    except:
        return None

def _normalize_cgroup_limit(value):
    if value is None:
        return None
    # Treat very large values as "no limit".
    if value > (1 << 60):
        return None
    return value

def get_cgroup_memory_info(pid):
    info = {'limit': None, 'current': None}
    try:
        with open(f'/proc/{pid}/cgroup', 'r') as f:
            lines = f.read().splitlines()
        for line in lines:
            parts = line.split(':', 2)
            if len(parts) != 3:
                continue
            controllers, path = parts[1], parts[2]
            if controllers == '':
                cgpath = os.path.join('/sys/fs/cgroup', path.lstrip('/'))
                info['limit'] = _normalize_cgroup_limit(_read_int_from_file(os.path.join(cgpath, 'memory.max')))
                info['current'] = _read_int_from_file(os.path.join(cgpath, 'memory.current'))
                return info
            if 'memory' in controllers.split(','):
                cgpath = os.path.join('/sys/fs/cgroup/memory', path.lstrip('/'))
                info['limit'] = _normalize_cgroup_limit(_read_int_from_file(os.path.join(cgpath, 'memory.limit_in_bytes')))
                info['current'] = _read_int_from_file(os.path.join(cgpath, 'memory.usage_in_bytes'))
                return info
    except:
        pass
    return info

def get_proc_status_stats(pid):
    stats = {}
    wanted = {'VmRSS', 'VmHWM', 'VmSwap', 'RssAnon', 'RssFile', 'RssShmem'}
    try:
        with open(f'/proc/{pid}/status', 'r') as f:
            for line in f:
                if ':' not in line:
                    continue
                key, rest = line.split(':', 1)
                if key not in wanted:
                    continue
                match = re.search(r'(\d+)\s+kB', rest)
                if match:
                    stats[key] = int(match.group(1)) * 1024
    except:
        pass
    return stats

def get_sys_memory_info(pid):
    stats = {'total_rss': 0, 'details': {}, 'platform': platform.system(), 'status': {}, 'cgroup': {}}
    if platform.system() == 'Darwin':
        try:
            result = subprocess.run(['ps', '-o', 'rss=', '-p', str(pid)], capture_output=True, text=True)
            if result.returncode == 0 and result.stdout.strip():
                stats['total_rss'] = int(result.stdout.strip()) * 1024 
        except: pass
    elif platform.system() == 'Linux':
        try:
            smap_stats = {'go_main_heap': 0, 'go_arena': 0, 'heap': 0, 'stack': 0, 'total_rss': 0}
            current_type = None
            with open(f'/proc/{pid}/smaps', 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if re.match(r'^[0-9a-f]+-[0-9a-f]+', line):
                        parts = line.split()
                        if len(parts) < 5:
                            current_type = None
                            continue
                        addr, perms, device, inode = parts[0], parts[1], parts[3], parts[4]
                        if addr.startswith('c') and 'rw-p' in perms: current_type = 'go_main_heap'
                        elif addr.startswith('7f') and 'rw-p' in perms and device == '00:00' and inode == '0':
                            current_type = 'go_arena' if len(parts) <= 6 else None
                        elif '[heap]' in line: current_type = 'heap'
                        elif '[stack]' in line: current_type = 'stack'
                        else: current_type = None
                        continue
                    if line.startswith('Rss:'):
                        match = re.search(r'Rss:\s+(\d+)\s+kB', line)
                        if match:
                            rss_kb = int(match.group(1))
                            smap_stats['total_rss'] += rss_kb
                            if current_type: smap_stats[current_type] += rss_kb
            stats['total_rss'] = smap_stats['total_rss'] * 1024
            stats['details'] = smap_stats
        except: pass
        try:
            status_stats = get_proc_status_stats(pid)
            if status_stats:
                stats['status'] = status_stats
                if not stats['total_rss'] and status_stats.get('VmRSS'):
                    stats['total_rss'] = status_stats['VmRSS']
        except: pass
        try:
            stats['cgroup'] = get_cgroup_memory_info(pid)
        except: pass
    return stats

def get_url_content(url, timeout=30, silent=False):
    try:
        with urlopen(url, timeout=timeout) as response:
            return response.read().decode('utf-8')
    except Exception as e:
        if not silent:
            print(f"{Style.YELLOW}Warning: Failed to fetch {url}: {e}{Style.END}", file=sys.stderr)
        return None

def get_go_runtime_stats(host, port):
    url = f"http://{host}:{port}/debug/pprof/heap?debug=1"
    content = get_url_content(url)
    if not content: return None
    stats = {}
    patterns = {
        'HeapAlloc': r'#\s*HeapAlloc\s*=\s*(\d+)',
        'HeapSys': r'#\s*HeapSys\s*=\s*(\d+)',
        'HeapIdle': r'#\s*HeapIdle\s*=\s*(\d+)',
        'HeapInuse': r'#\s*HeapInuse\s*=\s*(\d+)',
        'HeapReleased': r'#\s*HeapReleased\s*=\s*(\d+)',
        'StackSys': r'#\s*Stack\s*=\s*(\d+)',
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, content)
        if match: stats[key] = int(match.group(1))
    return stats

def get_goroutine_count(host, port):
    content = get_url_content(f"http://{host}:{port}/debug/pprof/goroutine?debug=1")
    if not content: return None
    match = re.search(r'goroutine profile: total (\d+)', content)
    return int(match.group(1)) if match else None

def dump_heap_profile(host, port, out_dir="."):
    url = f"http://{host}:{port}/debug/pprof/heap"
    ts = time.strftime("%Y%m%d-%H%M%S")
    safe_host = host.replace(":", "_")
    filename = f"heap-{safe_host}-{port}-{ts}.pprof"
    path = os.path.join(out_dir, filename)
    try:
        with urlopen(url, timeout=30) as response, open(path, "wb") as f:
            f.write(response.read())
        print(f"{Style.GREEN}Heap profile dumped: {path}{Style.END}")
        return path
    except Exception as e:
        print(f"{Style.RED}Error: Failed to dump heap profile from {url}: {e}{Style.END}", file=sys.stderr)
        return None

def get_mo_mpool_stats(host, port, content=_MISSING):
    if content is _MISSING:
        content = get_url_content(f"http://{host}:{port}/metrics", silent=True)
    if not content: return None
    mpools = {}
    for line in content.split('\n'):
        if line.startswith('mo_mem_mpool_allocated_size') or line.startswith('mo_mpool_allocated_bytes'):
            try:
                name_match = re.search(r'(?:type|name)=\"([^\"]+)\"', line)
                if name_match:
                    mpools[name_match.group(1)] = mpools.get(name_match.group(1), 0) + int(float(line.split()[-1]))
            except: continue
    return mpools

def get_mo_allocator_stats(host, port, content=_MISSING):
    if content is _MISSING:
        content = get_url_content(f"http://{host}:{port}/metrics", silent=True)
    if not content: return None
    offheap_stats = {}
    legacy_stats = {}
    malloc_gauge_stats = {}
    has_offheap = False
    has_legacy = False
    has_malloc_gauge = False
    for line in content.split('\n'):
        if not line or line.startswith('#'):
            continue
        try:
            if line.startswith('mo_mem_offheap_inuse_bytes'):
                has_offheap = True
                name_match = re.search(r'type=\"([^\"]+)\"', line)
                if name_match:
                    name = name_match.group(1)
                    offheap_stats[name] = offheap_stats.get(name, 0) + int(float(line.split()[-1]))
                continue
            if line.startswith('mo_off_heap_inuse_bytes'):
                has_legacy = True
                name_match = re.search(r'type=\"([^\"]+)\"', line)
                if name_match:
                    name = name_match.group(1)
                    legacy_stats[name] = legacy_stats.get(name, 0) + int(float(line.split()[-1]))
                continue
            if line.startswith('mo_mem_malloc_gauge'):
                has_malloc_gauge = True
                name_match = re.search(r'type=\"([^\"]+)\"', line)
                if name_match:
                    name = name_match.group(1)
                    if 'objects' in name or 'inuse' not in name:
                        continue
                    name = name.replace('-inuse', '')
                    malloc_gauge_stats[name] = malloc_gauge_stats.get(name, 0) + int(float(line.split()[-1]))
        except: pass
    if has_offheap and offheap_stats:
        return offheap_stats
    if has_legacy and legacy_stats:
        return legacy_stats
    if has_malloc_gauge and malloc_gauge_stats:
        return malloc_gauge_stats
    return None

def collect_snapshot(pid, args):
    sys_stats = get_sys_memory_info(pid)
    go_stats = get_go_runtime_stats(args.host, args.port)
    metrics_text = get_url_content(f"http://{args.host}:{args.metrics_port}/metrics", silent=True)
    mpool_stats = get_mo_mpool_stats(args.host, args.metrics_port, content=metrics_text)
    alloc_stats = get_mo_allocator_stats(args.host, args.metrics_port, content=metrics_text)
    if metrics_text is None and args.port != args.metrics_port:
        metrics_text = get_url_content(f"http://{args.host}:{args.port}/metrics", silent=True)
        mpool_stats = get_mo_mpool_stats(args.host, args.port, content=metrics_text)
        alloc_stats = get_mo_allocator_stats(args.host, args.port, content=metrics_text)
    goroutine_count = get_goroutine_count(args.host, args.port)
    return {
        'timestamp': time.time(),
        'sys_stats': sys_stats,
        'go_stats': go_stats,
        'mpool_stats': mpool_stats,
        'alloc_stats': alloc_stats,
        'goroutine_count': goroutine_count,
    }

def format_bytes(bytes_val):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024: return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.2f} PB"

def _snapshot_values(snapshot):
    sys_stats = snapshot['sys_stats']
    go_stats = snapshot['go_stats'] or {}
    alloc_stats = snapshot['alloc_stats'] or {}
    rss = sys_stats['total_rss']
    heap_inuse = go_stats.get('HeapInuse', 0)
    heap_alloc = go_stats.get('HeapAlloc', 0)
    off_heap = sum(alloc_stats.values()) if alloc_stats else 0
    return rss, heap_inuse, heap_alloc, off_heap

def _trend_ratio(values):
    if len(values) < 2:
        return 0
    increases = sum(1 for i in range(1, len(values)) if values[i] >= values[i - 1])
    return increases / (len(values) - 1)

def print_watch_header():
    print(f"{Style.BOLD}Time     PID     RSS       HeapInuse   HeapAlloc   OffHeap     Goroutines{Style.END}")

def print_watch_line(pid, snapshot, oom_threshold):
    ts = time.strftime("%H:%M:%S", time.localtime(snapshot['timestamp']))
    rss, heap_inuse, heap_alloc, off_heap = _snapshot_values(snapshot)
    goroutines = snapshot.get('goroutine_count') or 0
    line = (f"{ts}  {pid:<6} {format_bytes(rss):>10} "
            f"{format_bytes(heap_inuse):>10} {format_bytes(heap_alloc):>10} "
            f"{format_bytes(off_heap):>10} {goroutines:>10}")
    cgroup = snapshot['sys_stats'].get('cgroup') or {}
    if cgroup.get('limit') and oom_threshold:
        limit = cgroup['limit']
        rss_pct = (rss / limit * 100) if limit else 0
        if rss_pct >= oom_threshold:
            line = f"{Style.RED}{line}  OOM {rss_pct:.1f}%{Style.END}"
    print(line)

def analyze_growth(pid, history, threshold_bytes):
    if len(history) < 2:
        return
    rss_values = [h[0] for h in history]
    heap_alloc_values = [h[2] for h in history]
    off_heap_values = [h[3] for h in history]
    checks = [
        ('RSS', rss_values),
        ('HeapAlloc', heap_alloc_values),
        ('OffHeap', off_heap_values),
    ]
    for name, values in checks:
        delta = values[-1] - values[0]
        if delta <= threshold_bytes:
            continue
        if _trend_ratio(values) >= 0.7:
            print(f"{Style.YELLOW}⚠️  PID {pid} {name} 持续增长: {format_bytes(delta)}{Style.END}")

def run_watch(pids, args):
    history = {pid: [] for pid in pids}
    print_watch_header()
    samples_taken = 0
    while args.samples == 0 or samples_taken < args.samples:
        for pid in pids:
            snapshot = collect_snapshot(pid, args)
            history[pid].append(_snapshot_values(snapshot))
            print_watch_line(pid, snapshot, args.oom_threshold)
        samples_taken += 1
        if args.samples == 0 or samples_taken < args.samples:
            time.sleep(max(1, args.interval))
    threshold_bytes = int(args.growth_threshold_mb * 1024 * 1024)
    for pid, values in history.items():
        analyze_growth(pid, values, threshold_bytes)

def print_report(pid, sys_stats, go_stats, mpool_stats, alloc_stats, goroutine_count, oom_threshold):
    # --- Header ---
    print(f"{Style.BOLD}{Style.HEADER}┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓{Style.END}")
    print(f"{Style.BOLD}{Style.HEADER}┃ MatrixOne Memory Analysis | PID: {pid:<8} | OS: {sys_stats['platform']:<10} ┃{Style.END}")
    print(f"{Style.BOLD}{Style.HEADER}┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛{Style.END}")

    # 1. OS View
    print(f"\n{Style.BOLD}{Style.BLUE} 📊 [系统内存 (OS View)]{Style.END}")
    rss = sys_stats['total_rss']
    print(f"  总 RSS (物理内存):     {Style.BOLD}{format_bytes(rss):>12}{Style.END}  {bar(100, color=Style.BLUE)}")
    
    if sys_stats['details']:
        d = sys_stats['details']
        print(f"  ├─ Go 主堆 (c000):     {format_bytes(d['go_main_heap'] * 1024):>12}")
        print(f"  ├─ Go Arena (7f...):   {format_bytes(d['go_arena'] * 1024):>12}")
        print(f"  └─ CGO Heap (原生):    {format_bytes(d['heap'] * 1024):>12}")
    status = sys_stats.get('status') or {}
    if status:
        if status.get('RssAnon'):
            print(f"  ├─ RSS 匿名页 (Anon):  {format_bytes(status['RssAnon']):>12}")
        if status.get('RssFile'):
            print(f"  ├─ RSS 文件页 (File):  {format_bytes(status['RssFile']):>12}")
        if status.get('RssShmem'):
            print(f"  ├─ RSS 共享页 (Shmem): {format_bytes(status['RssShmem']):>12}")
        if status.get('VmSwap'):
            print(f"  └─ Swap 使用:          {format_bytes(status['VmSwap']):>12}")
    cgroup = sys_stats.get('cgroup') or {}
    if cgroup.get('limit'):
        limit = cgroup['limit']
        current = cgroup.get('current')
        pct = (rss / limit * 100) if limit else 0
        if current:
            print(f"  └─ Cgroup 用量/上限:   {format_bytes(current):>12} / {format_bytes(limit):>12} ({pct:.1f}%)")
        else:
            print(f"  └─ Cgroup 上限:        {format_bytes(limit):>12} ({pct:.1f}%)")

    # 2. Go Runtime
    if go_stats:
        print(f"\n{Style.BOLD}{Style.CYAN} 🔷 [Go Runtime (Pprof View)]{Style.END}")
        hs = go_stats.get('HeapSys', 0)
        hi = go_stats.get('HeapInuse', 0)
        ha = go_stats.get('HeapAlloc', 0)
        hl = go_stats.get('HeapReleased', 0)
        hd = go_stats.get('HeapIdle', 0)

        # HeapSys: total bytes of heap mapped from OS (inuse + idle).
        # HeapInuse: bytes in spans marked in-use (live + internal overhead).
        # HeapAlloc: live objects only (what GC considers reachable).
        # HeapReleased: idle heap returned back to OS.
        print(f"  HeapSys (向OS申请):    {format_bytes(hs):>12}")
        print(f"  HeapInuse (正在使用):  {Style.BOLD}{format_bytes(hi):>12}{Style.END}  {bar(hi/hs*100 if hs else 0, color=Style.CYAN)}")
        print(f"  HeapAlloc (存活对象):  {format_bytes(ha):>12}")
        print(f"  HeapReleased (已还OS): {format_bytes(hl):>12}")
        
        # 碎片率
        if hi > 0:
            internal_frag = hi - ha
            frag_pct = (internal_frag / hi) * 100
            color = Style.YELLOW if frag_pct > 40 else Style.GREEN
            print(f"  堆内碎片 (Inuse-Alloc):{color}{format_bytes(internal_frag):>12}  ({frag_pct:.1f}%){Style.END}")
            if frag_pct > 40:
                print(f"    {Style.YELLOW}↳ ⚠️ 提示: 碎片较高，通常由大量小对象引起{Style.END}")

    # 3. Off-Heap
    if alloc_stats:
        print(f"\n{Style.BOLD}{Style.GREEN} 🍀 [堆外内存 (Off-Heap Mmap)]{Style.END}")
        total_off = sum(alloc_stats.values())
        print(f"  Total Off-Heap:        {Style.BOLD}{format_bytes(total_off):>12}{Style.END}")
        for k, v in sorted(alloc_stats.items(), key=lambda x:x[1], reverse=True):
            if v > 1024*1024:
                print(f"  ├─ {k:<20}: {format_bytes(v):>12}")

    # 4. Logical View
    if mpool_stats:
        print(f"\n{Style.BOLD}{Style.BLUE} 🧩 [内存池 (Logical Mpool)]{Style.END}")
        total_mp = sum(mpool_stats.values())
        print(f"  Total Mpool Alloc:     {format_bytes(total_mp):>12}")
        for k, v in sorted(mpool_stats.items(), key=lambda x:x[1], reverse=True)[:5]:
            print(f"  ├─ {k:<20}: {format_bytes(v):>12}")

    # 5. Analysis Summary
    print(f"\n{Style.BOLD}{Style.UNDERLINE} 📋 [分析总结]{Style.END}")
    if not go_stats or go_stats.get('HeapSys', 0) == 0:
        print(f"  {Style.RED}❌ 无法解析 Pprof 内存统计，请检查配置或输出格式。{Style.END}")
    else:
        # 计算理论偏差
        off_heap = sum(alloc_stats.values()) if alloc_stats else 0
        hs = go_stats.get('HeapSys', 0)
        hl = go_stats.get('HeapReleased', 0)
        hd = go_stats.get('HeapIdle', 0)
        heap_mapped = hs - hl
        expected = heap_mapped + off_heap if sys_stats['platform'] == 'Linux' else hs + off_heap
        diff = rss - expected
        
        if abs(diff) > 2 * 1024*1024*1024:
            if diff > 0:
                print(f"  {Style.YELLOW}⚠️  发现无法解释的内存占用: {format_bytes(diff)}{Style.END}")
                print(f"     公式: RSS - ((HeapSys - HeapReleased) + OffHeap)")
                if alloc_stats is None:
                    print(f"     {Style.CYAN}❓ 建议: Metrics 接口未响应，这部分可能正是 Memory Cache。{Style.END}")
                else:
                    print(f"     {Style.CYAN}💡 可能原因: CGO 隐藏分配、操作系统 Page Cache 或 Go 内存释放延迟。{Style.END}")
            else:
                print(f"  {Style.YELLOW}⚠️  统计值明显高于 RSS: {format_bytes(-diff)}{Style.END}")
                print(f"     公式: RSS - ((HeapSys - HeapReleased) + OffHeap)")
                print(f"     {Style.CYAN}💡 可能原因: HeapSys 为虚拟映射/未驻留，或 Metrics 为累计值/统计重复(多进程共用端口)/Pprof 与 PID 不匹配。{Style.END}")
        else:
            print(f"  {Style.GREEN}✓ 内存账目吻合，未发现明显泄漏。{Style.END}")
        if hi and heap_mapped > (hi * 2) and heap_mapped > 4 * 1024 * 1024 * 1024:
            print(f"  {Style.YELLOW}⚠️  Go Heap 映射远大于 Inuse: {format_bytes(heap_mapped)} vs {format_bytes(hi)}{Style.END}")
            print(f"     {Style.CYAN}💡 可能为历史高水位/未及时释放，建议观察 GC 释放情况。{Style.END}")
        if hd and hl and hd > hl:
            idle_unreleased = hd - hl
            if idle_unreleased > 2 * 1024 * 1024 * 1024:
                print(f"  {Style.YELLOW}⚠️  Go HeapIdle 未释放较多: {format_bytes(idle_unreleased)}{Style.END}")
                print(f"     {Style.CYAN}💡 可能导致 RSS 偏高，可观察 GC/FreeOSMemory 行为。{Style.END}")
    status = sys_stats.get('status') or {}
    if status.get('VmSwap'):
        print(f"  {Style.YELLOW}⚠️  检测到 Swap 使用: {format_bytes(status['VmSwap'])}{Style.END}")
    cgroup = sys_stats.get('cgroup') or {}
    if cgroup.get('limit') and oom_threshold:
        limit = cgroup['limit']
        rss_pct = (rss / limit * 100) if limit else 0
        if rss_pct >= oom_threshold:
            print(f"  {Style.RED}🚨  接近 OOM: RSS {rss_pct:.1f}% / 阈值 {oom_threshold:.1f}%{Style.END}")
    
    if goroutine_count:
        print(f"\n  {Style.BOLD}Goroutines:{Style.END} {goroutine_count}")

def main():
    args = parse_args()
    pids = get_pids(args.pid)
    if not pids:
        print(f"{Style.RED}Error: mo-service process not found.{Style.END}")
        sys.exit(1)
    
    print(f"{Style.BLUE}Target: {args.host}:{args.port} (Pprof), {args.metrics_port} (Metrics){Style.END}")
    selected_pids = pids
    if len(pids) > 1 and not args.pid:
        resolved_pid = None
        if platform.system() == 'Linux':
            resolved_pid = find_pid_by_listen_port(args.port)
        if resolved_pid and resolved_pid in pids:
            selected_pids = [resolved_pid]
            print(f"{Style.YELLOW}Warning: Multiple mo-service PIDs detected; using PID {resolved_pid} mapped to port {args.port}.{Style.END}")
        else:
            print(f"{Style.YELLOW}Warning: Multiple mo-service PIDs detected; Pprof/Metrics reflect a single endpoint and may not match each PID. Use --pid to choose.{Style.END}")

    if args.watch:
        run_watch(selected_pids, args)
        return

    for pid in selected_pids:
        snapshot = collect_snapshot(pid, args)
        print_report(
            pid,
            snapshot['sys_stats'],
            snapshot['go_stats'],
            snapshot['mpool_stats'],
            snapshot['alloc_stats'],
            snapshot['goroutine_count'],
            args.oom_threshold,
        )
        if args.dump:
            dump_heap_profile(args.host, args.port)

if __name__ == '__main__':
    main()
