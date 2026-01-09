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
from collections import defaultdict
from urllib.request import urlopen
from urllib.error import URLError

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

def parse_args():
    parser = argparse.ArgumentParser(description="MatrixOne Memory Analyzer")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"MO service host (default: {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"MO debug port (default: {DEFAULT_PORT})")
    parser.add_argument("--metrics-port", type=int, default=7001, help="MO metrics/status port (default: 7001)")
    parser.add_argument("--pid", type=int, help="Specific PID to analyze (optional)")
    parser.add_argument("--dump", action="store_true", help="Dump heap profile to file")
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

def get_sys_memory_info(pid):
    stats = {'total_rss': 0, 'details': {}, 'platform': platform.system()}
    if platform.system() == 'Darwin':
        try:
            result = subprocess.run(['ps', '-o', 'rss=', '-p', str(pid)], capture_output=True, text=True)
            if result.returncode == 0 and result.stdout.strip():
                stats['total_rss'] = int(result.stdout.strip()) * 1024 
        except: pass
    elif platform.system() == 'Linux':
        try:
            with open(f'/proc/{pid}/smaps', 'r') as f:
                content = f.read()
            smap_stats = {'go_main_heap': 0, 'go_arena': 0, 'heap': 0, 'stack': 0, 'total_rss': 0}
            current_type = None
            for line in content.split('\n'):
                if re.match(r'^[0-9a-f]+-[0-9a-f]+', line):
                    parts = line.split()
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

def get_mo_mpool_stats(host, port):
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

def get_mo_allocator_stats(host, port):
    content = get_url_content(f"http://{host}:{port}/metrics", silent=True)
    if not content: return None
    stats = {}
    found = False
    for line in content.split('\n'):
        if line.startswith('mo_mem_offheap_inuse_bytes') or line.startswith('mo_off_heap_inuse_bytes') or \
           (line.startswith('mo_mem_malloc_') and 'gauge' in line):
            try:
                name_match = re.search(r'type=\"([^\"]+)\"', line)
                if name_match:
                    name = name_match.group(1).replace('-inuse', '')
                    stats[name] = stats.get(name, 0) + int(float(line.split()[-1]))
                    found = True
            except: pass
    return stats

def format_bytes(bytes_val):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024: return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.2f} PB"

def print_report(pid, sys_stats, go_stats, mpool_stats, alloc_stats, goroutine_count):
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

    # 2. Go Runtime
    if go_stats:
        print(f"\n{Style.BOLD}{Style.CYAN} 🔷 [Go Runtime (Pprof View)]{Style.END}")
        hs = go_stats.get('HeapSys', 0)
        hi = go_stats.get('HeapInuse', 0)
        ha = go_stats.get('HeapAlloc', 0)
        hl = go_stats.get('HeapReleased', 0)
        hd = go_stats.get('HeapIdle', 0)

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
    if not go_stats:
        print(f"  {Style.RED}❌ 无法连接 Pprof 端口，请检查配置。{Style.END}")
    else:
        # 计算理论偏差
        off_heap = sum(alloc_stats.values()) if alloc_stats else 0
        expected = (go_stats['HeapSys'] - go_stats['HeapReleased']) + off_heap if sys_stats['platform'] == 'Linux' else go_stats['HeapSys'] + off_heap
        diff = rss - expected
        
        if diff > 2 * 1024*1024*1024:
            print(f"  {Style.YELLOW}⚠️  发现无法解释的内存占用: {format_bytes(diff)}{Style.END}")
            print(f"     公式: RSS - ((HeapSys - HeapReleased) + OffHeap)")
            if not alloc_stats:
                print(f"     {Style.CYAN}❓ 建议: Metrics 接口未响应，这部分可能正是 Memory Cache。{Style.END}")
            else:
                print(f"     {Style.CYAN}💡 可能原因: CGO 隐藏分配、操作系统 Page Cache 或 Go 内存释放延迟。{Style.END}")
        else:
            print(f"  {Style.GREEN}✓ 内存账目吻合，未发现明显泄漏。{Style.END}")
    
    if goroutine_count:
        print(f"\n  {Style.BOLD}Goroutines:{Style.END} {goroutine_count}")

def main():
    args = parse_args()
    pids = get_pids(args.pid)
    if not pids:
        print(f"{Style.RED}Error: mo-service process not found.{Style.END}")
        sys.exit(1)
    
    print(f"{Style.BLUE}Target: {args.host}:{args.port} (Pprof), {args.metrics_port} (Metrics){Style.END}")
    
    for pid in pids:
        sys_stats = get_sys_memory_info(pid)
        go_stats = get_go_runtime_stats(args.host, args.port)
        mpool_stats = get_mo_mpool_stats(args.host, args.metrics_port) or (get_mo_mpool_stats(args.host, args.port) if args.port != args.metrics_port else None)
        alloc_stats = get_mo_allocator_stats(args.host, args.metrics_port) or (get_mo_allocator_stats(args.host, args.port) if args.port != args.metrics_port else None)
        goroutine_count = get_goroutine_count(args.host, args.port)
        
        print_report(pid, sys_stats, go_stats, mpool_stats, alloc_stats, goroutine_count)
        
        if args.dump:
            dump_heap_profile(args.host, args.port)

if __name__ == '__main__':
    main()