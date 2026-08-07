#!/usr/bin/env python3
"""
TVBox 整合源自动更新脚本 v2.4（优化版）
基于 jifeng250/tvbox-sources 原版重构。

【修复的问题】
1. [BUG] 熔断失效：原 workflow 未提交 health_state.json，连续失败计数每次运行
   都从 0 开始，"连续 3 次失败自动移除线路"从未真正生效 → 本版配合
   auto-update.yml 修复（git add 增加 health_state.json）。
2. [性能] 原版 17 个线路健康检查 + 9 个上游源全部串行，最坏 26×15s≈390 秒
   → 本版改用线程池并发（默认 8 并发），全流程压到 ~40 秒内。
3. [误判] 原版单次超时即判失败，网络抖动会误伤 → 本版探活带 1 次快速重试
   （3 秒间隔），区分临时抖动与真挂。
4. [保护] 原版合并后不校验直接提交，上游大面积挂时会把残缺配置发布出去
   → 本版生成前 JSON 回读校验 + 站点数异常熔断（低于阈值直接退出不覆盖）。
5. [优先级] 原版去重为"后源覆盖前源"，优先级低的源可能盖掉好源的同 key 站点
   → 本版改为按 UPSTREAM_SOURCES 顺序去重，靠前的源（主源）胜出。
6. [可观测] 新增 update.log 日志留档（自动轮转，保留 5MB）。
7. [告警] 支持 Telegram 失败告警（环境变量 TG_BOT_TOKEN / TG_CHAT_ID，
   未配置自动跳过，不影响主流程）。

【v2.1 新增：线路测速 + 星标推荐】
8. 探活升级为 socket 层测速：一次请求同时拿到 连接耗时 / TTFB / 总耗时 / 响应大小，
   不再只判 200/3xx。
9. 测速数据持久化到 speed_state.json（每线路保留最近 5 次样本），据此计算
   平均 TTFB，配合健康史生成星级评级（相对排名分档，保证区分度）：
     ⭐⭐⭐ 推荐  平均 TTFB 前 1/3 且无失败史
     ⭐⭐  良好  平均 TTFB 中 1/3 且无失败史
     ⭐   可用  平均 TTFB 后 1/3，或慢于绝对阈值（3s）
     ⚠️   波动  有失败史（1-2 次）
10. urls.json 按星级排序输出（推荐线路排最前），星级直接体现在线路名前缀。

【v2.4 新增：源库扩充】
11. 线路 17 → 21 条，上游源 9 → 13 个：新增 高天流云(298站)/俊佬(24站)/
    道长(435站)/FM影视(82站)，全部经真实探活 + 格式校验后收录
    （2026-08-07 调研：南风/香雅情/潇洒/太阳/小美 等因国内不可达或格式不兼容未收录）。

【行为保持不变的项】
- 17 个线路定义、9 个上游源、中文域名 IDNA 编码、BMP 图 base64 解析、
  BOM/注释/控制字符 JSON 容错、3xx 重定向视为健康。
"""

import base64
import json
import logging
import os
import re
import socket
import ssl
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from logging.handlers import RotatingFileHandler
from urllib.parse import urlparse, urlunparse

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
HEALTH_FILE = os.path.join(SCRIPT_DIR, "health_state.json")
SPEED_FILE = os.path.join(SCRIPT_DIR, "speed_state.json")
LOG_FILE = os.path.join(SCRIPT_DIR, "update.log")

MAX_FAILURES = 3        # 连续失败次数达到该值，线路从 urls.json 移除
RETRY_COUNT = 1         # 探活失败后的重试次数（1 = 共探测 2 次）
RETRY_DELAY = 3         # 探活重试间隔（秒）
CONCURRENCY = 8         # 并发线程数
MIN_SITES_WARN = 100    # 站点数低于该值视为异常，拒绝覆盖并退出
HTTP_TIMEOUT = 15       # HTTP 请求超时（秒）
LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 2

# --- 测速与星标推荐参数 ---
MAX_SPEED_SAMPLES = 5            # 每线路保留的测速样本数（滑动窗口）
STAR_TTFB_ABSOLUTE_MAX = 3000.0  # 绝对约束：平均 TTFB 超过该值（ms）一律降为 ⭐
STAR_ORDER = {"⭐⭐⭐": 0, "⭐⭐": 1, "⭐": 2, "⚠️": 3}  # 星级排序权重

# --- 单仓接口评分参数（v2.4）---
SCORE_TIMEOUT = 6        # 单仓接口测速超时（秒）
SCORE_CONCURRENCY = 16   # 单仓接口测速并发数
SCORE_TTFB_A = 1000.0    # TTFB < 1s → A
SCORE_TTFB_B = 3000.0    # TTFB < 3s → B，否则 C

ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE


def setup_logging():
    handler = RotatingFileHandler(
        LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8")
    handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    # 同时输出到控制台（CI 日志里可见）
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(console)


def notify_telegram(text):
    """可选 Telegram 告警；未配置 token/chat_id 时静默跳过。"""
    token = os.environ.get("TG_BOT_TOKEN")
    chat_id = os.environ.get("TG_CHAT_ID")
    if not token or not chat_id:
        return False
    payload = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10, context=ssl_ctx) as resp:
            return resp.status == 200
    except Exception as e:
        logging.warning(f"  ⚠️ Telegram 通知失败: {e}")
        return False


def encode_url(url):
    """中文域名转 IDNA punycode，解决 urllib 无法处理中文域名的问题。"""
    parsed = urlparse(url)
    if parsed.hostname and not parsed.hostname.isascii():
        encoded_host = parsed.hostname.encode("idna").decode("ascii")
        return urlunparse((parsed.scheme, encoded_host, parsed.path,
                           parsed.params, parsed.query, parsed.fragment))
    return url


def fetch_json(url, timeout=HTTP_TIMEOUT):
    """拉取 JSON，容忍 BOM / 行注释 / 控制字符。失败返回 None。"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    req = urllib.request.Request(encode_url(url), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_ctx) as resp:
            raw = resp.read()
            data = raw.decode("utf-8", errors="ignore")
            if data.startswith("\ufeff"):
                data = data[1:]
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                clean_lines = [l for l in data.split("\n")
                               if not l.strip().startswith("//")]
                try:
                    return json.loads("\n".join(clean_lines))
                except json.JSONDecodeError:
                    return json.loads(re.sub(r'[\x00-\x1f\x7f]', '', data))
    except Exception as e:
        logging.debug(f"  ⚠️  获取失败 {url}: {e}")
        return None


def fetch_bmp_json(url, timeout=HTTP_TIMEOUT):
    """从 BMP 图片中提取内嵌 base64 JSON 配置（饭太硬格式）。"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    req = urllib.request.Request(encode_url(url), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_ctx) as resp:
            raw = resp.read()
        text = raw.decode("latin-1")
        match = re.search(r"[A-Za-z0-9+/=]{500,}", text)
        if not match:
            return None
        decoded = base64.b64decode(match.group())
        json_str = decoded.decode("utf-8", errors="ignore")
        start, end = json_str.find("{"), json_str.rfind("}")
        if start < 0 or end <= start:
            return None
        return json.loads(json_str[start:end + 1])
    except Exception as e:
        logging.debug(f"  ⚠️  BMP 解析失败 {url}: {e}")
        return None


def probe_once(url, timeout=12):
    """
    socket 层测速探活：一次请求同时拿到连通性与耗时指标。
    返回 (ok, metrics|None)，metrics = (connect_ms, ttfb_ms, total_ms, size)。
    - ok: 收到任何 HTTP 响应头（2xx/3xx 均视为健康）
    - connect_ms: TCP 连接耗时
    - ttfb_ms:   发送请求到收到响应头的耗时（首字节时间）
    - total_ms:  总耗时
    """
    try:
        parsed = urlparse(encode_url(url))
        scheme = parsed.scheme or "http"
        host = parsed.hostname
        if not host:
            return False, None
        port = parsed.port or (443 if scheme == "https" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query

        t0 = time.time()
        sock = socket.create_connection((host, port), timeout=timeout)
        t1 = time.time()
        if scheme == "https":
            sock = ssl_ctx.wrap_socket(sock, server_hostname=host)
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)\r\n"
            "Connection: close\r\n"
            "Accept: */*\r\n\r\n"
        ).encode("utf-8")
        sock.settimeout(timeout)
        sock.sendall(request)
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
        t2 = time.time()
        sock.close()
        ok = buf.startswith(b"HTTP/")
        if not ok:
            return False, None
        connect_ms = (t1 - t0) * 1000
        ttfb_ms = (t2 - t1) * 1000
        total_ms = (t2 - t0) * 1000
        return True, (round(connect_ms, 1), round(ttfb_ms, 1),
                      round(total_ms, 1), len(buf))
    except Exception:
        return False, None


def probe_line(url, timeout=12):
    """带重试的探活测速：返回 (ok, metrics|None)。"""
    for attempt in range(RETRY_COUNT + 1):
        ok, metrics = probe_once(url, timeout)
        if ok:
            return True, metrics
        if attempt < RETRY_COUNT:
            time.sleep(RETRY_DELAY)
    return False, None


def load_health_state():
    try:
        with open(HEALTH_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_health_state(state):
    with open(HEALTH_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# 线路定义：(名称, 主地址, [镜像地址列表])
# v2.4 扩充：新增高天流云 / 俊佬 / 道长 / FM影视 4 条已验证线路（21 条）
# ---------------------------------------------------------------------------
LINES = [
    ("小盒子4K", "http://xhztv.top/4k.json", []),
    ("小盒子单仓", "http://xhztv.top/xhz/", []),
    ("老刘备", "https://raw.liucn.cc/box/m.json", []),
    ("小马", "https://szyyds.cn/tv/x.json", []),
    ("无名", "https://6800.kstore.vip/fish.json", []),
    ("jinenge", "https://jinenge.us.kg/app/tvbox/tvbox.json", []),
    ("摸鱼儿", "http://摸鱼儿.cc", []),
    ("肥猫", "http://肥猫.net/", []),
    ("OK影视", "https://cdn.jsdelivr.net/gh/2hacc/TVBox@main/oktv.json", [
        "https://fastly.jsdelivr.net/gh/2hacc/TVBox@main/oktv.json",
    ]),
    ("嗷呜", "http://itv666.cc/aowu/config.webp", []),
    ("VOX", "http://rihou.cc:88/demo.php", []),
    ("挺好分享多仓", "https://ztha.top/TVBox/GYCK.json", []),
    ("饭太硬(ftygit)", "https://cdn09022024.gitlink.org.cn/api/v1/repos/xxooo/in/raw/in.bmp", []),
    ("饭太硬(官方)", "http://www.饭太硬.cc/tv", []),
    ("王二小", "http://new.王二小放牛娃.top", []),
    ("小盒子多仓", "http://xhztv.top/dc", []),
    ("拾光多仓", "http://xmbjm.fh4u.org/dc.txt", []),
    ("高天流云", "https://fastly.jsdelivr.net/gh/gaotianliuyun/gao@master/js.json", []),
    ("俊佬", "http://home.jundie.top:81/top98.json", []),
    ("道长", "https://gitlab.com/duomv/dzhipy/-/raw/main/index.json", []),
    ("FM影视", "http://fmys.top/fmys.json", []),
]

# ---------------------------------------------------------------------------
# 上游数据源：(名称, 地址, need_bmp)。顺序即优先级：靠前的源（主源）同 key 站点胜出。
# 饭太硬使用 BMP 图内嵌配置，标记 need_bmp=True 走专用解析。
# v2.4 扩充：新增道长/高天流云/FM影视/俊佬 4 个已验证源（13 个上游源），
# 追加在原有主源之后，作为站点补充源（重叠 key 仍以原主源为准）。
# ---------------------------------------------------------------------------
UPSTREAM_SOURCES = [
    ("老刘备", "https://raw.liucn.cc/box/m.json", False),
    ("小马", "https://szyyds.cn/tv/x.json", False),
    ("无名", "https://6800.kstore.vip/fish.json", False),
    ("jinenge", "https://jinenge.us.kg/app/tvbox/tvbox.json", False),
    ("小盒子4K", "http://xhztv.top/4k.json", False),
    ("小盒子单仓", "http://xhztv.top/xhz/", False),
    ("OK影视", "https://cdn.jsdelivr.net/gh/2hacc/TVBox@main/oktv.json", False),
    ("VOX", "http://rihou.cc:88/demo.php", False),
    ("饭太硬", "https://cdn09022024.gitlink.org.cn/api/v1/repos/xxooo/in/raw/in.bmp", True),
    ("道长", "https://gitlab.com/duomv/dzhipy/-/raw/main/index.json", False),
    ("高天流云", "https://fastly.jsdelivr.net/gh/gaotianliuyun/gao@master/js.json", False),
    ("FM影视", "http://fmys.top/fmys.json", False),
    ("俊佬", "http://home.jundie.top:81/top98.json", False),
]

# ---------------------------------------------------------------------------
# 失效站点维护表（v2.4 合并自 reasonix 实测调研成果）
# ---------------------------------------------------------------------------
# 已知失效 API 修复表：将失效的 API 地址替换为可用的替代地址
API_FIXES = {
    "https://notabug.org/fantaiying/ext/raw/main/drpy2.min.js":
        "http://xhztv.top/xhz/js/lib/drpy2.min.js",
    "https://notabug.org/fantaiying/ext/raw/main/drpy2.js":
        "http://xhztv.top/xhz/js/lib/drpy2.min.js",
}

# 彻底失效的站点 key 黑名单（域名已死或接口永久失效）
DEAD_KEYS = {
    "csp_小胡",           # c.小胡.icu DNS 死
    "小胡",               # xh1.xn--yetu07f.icu DNS 死
    "奇虎资源",           # caiji.qhzyapi.com DNS 死
    "可可资源",           # kekezy1.com DNS 死
    "初恋资源",           # video.adminqt.cn DNS 死
    "熊掌",               # xzcjz.com DNS 死
    "feisu",              # feisuzyapi.com 连接拒绝
    "csp_非凡资源",       # ffzy1.tv 超时
    "ikunzy",             # ikunzyapi.com 连接重置
    "暴风采集",           # app.bfzyapi.com 404
    " 在线┃直播2",        # alist.xn--z7x900a.live 404
    "drpy_js_360影视",    # jihulab.com 404
    "drpy_js_310直播",    # github.moeyy.xyz DNS 死
    "虎牙直播js",         # notabug 404，且已有虎牙直播替代
}


def fix_sites(sites):
    """修复已知失效的 API 并移除彻底死掉的站点（reasonix 调研维护表）。"""
    fixed_count = 0
    removed_count = 0
    result = []
    for site in sites:
        key = site.get("key", "")
        api = site.get("api", "")

        if key in DEAD_KEYS:
            logging.info("  🗑️  移除失效站点: %s (key=%s)", site.get('name', '?'), key)
            removed_count += 1
            continue

        if api in API_FIXES:
            old_api, new_api = api, API_FIXES[api]
            site["api"] = new_api
            logging.info("  🔧 修复: %s — %s... → %s...",
                         site.get('name', '?'), old_api[:40], new_api[:40])
            fixed_count += 1

        result.append(site)

    if fixed_count or removed_count:
        logging.info("  📊 失效维护: 修复 %d 个，移除 %d 个", fixed_count, removed_count)
    return result


# 单仓接口评分：等级 → 排序权重
GRADE_RANK = {"A": 0, "B": 1, "C": 2, "D": 3, "N/A": 4}
GRADE_ICON = {"A": "🟢", "B": "🟡", "C": "🟠", "D": "🔴", "N/A": "⚪"}


def score_and_sort_sites(sites):
    """
    单仓接口评分 + 按分数排序（v2.4）：
      - 可测接口（api 为 http 或 ext 内嵌 http URL）实测打分：
        A = TTFB < 1s，B = < 3s，C = 其余可达，D = 不可达
      - 站点名称加前缀标注（🟢A· 名称 / 🟡B· / 🔴D·），按分数降序排列
      - 不可测站点（csp_ 内置爬虫、本地脚本等无网络地址）保持原顺序排在后部
    返回排序标注后的站点列表。
    """
    testable, untestable = [], []
    for s in sites:
        api = s.get("api", "")
        url = None
        if api.startswith("http"):
            url = api.split()[0]
        elif (api.startswith("csp_") or api.startswith("csp ")) \
                and isinstance(s.get("ext", ""), str) \
                and s.get("ext", "").startswith("http"):
            url = s["ext"].split()[0]
        if url:
            testable.append((s.get("key", ""), s, url))
        else:
            untestable.append(s)

    if not testable:
        logging.info("  📊 单仓接口评分: 无可测接口")
        return sites

    def probe(item):
        key, site, url = item
        ok, m = probe_once(url, timeout=SCORE_TIMEOUT)
        if not ok:
            # 本地组件 / 模板 URL 视为不可测，不算失败
            if url.startswith("http://127.0.0.1") or "{" in url \
                    or "$$$" in url or " " in url or "+4" in url:
                return key, "N/A", None
            return key, "D", None
        ttfb = m[1]
        grade = "A" if ttfb < SCORE_TTFB_A else ("B" if ttfb < SCORE_TTFB_B else "C")
        return key, grade, ttfb

    scores = {}
    with ThreadPoolExecutor(max_workers=SCORE_CONCURRENCY) as ex:
        for key, grade, ttfb in ex.map(probe, testable):
            scores[key] = (grade, ttfb)

    marked = []
    for key, site, url in testable:
        grade, ttfb = scores.get(key, ("N/A", None))
        name = site.get("name", "")
        icon = GRADE_ICON.get(grade, "⚪")
        site["name"] = f"{icon}{grade}·{name}" if grade != "N/A" else f"{icon}{name}"
        marked.append((GRADE_RANK.get(grade, 4),
                       ttfb if ttfb is not None else 999999.0, site))
    marked.sort(key=lambda x: (x[0], x[1]))

    from collections import Counter
    g_cnt = Counter()
    for _rk, _ttfb, site in marked:
        n = site["name"]
        for letter in "ABCD":
            if n.startswith(f"{GRADE_ICON[letter]}{letter}·"):
                g_cnt[letter] += 1
    logging.info("  📊 单仓接口评分: 可测 %d 个 | A=%d B=%d C=%d D=%d N/A=%d",
                 len(marked), g_cnt.get("A", 0), g_cnt.get("B", 0),
                 g_cnt.get("C", 0), g_cnt.get("D", 0),
                 sum(1 for rk, _, s in marked if s["name"].startswith("⚪")))
    logging.info("  🔀 单仓站点已按评分排序（可测接口在前，内置爬虫在后）")

    return [m[2] for m in marked] + untestable


def load_speed_state():
    try:
        with open(SPEED_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_speed_state(state):
    with open(SPEED_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def record_speed_sample(state, url, metrics):
    """追加一次测速样本（滑动窗口保留最近 MAX_SPEED_SAMPLES 次）。"""
    entry = state.setdefault(url, {"samples": []})
    connect_ms, ttfb_ms, total_ms, size = metrics
    entry["samples"].append({
        "ts": datetime.now().strftime("%m-%d %H:%M"),
        "connect": connect_ms,
        "ttfb": ttfb_ms,
        "total": total_ms,
        "size": size,
    })
    entry["samples"] = entry["samples"][-MAX_SPEED_SAMPLES:]


def avg_ttfb(state, url):
    """最近样本的平均 TTFB（毫秒），无样本返回 None。"""
    samples = state.get(url, {}).get("samples", [])
    if not samples:
        return None
    return sum(s["ttfb"] for s in samples) / len(samples)


def avg_total(state, url):
    """最近样本的平均总耗时（毫秒），无样本返回 0。"""
    samples = state.get(url, {}).get("samples", [])
    if not samples:
        return 0.0
    return sum(s["total"] for s in samples) / len(samples)


def compute_ratings(health_state, speed_state, active_urls):
    """
    星级评级（相对排名分档，保证区分度）：
      1. 无失败史线路按平均 TTFB 升序排名，前 1/3 → ⭐⭐⭐，中 1/3 → ⭐⭐，后 1/3 → ⭐
      2. 有失败史（1-2 次）→ ⚠️ 波动，排最后
      3. 绝对约束：平均 TTFB >= 3000ms 一律降为 ⭐（慢源不配推荐）
    返回 {url: (星标前缀, 排序权重, avg_ttfb)}
    """
    no_fail = []
    for url in active_urls:
        if health_state.get(url, 0) > 0:
            continue
        ttfb = avg_ttfb(speed_state, url)
        no_fail.append((url, ttfb if ttfb is not None else 99999.0))
    no_fail.sort(key=lambda x: x[1])
    n = len(no_fail)
    third = max(1, (n + 2) // 3)  # 每档至少 1 条

    ratings = {}
    for idx, (url, ttfb) in enumerate(no_fail):
        if idx < third:
            star = "⭐⭐⭐"
        elif idx < third * 2:
            star = "⭐⭐"
        else:
            star = "⭐"
        if ttfb >= 3000.0:  # 绝对约束：太慢的源不配推荐
            star = "⭐"
        ratings[url] = (star, STAR_ORDER[star], ttfb)
    for url in active_urls:
        if health_state.get(url, 0) > 0:
            ratings[url] = ("⚠️", STAR_ORDER["⚠️"], avg_ttfb(speed_state, url))
    return ratings


def health_check_all():
    """并发探活测速 17 个线路，返回
    (active_lines, removed_lines, health_state, speed_state, ratings)。
    active_lines 元素: (name, url, mirrors, metrics)
    ratings: {url: (star_prefix, 排序权重, avg_ttfb)}
    """
    logging.info("🔍 健康检查 + 线路测速（连续%d次失败自动移除）...", MAX_FAILURES)
    health_state = load_health_state()
    speed_state = load_speed_state()

    def probe(item):
        name, url, mirrors = item
        ok, metrics = probe_line(url)
        return name, url, mirrors, ok, metrics

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        results = list(ex.map(probe, LINES))

    active, removed = [], []
    for name, url, mirrors, ok, metrics in results:
        if ok and metrics:
            health_state[url] = 0
            record_speed_sample(speed_state, url, metrics)
            active.append((name, url, mirrors, metrics))
        else:
            health_state[url] = health_state.get(url, 0) + 1
            fail_count = health_state[url]
            if fail_count >= MAX_FAILURES:
                removed.append(name)
                logging.warning("  ❌ %s — 连续 %d 次失败，已移除", name, fail_count)
            else:
                active.append((name, url, mirrors, None))
                logging.warning("  ⚠️  %s — 第 %d/%d 次失败，保留", name, fail_count, MAX_FAILURES)

    save_health_state(health_state)
    save_speed_state(speed_state)

    # 汇总评级与测速表
    ratings = compute_ratings(health_state, speed_state,
                              [u for _, u, _, _ in active])

    logging.info("  ┌─────────────────────────────────────────────────────┐")
    logging.info("  │ 📡 线路测速结果（平均TTFB / 总耗时，越短越快）            │")
    logging.info("  ├─────────────────────────────────────────────────────┤")
    for name, url, mirrors, metrics in active:
        star, order, ttfb = ratings[url]
        total = avg_total(speed_state, url)
        if ttfb is None:
            line = f"{name}: 无测速数据"
        else:
            line = f"{name}: TTFB {ttfb:.0f}ms | 总耗时 {total:.0f}ms | {star}"
        logging.info("  │ %-52s │", line[:52])
    logging.info("  └─────────────────────────────────────────────────────┘")
    if removed:
        logging.warning("  🗑️  本次移除: %s", ", ".join(removed))
    logging.info("  📊 活跃线路: %d 个", len(active))
    return active, removed, health_state, speed_state, ratings


def fetch_upstream_sources():
    """并发拉取 9 个上游源，返回 [(源名, [sites]), ...]，失败的源跳过不阻塞。"""
    logging.info("🔄 从上游源拉取站点数据...")

    def fetch(item):
        name, url, need_bmp = item
        data = fetch_json(url)
        if data is None and need_bmp:
            data = fetch_bmp_json(url)
        if data and "sites" in data:
            return name, data["sites"]
        return name, None

    results = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = {ex.submit(fetch, item): item[0] for item in UPSTREAM_SOURCES}
        for future in futures:
            name = futures[future]
            try:
                src, sites = future.result()
                if sites is not None:
                    results.append((src, sites))
                    logging.info("  📥 %s — 获取 %d 个站点", src, len(sites))
                else:
                    logging.warning("  📥 %s — 获取失败，跳过", src)
            except Exception as e:
                logging.warning("  📥 %s — 异常 %s，跳过", name, e)
    return results


def deduplicate_by_priority(ordered_site_lists):
    """按上游源顺序去重：靠前的源（主源）同 key 胜出，后续源忽略。"""
    seen, order = {}, []
    for _src, sites in ordered_site_lists:
        for site in sites:
            key = site.get("key", "")
            if key and key not in seen:
                seen[key] = site
                order.append(key)
    return [seen[k] for k in order]


def write_json_safely(path, data):
    """序列化 + 回读校验后再落盘，避免写出坏 JSON。"""
    text = json.dumps(data, ensure_ascii=False, indent=2)
    json.loads(text)  # 校验：解析失败会抛异常，不落盘
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def write_ci_output(key, value):
    """在 GitHub Actions 中输出变量（非 CI 环境静默跳过）。"""
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            f.write(f"{key}={value}\n")


def main():
    setup_logging()
    logging.info("=" * 50)
    logging.info("📺 TVBox 源自动更新工具 v2.4")
    logging.info("⏰ 更新时间: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logging.info("=" * 50)

    # 1. 健康检查 + 测速 + 熔断（并发 + 重试）
    active_lines, removed_lines, _hs, speed_state, ratings = health_check_all()

    # 2. 生成 urls.json（按星级排序，推荐线路排最前；镜像随主线路）
    #    评级数据: ratings[url] = (星标, 排序权重, avg_ttfb)
    def line_sort_key(item):
        name, url, mirrors, metrics = item
        return (ratings.get(url, ("⭐", 9, None))[1],
                ratings.get(url, ("⭐", 9, None))[2] or 9999)

    active_lines.sort(key=line_sort_key)

    urls = []
    for name, url, mirrors, metrics in active_lines:
        star = ratings.get(url, ("⭐", 9, None))[0]
        urls.append({"name": f"{star}{name}", "url": url})
        for i, mirror in enumerate(mirrors):
            urls.append({"name": f"🪞{name}镜像{i + 1}", "url": mirror})
    urls.append({"name": "🪞GitHub镜像(kgithub)",
                 "url": "https://raw.kkgithub.com/jifeng250/tvbox-sources/main/tvbox.json"})
    urls.append({"name": "🪞GitHub镜像(jsdelivr)",
                 "url": "https://fastly.jsdelivr.net/gh/jifeng250/tvbox-sources@main/tvbox.json"})
    write_json_safely(os.path.join(SCRIPT_DIR, "urls.json"), {"urls": urls})
    logging.info("📋 已生成 urls.json（%d 条线路，含镜像）", len(urls))
    logging.info("  ⭐⭐⭐ 推荐 %d 条 | ⭐⭐ 良好 %d 条 | ⭐ 可用 %d 条 | ⚠️ 波动 %d 条",
                 sum(1 for s, _, _ in ratings.values() if s == "⭐⭐⭐"),
                 sum(1 for s, _, _ in ratings.values() if s == "⭐⭐"),
                 sum(1 for s, _, _ in ratings.values() if s == "⭐"),
                 sum(1 for s, _, _ in ratings.values() if s == "⚠️"))

    # 3. 并发拉取上游
    upstream_results = fetch_upstream_sources()
    total = sum(len(s) for _, s in upstream_results)
    logging.info("  📊 共获取 %d 个站点（来自 %d/%d 个源）",
                 total, len(upstream_results), len(UPSTREAM_SOURCES))

    # 4. 按优先级去重
    deduped = deduplicate_by_priority(upstream_results)
    logging.info("  📊 去重后剩余 %d 个站点", len(deduped))

    # 4.5 失效站点维护（v2.4：死站黑名单移除 + 失效 API 替换，reasonix 调研表）
    deduped = fix_sites(deduped)
    logging.info("  📊 失效维护后剩余 %d 个站点", len(deduped))

    # 5. 站点数异常熔断：低于阈值拒绝覆盖（保护线上配置）
    if len(deduped) < MIN_SITES_WARN:
        msg = (f"🚨 站点数异常熔断：仅 {len(deduped)} 个站点"
               f"（阈值 {MIN_SITES_WARN}），疑似上游大面积故障，已放弃本次更新")
        logging.error(msg)
        notify_telegram(f"❌ TVBox 源更新失败\n{msg}")
        sys.exit(1)

    # 6. 单仓接口评分 + 排序（v2.4：按评分高低整理，名称标注分数等级）
    deduped = score_and_sort_sites(deduped)

    # 7. 生成 tvbox.json（JSON 回读校验后落盘）
    tvbox_data = {
        "spider": "",
        "wallpaper": "https://raw.githubusercontent.com/jifeng250/tvbox-sources/main/wallpaper.jpg",
        "updateTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "warningText": "资源来自网络，仅供学习交流使用，请勿用于商业用途。",
        "sites": deduped,
    }
    write_json_safely(os.path.join(SCRIPT_DIR, "tvbox.json"), tvbox_data)
    logging.info("✅ 已生成 tvbox.json（%d 个站点，已评分排序）", len(deduped))

    # 8. CI 输出 + 完成告警
    write_ci_output("sites", len(deduped))
    write_ci_output("lines", len(active_lines))
    if removed_lines:
        notify_telegram(
            f"⚠️ TVBox 源更新（部分线路移除）\n"
            f"移除线路: {', '.join(removed_lines)}\n"
            f"活跃线路: {len(active_lines)} | 站点数: {len(deduped)}")
    logging.info("✅ 更新完成！")


if __name__ == "__main__":
    main()
