#!/usr/bin/env python3
"""云端自动打包 TVBox 精选源本地包（在 GitHub Actions 中运行）

逻辑与本地 tools/update_tvbox_local_pack.py 一致，但 SRC=当前工作目录，
用于云端每次源/直播更新后自动生成 tvbox-sources-YYYYMMDD.zip + version.json，
并存回仓库根，使 PWA 下载页始终显示最新包。

打包内容：curated.json + curated-local.json + curated-shorts.json + curated-shorts-local.json + 3/4 个 jar + 直播源 + 台标/EPG 配置等
排除：.git / __pycache__ / 其它 tvbox-sources-*.zip / version.json / sync.bat / 调试jar
"""
import zipfile, os, sys, json
from datetime import datetime

SRC = os.getcwd()  # GitHub Actions checkout 目录
# apk/.github/build-logs/LiveSpeedTest 不打进配置包（apk 在网页「影视软件」区块单独下载，避免包膨胀到~100MB）
EXCLUDE_DIRS = {'.git', '__pycache__', 'apk', '.github', 'build-logs', 'LiveSpeedTest'}
REQUIRED = ['curated.json', 'fan.jar', 'custom_spider.jar', 'pg.jar', 'xyq.jar', 'tvfan/Cloud-drive.txt']
# 这些文件不应打进 TV 包（本地维护脚本/调试遗留）
EXCLUDE_FILES = {'sync.bat', 'XBPQ_upgraded.jar', 'update.log', 'HCCX.jar', 'curated-bak-unsafe.json',
                 'pack.py', 'update.py', 'gradle.log', 'media-gradle.log', 'alist.json',
                 'dead_candidates.json', 'health_state.json', 'speed_state.json'}


def _localize(name, jar):
    """生成某仓的 -local.json：仅 jar 字段指向包内相对路径；js源保持在线URL(影视仓本地js源不可靠,曾致全源崩溃)"""
    src = os.path.join(SRC, name)
    dst = src.replace('.json', '-local.json')
    if not os.path.exists(src):
        return None
    cfg = json.load(open(src, encoding='utf-8'))
    cfg['spider'] = jar
    for s in cfg.get('sites', []):
        if 'jar' in s:
            for jn in ['custom_spider.jar', 'pg.jar', 'fan.jar', 'spider_shorts.jar']:
                if jn in s['jar']:
                    s['jar'] = './' + jn
                    break
    # 注: 不要改写 type=3 的 js 源 api/ext 为本地路径 —— 影视仓本地包加载 js 源会失败并拖垮整个配置
    cfg['updateTime'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    cfg['warningText'] = '本地版：jar 走本地文件，断网可用。若源加载不出目录，把 jar 改为 file:///绝对路径（见 README）'
    with open(dst, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=1)
    return dst


def make_local_configs():
    """生成所有仓的 -local.json：精选主仓(./fan.jar) + 短剧专仓(./spider_shorts.jar)"""
    _localize('curated.json', './fan.jar')
    _localize('curated-shorts.json', './spider_shorts.jar')


def main():
    # 前置校验：关键文件存在
    missing = [r for r in REQUIRED if not os.path.exists(os.path.join(SRC, r))]
    if missing:
        print(f"❌ 缺少关键文件: {', '.join(missing)}，拒绝打包")
        sys.exit(1)

    make_local_configs()
    print("已生成本地版配置: curated-local.json + curated-shorts-local.json")

    date_str = datetime.now().strftime("%Y%m%d")
    dst = os.path.join(SRC, f"tvbox-sources-{date_str}.zip")

    # 清理旧 zip（保留当天最新一份）
    for old in os.listdir(SRC):
        if old.startswith("tvbox-sources-") and old.endswith(".zip") and old != os.path.basename(dst):
            try:
                os.remove(os.path.join(SRC, old))
                print(f"  清理旧包: {old}")
            except OSError:
                pass

    count = 0
    with zipfile.ZipFile(dst, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        for root, dirs, files in os.walk(SRC):
            dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
            for f in files:
                if f.endswith('.zip') or f == 'version.json' or f in EXCLUDE_FILES:
                    continue
                full = os.path.join(root, f)
                arc = os.path.join('tvbox-sources', os.path.relpath(full, SRC))
                z.write(full, arc)
                count += 1

    size_kb = os.path.getsize(dst) / 1024
    print(f"OK: {count} files, {size_kb:.1f} KB")
    print(f"新包: {dst}")

    # 生成 version.json（PWA 页面读取）
    cfg = json.load(open(os.path.join(SRC, 'curated.json'), encoding='utf-8'))
    filename = os.path.basename(dst)
    raw = f"https://raw.githubusercontent.com/jifeng250/tvbox-sources/main/{filename}"
    urls = [
        f"https://gh-proxy.com/{raw}",
        f"https://ghproxy.net/{raw}",
        f"https://cdn.jsdelivr.net/gh/jifeng250/tvbox-sources@main/{filename}",
        f"https://fastly.jsdelivr.net/gh/jifeng250/tvbox-sources@main/{filename}",
        f"https://gcore.jsdelivr.net/gh/jifeng250/tvbox-sources@main/{filename}",
        raw,
    ]
    ver = {
        "version": date_str,
        "date": datetime.now().strftime('%Y-%m-%d %H:%M'),
        "size": f"{size_kb / 1024:.2f} MB",
        "sites": len(cfg.get('sites', [])),
        "url": urls[0],
        "urls": urls,
    }
    with open(os.path.join(SRC, 'version.json'), 'w', encoding='utf-8') as f:
        json.dump(ver, f, ensure_ascii=False, indent=2)
    print(f"已生成 version.json (sites={ver['sites']})")


if __name__ == "__main__":
    main()