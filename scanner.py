"""
m3u8 频道扫描器 — 深度验证版（含失败原因记录）
================================================
功能：
  1. 读取 targets.yaml 里配置的扫描目标（支持模板/片段/正则/自动推断）
  2. 按模板批量构造候选 URL，并发深度验证（m3u8 -> 子列表 -> 分片）
  3. 输出可播放的 M3U 播放列表
  4. 输出详细报告，包含每个目标的失败原因统计和样本，便于排查

维护提示：
  - 若某目标 valid=0，先看 scan_report.json 里的 failure_reasons 字段
  - 常见失败原因见文件末尾 COMMON_REASONS 注释
"""
import re
import asyncio
import aiohttp
import yaml
import json
import time
import argparse
from urllib.parse import urlparse, urljoin
from pathlib import Path
from datetime import datetime


# ============================================================
# 全局常量
# ============================================================

# 默认 User-Agent（部分服务器会校验）
DEFAULT_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/120.0.0.0 Safari/537.36")

# 视为"视频/音频"的 Content-Type 前缀（用于判断分片是否合法）
VALID_CT_PREFIX = ("video/", "audio/", "application/octet-stream",
                   "application/vnd.apple.mpegurl", "binary/octet-stream")


# ============================================================
# 频道名映射表
# ============================================================

def load_channel_map(path="channel_map.yaml"):
    """
    读取 channel_map.yaml，返回 {数字ID: 频道名}。
    用于给纯数字 URL 补上可读的频道名。
    """
    if not Path(path).exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    # 统一转成字符串 key，避免 YAML 把纯数字解析成 int
    return {str(k): v for k, v in data.items()}


# ============================================================
# URL 模板推断
# ============================================================

def extract_digit_segments(url):
    """
    提取 URL 中所有数字片段（排除协议/IP/端口）。
    返回 [(数字字符串, 起始位置, 结束位置), ...]
    """
    parsed = urlparse(url)
    # 从 scheme://netloc 之后开始截，避开 IP 和端口里的数字
    tail_start = len(f"{parsed.scheme}://{parsed.netloc}")
    tail = url[tail_start:]
    return [(m.group(), tail_start + m.start(), tail_start + m.end())
            for m in re.finditer(r'\d+', tail)]


def resolve_template(target):
    """
    根据 target 配置决定扫描用的 URL 模板。
    优先级：显式 template > id_regex > id_segment > 自动推断
    返回 (template, note)，note 用于日志显示推断依据。
    """
    base_url = target["base_url"]

    # 1. 用户显式指定模板，最可靠
    if target.get("template"):
        return target["template"], "手动模板"

    # 2. 用户用正则指定要替换的片段
    if target.get("id_regex"):
        regex = target["id_regex"]
        m = re.search(regex, base_url)
        if not m or m.lastindex is None:
            raise ValueError(f"id_regex 未匹配到有效捕获组: {regex}")
        s, e = m.start(1), m.end(1)
        return base_url[:s] + "{id}" + base_url[e:], f"正则 {regex}"

    # 3. 用户用序号指定第几段数字
    if target.get("id_segment") is not None:
        idx = int(target["id_segment"]) - 1  # 用户从 1 开始数
        segs = extract_digit_segments(base_url)
        if idx < 0 or idx >= len(segs):
            raise ValueError(
                f"id_segment={idx+1} 超范围，该 URL 只有 {len(segs)} 个数字片段: "
                f"{[s[0] for s in segs]}"
            )
        num, s, e = segs[idx]
        return base_url[:s] + "{id}" + base_url[e:], f"片段{idx+1}={num}"

    # 4. 自动推断：取最后一个非年份的数字片段
    segs = extract_digit_segments(base_url)
    if not segs:
        return base_url, "无数字片段，仅验证原地址"

    # 排除形如 2024/2023 的年份
    candidates = [s for s in segs
                  if not (len(s[0]) == 4 and s[0].startswith(("19", "20")))]
    if not candidates:
        candidates = segs
    num, s, e = candidates[-1]
    template = base_url[:s] + "{id}" + base_url[e:]
    return template, f"自动推断替换 '{num}'（如不对请显式设置 template）"


# ============================================================
# HTTP 工具
# ============================================================

async def fetch_text(session, url, headers, timeout, max_bytes=16384):
    """
    拉取文本内容。只读前 max_bytes 字节，避免下载整个分片。
    返回 (text, None) 成功； (None, reason) 失败。
    """
    try:
        async with session.get(url, headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=True) as r:
            if r.status != 200:
                return None, f"HTTP{r.status}"
            data = await r.content.read(max_bytes)
            if not data:
                return None, "空响应"
            return data.decode("utf-8", errors="ignore"), None
    except asyncio.TimeoutError:
        return None, "超时"
    except Exception as e:
        # 只保留前 40 字符，避免报告被异常栈撑爆
        return None, str(e)[:40]


# ============================================================
# m3u8 解析工具
# ============================================================

def extract_name_from_text(text, url):
    """
    从 m3u8 文本里提取频道名。
    优先 #EXTINF 行，其次 #EXT-X-STREAM-INF 的 NAME 属性。
    """
    m = re.search(r'#EXTINF:[^\n]*?,(.+)', text)
    if m:
        n = m.group(1).strip()
        # 过滤无意义的占位名
        if n and n.lower() not in ("live", "stream", "unknown"):
            return n
    m = re.search(r'#EXT-X-STREAM-INF:[^\n]*?NAME="([^"]+)"', text)
    return m.group(1).strip() if m else ""


def pick_sub_playlist(master_text, base_url):
    """从 master playlist 里取第一个子播放列表地址"""
    lines = master_text.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF") and i + 1 < len(lines):
            nxt = lines[i + 1].strip()
            if nxt and not nxt.startswith("#"):
                return urljoin(base_url, nxt)
    return None


def pick_first_segment(media_text, base_url):
    """从 media playlist 里取第一个分片地址"""
    for line in media_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if any(line.endswith(ext) for ext in
               (".ts", ".m4s", ".aac", ".mp4", ".m4a")) or "?" in line:
            return urljoin(base_url, line)
    return None


def extract_path_name(url):
    """从 URL 路径里提取可读标识（如 /cctv1/index.m3u8 -> cctv1）"""
    m = re.search(r'/([a-zA-Z][a-zA-Z0-9_-]{1,30})(?:/|\.m3u8)', urlparse(url).path)
    return m.group(1) if m else ""


def lookup_channel_map(url, channel_map):
    """用 URL 里的数字片段去 channel_map 查频道名"""
    for num in re.findall(r'\d+', urlparse(url).path):
        if num in channel_map:
            return channel_map[num]
    return ""


# ============================================================
# 深度验证（核心）
# ============================================================

async def deep_validate(session, m3u8_url, headers, timeout, channel_map):
    """
    深度验证一个 m3u8 地址是否可播：
      1. 拉原始 m3u8，确认有 #EXTM3U
      2. 若是 master playlist，取子列表再拉一次
      3. 从 media playlist 里取第一个分片，实际请求验证非空
    返回 dict，包含 playable / reason / name 等字段。
    """
    result = {
        "playable": False,
        "verify_level": None,      # segment / playlist
        "name": "",
        "name_source": "",         # extinf / path / map / fallback
        "reason": "",              # 失败原因
        "segment_size": 0,
        "segment_time": 0.0,
    }

    # 步骤 1：拉 m3u8
    text, err = await fetch_text(session, m3u8_url, headers, timeout)
    if err:
        result["reason"] = f"m3u8失败:{err}"
        return result
    if "#EXTM3U" not in text:
        result["reason"] = "非m3u8"
        return result

    # 提取频道名（先记下来，后面若子列表里有更准确的名字会覆盖）
    name = extract_name_from_text(text, m3u8_url)
    name_source = "extinf" if name else ""

    # 步骤 2：处理 master playlist
    current_url = m3u8_url
    if "#EXT-X-STREAM-INF" in text:
        sub = pick_sub_playlist(text, m3u8_url)
        if not sub:
            result.update({"reason": "master无子列表", "name": name,
                           "name_source": name_source})
            return result
        sub_text, err = await fetch_text(session, sub, headers, timeout)
        if err:
            result.update({"reason": f"子列表失败:{err}", "name": name,
                           "name_source": name_source})
            return result
        text, current_url = sub_text, sub
        # 子列表里若有 EXTINF，名字更准确
        n2 = extract_name_from_text(text, sub)
        if n2:
            name, name_source = n2, "extinf"

    # 频道名兜底：路径 -> 映射表 -> fallback
    if not name:
        pn = extract_path_name(m3u8_url)
        if pn:
            name, name_source = pn, "path"
    if not name:
        mn = lookup_channel_map(m3u8_url, channel_map)
        if mn:
            name, name_source = mn, "map"
    if not name:
        stem = Path(urlparse(m3u8_url).path).stem
        name, name_source = (f"fallback-{stem}" if stem else "未知"), "fallback"

    result["name"] = name
    result["name_source"] = name_source

    # 步骤 3：取分片实际请求
    segment = pick_first_segment(text, current_url)
    if not segment:
        # 没有分片但有 EXTINF：可能是直播刚开，或纯列表，标记为 playlist 级
        if "#EXTINF" in text:
            result.update({"playable": True, "verify_level": "playlist",
                           "reason": "仅列表级(无分片可验)"})
            return result
        result["reason"] = "无分片"
        return result

    # 步骤 4：请求分片，验证非空且类型合法
    t0 = time.monotonic()
    try:
        async with session.get(segment, headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200:
                result["reason"] = f"分片HTTP{r.status}"
                return result
            ct = (r.headers.get("Content-Type") or "").lower()
            if ct and not any(ct.startswith(p) for p in VALID_CT_PREFIX):
                result["reason"] = f"分片类型异常:{ct}"
                return result
            chunk = await r.content.read(4096)  # 只读 4KB 够判断非空
            elapsed = time.monotonic() - t0
            if not chunk:
                result["reason"] = "分片为空"
                return result
            result.update({"playable": True, "verify_level": "segment",
                           "reason": "ok", "segment_size": len(chunk),
                           "segment_time": round(elapsed, 3)})
            return result
    except asyncio.TimeoutError:
        result["reason"] = "分片超时"
        return result
    except Exception as e:
        result["reason"] = f"分片异常:{str(e)[:30]}"
        return result


# ============================================================
# 扫描单个目标
# ============================================================

async def scan_target(session, target, settings, semaphore, channel_map):
    """
    扫描一个 target 下的所有候选 URL。
    返回 dict，包含可播频道列表和失败原因列表。
    """
    name = target.get("name", "未命名")
    base_url = target["base_url"]
    start = int(target.get("start", 1))
    end = int(target.get("end", 100))
    referer = target.get("referer", "")
    user_agent = target.get("user_agent") or settings.get("user_agent") or DEFAULT_UA
    timeout = settings.get("timeout", 10)

    # 构造请求头
    headers = {"User-Agent": user_agent, "Accept": "*/*",
               "Connection": "keep-alive"}
    if referer:
        headers["Referer"] = referer

    # 解析模板（失败则直接返回错误信息）
    try:
        template, note = resolve_template(target)
    except ValueError as e:
        print(f"[{name}] 模板解析失败: {e}")
        return {"name": name, "template": "", "total": 0, "valid": 0,
                "inferred": False, "channels": [], "failures": [],
                "error": str(e)}

    inferred = "自动推断" in note
    print(f"[{name}] 模板: {template}")
    print(f"[{name}] 说明: {note}")

    # 生成候选 URL 列表
    urls = ([template.replace("{id}", str(i)) for i in range(start, end + 1)]
            if "{id}" in template else [base_url])

    print(f"[{name}] 候选数: {len(urls)}")

    # 并发限流：用 semaphore 控制同时进行的请求数
    async def limited(u):
        async with semaphore:
            return await deep_validate(session, u, headers, timeout, channel_map)

    results = await asyncio.gather(*[limited(u) for u in urls])

    # 分流：可播的进 valid，失败的进 failures
    valid = []
    failures = []
    for url, r in zip(urls, results):
        if r["playable"]:
            valid.append({
                "url": url, "name": r["name"],
                "name_source": r["name_source"],
                "verify_level": r["verify_level"],
                "segment_size": r["segment_size"],
                "segment_time": r["segment_time"],
                "referer": referer, "user_agent": user_agent,
                "source": name,
            })
        else:
            failures.append({"url": url, "reason": r["reason"]})

    seg_count = sum(1 for v in valid if v["verify_level"] == "segment")
    print(f"[{name}] 可播: {len(valid)}/{len(urls)} (分片级 {seg_count})")

    # 关键改进：失败原因统计 + 样本，写进日志
    if failures:
        reason_count = {}
        for f in failures:
            reason_count[f["reason"]] = reason_count.get(f["reason"], 0) + 1
        print(f"[{name}] 失败原因统计: {reason_count}")
        print(f"[{name}] 失败样本（前3条）:")
        for f in failures[:3]:
            print(f"    {f['url']}  ->  {f['reason']}")

    return {"name": name, "template": template, "total": len(urls),
            "valid": len(valid), "inferred": inferred,
            "channels": valid, "failures": failures}


# ============================================================
# 辅助：把失败列表按 reason 分组计数
# ============================================================

def _count_reasons(failures):
    stats = {}
    for f in failures:
        key = f["reason"]
        stats[key] = stats.get(key, 0) + 1
    return stats


# ============================================================
# 主流程
# ============================================================

async def main():
    parser = argparse.ArgumentParser(description="m3u8 频道扫描器（深度验证版）")
    parser.add_argument("--config", default="targets.yaml")
    parser.add_argument("--channel-map", default="channel_map.yaml")
    parser.add_argument("--output", default="output/playlist.m3u")
    parser.add_argument("--report", default="output/scan_report.json")
    parser.add_argument("--strict", action="store_true",
                        help="只输出分片级验证通过的频道")
    args = parser.parse_args()

    if not Path(args.config).exists():
        print(f"找不到配置文件 {args.config}")
        return

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    settings = cfg.get("settings", {})
    targets = cfg.get("targets", []) or []
    channel_map = load_channel_map(args.channel_map)

    if not targets:
        print("targets.yaml 中没有任何扫描目标")
        return

    concurrency = settings.get("concurrency", 20)
    semaphore = asyncio.Semaphore(concurrency)

    print(f"[*] 共 {len(targets)} 个目标，并发数 {concurrency}")
    print(f"[*] 频道映射表: {len(channel_map)} 条")
    print("=" * 55)

    # 逐个扫描目标（目标之间串行，目标内并发）
    async with aiohttp.ClientSession() as session:
        target_results = []
        for t in targets:
            try:
                r = await scan_target(session, t, settings, semaphore, channel_map)
            except Exception as e:
                print(f"[{t.get('name', '?')}] 扫描异常: {e}")
                r = {"name": t.get("name", "?"), "template": "",
                     "total": 0, "valid": 0, "inferred": False,
                     "channels": [], "failures": [], "error": str(e)}
            target_results.append(r)

    # 汇总所有可播频道，按 URL 去重
    all_channels, seen = [], set()
    for r in target_results:
        for ch in r["channels"]:
            if ch["url"] not in seen:
                seen.add(ch["url"])
                all_channels.append(ch)

    # strict 模式：只保留分片级验证通过的
    if args.strict:
        output_channels = [c for c in all_channels if c["verify_level"] == "segment"]
        print(f"[*] strict 模式: {len(all_channels)} -> {len(output_channels)}")
    else:
        output_channels = all_channels

    # 统计信息
    by_source, by_level = {}, {}
    for c in all_channels:
        by_source[c["name_source"]] = by_source.get(c["name_source"], 0) + 1
        by_level[c["verify_level"]] = by_level.get(c["verify_level"], 0) + 1

    # 生成 M3U 播放列表
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    lines = ["#EXTM3U"]
    for c in output_channels:
        lines.append(f'#EXTINF:-1 group-title="{c["source"]}",{c["name"]}')
        # 带 Referer 的站点，写入 #EXTVLCOPT 让播放器自动带上
        if c.get("referer"):
            lines.append(f'#EXTVLCOPT:http-referrer={c["referer"]}')
        if c.get("user_agent") and c["user_agent"] != DEFAULT_UA:
            lines.append(f'#EXTVLCOPT:http-user-agent={c["user_agent"]}')
        lines.append(c["url"])

    with open(args.output, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # 汇总全局失败原因
    fail_reason_stats = {}
    for r in target_results:
        for f in r.get("failures", []):
            key = f["reason"]
            fail_reason_stats[key] = fail_reason_stats.get(key, 0) + 1

    # 生成报告（含失败原因，便于排查）
    report = {
        "scan_time": datetime.utcnow().isoformat() + "Z",
        "settings": settings,
        "strict_mode": args.strict,
        "targets": [
            {
                "name": r["name"],
                "template": r["template"],
                "total": r["total"],
                "valid": r["valid"],
                "inferred": r["inferred"],
                # 每个目标内联失败原因汇总，valid=0 时看这里
                "failure_reasons": _count_reasons(r.get("failures", [])),
                # 前 10 条失败样本，看具体是哪些 URL 失败、失败原因
                "failure_samples": r.get("failures", [])[:10],
                **({"error": r["error"]} if r.get("error") else {}),
            }
            for r in target_results
        ],
        "total_scanned": sum(r["total"] for r in target_results),
        "total_valid": len(all_channels),
        "total_output": len(output_channels),
        "by_source": by_source,
        "by_verify_level": by_level,
        # 全局失败原因统计，一眼看出整体情况
        "global_failure_reasons": fail_reason_stats,
        "channels": all_channels,
    }
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # 打印最终汇总
    print("=" * 55)
    print(f"[OK] 可用频道: {len(all_channels)}")
    print(f"[OK] 输出频道: {len(output_channels)}")
    print(f"[OK] 名字来源: {by_source}")
    print(f"[OK] 验证级别: {by_level}")
    if fail_reason_stats:
        print(f"[OK] 全局失败原因: {fail_reason_stats}")
    print(f"[OK] 播放列表: {args.output}")
    print(f"[OK] 扫描报告: {args.report}")


# ============================================================
# 常见失败原因对照表（便于排查，不参与运行）
# ============================================================
#
# m3u8失败:HTTP403          服务器拒绝访问。最常见原因是海外 IP 被区域限制，
#                           或缺少 Referer/Cookie。本地跑通常能过。
# m3u8失败:HTTP404          地址不存在。模板替换位置错了，检查 template。
# m3u8失败:超时             连接超时。服务器慢、被墙、或已下线。
# 子列表失败:HTTP404        master playlist 拉到了，但里面的子列表 404。
#                           可能是相对路径拼接问题，或服务器对云端 IP 返回不同内容。
# 分片超时                  m3u8 通，但分片拉不到。多半是限速或 IP 限制。
# 分片类型异常:text/html    服务器返回了 HTML 拦截页而非视频。需要 Cookie 或 IP 被封。
# 分片为空                  分片 HTTP 200 但内容为空。频道未开播或服务器拦截。
# 非m3u8                    返回内容不含 #EXTM3U。地址错或服务器返回跳转页。
# 无分片                    m3u8 有效但没有分片。频道未开播或需要鉴权。
#
# 若某目标 failure_reasons 里绝大多数是 m3u8失败:HTTP403 或 分片超时，
# 基本可确定是 GitHub Actions 的海外 IP 被目标服务器拒绝，
# 本地跑同样代码即可正常扫描。
# ============================================================


if __name__ == "__main__":
    asyncio.run(main())
