"""
m3u8 频道扫描器 — 深度验证版
读取 targets.yaml，批量扫描并深度验证每个频道，输出可播放的 M3U 列表。
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


DEFAULT_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/120.0.0.0 Safari/537.36")

VALID_CT_PREFIX = ("video/", "audio/", "application/octet-stream",
                   "application/vnd.apple.mpegurl", "binary/octet-stream")


# ---------- 频道名映射 ----------

def load_channel_map(path="channel_map.yaml"):
    if not Path(path).exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return {str(k): v for k, v in data.items()}


# ---------- 模板推断 ----------

def extract_digit_segments(url):
    parsed = urlparse(url)
    tail_start = len(f"{parsed.scheme}://{parsed.netloc}")
    tail = url[tail_start:]
    return [(m.group(), tail_start + m.start(), tail_start + m.end())
            for m in re.finditer(r'\d+', tail)]


def resolve_template(target):
    """返回 (template, note)"""
    base_url = target["base_url"]

    if target.get("template"):
        return target["template"], "手动模板"

    if target.get("id_regex"):
        regex = target["id_regex"]
        m = re.search(regex, base_url)
        if not m or m.lastindex is None:
            raise ValueError(f"id_regex 未匹配到有效捕获组: {regex}")
        s, e = m.start(1), m.end(1)
        return base_url[:s] + "{id}" + base_url[e:], f"正则 {regex}"

    if target.get("id_segment") is not None:
        idx = int(target["id_segment"]) - 1
        segs = extract_digit_segments(base_url)
        if idx < 0 or idx >= len(segs):
            raise ValueError(
                f"id_segment={idx+1} 超范围，该 URL 只有 {len(segs)} 个数字片段: "
                f"{[s[0] for s in segs]}"
            )
        num, s, e = segs[idx]
        return base_url[:s] + "{id}" + base_url[e:], f"片段{idx+1}={num}"

    segs = extract_digit_segments(base_url)
    if not segs:
        return base_url, "无数字片段，仅验证原地址"

    candidates = [s for s in segs
                  if not (len(s[0]) == 4 and s[0].startswith(("19", "20")))]
    if not candidates:
        candidates = segs
    num, s, e = candidates[-1]
    template = base_url[:s] + "{id}" + base_url[e:]
    return template, f"自动推断替换 '{num}'（如不对请显式设置 template）"


# ---------- HTTP 工具 ----------

async def fetch_text(session, url, headers, timeout, max_bytes=16384):
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
        return None, str(e)[:40]


# ---------- 解析工具 ----------

def extract_name_from_text(text, url):
    m = re.search(r'#EXTINF:[^\n]*?,(.+)', text)
    if m:
        n = m.group(1).strip()
        if n and n.lower() not in ("live", "stream", "unknown"):
            return n
    m = re.search(r'#EXT-X-STREAM-INF:[^\n]*?NAME="([^"]+)"', text)
    return m.group(1).strip() if m else ""


def pick_sub_playlist(master_text, base_url):
    lines = master_text.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF") and i + 1 < len(lines):
            nxt = lines[i + 1].strip()
            if nxt and not nxt.startswith("#"):
                return urljoin(base_url, nxt)
    return None


def pick_first_segment(media_text, base_url):
    for line in media_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if any(line.endswith(ext) for ext in
               (".ts", ".m4s", ".aac", ".mp4", ".m4a")) or "?" in line:
            return urljoin(base_url, line)
    return None


def extract_path_name(url):
    m = re.search(r'/([a-zA-Z][a-zA-Z0-9_-]{1,30})(?:/|\.m3u8)', urlparse(url).path)
    return m.group(1) if m else ""


def lookup_channel_map(url, channel_map):
    for num in re.findall(r'\d+', urlparse(url).path):
        if num in channel_map:
            return channel_map[num]
    return ""


# ---------- 深度验证 ----------

async def deep_validate(session, m3u8_url, headers, timeout, channel_map):
    result = {"playable": False, "verify_level": None, "name": "",
              "name_source": "", "reason": "", "segment_size": 0,
              "segment_time": 0.0}

    text, err = await fetch_text(session, m3u8_url, headers, timeout)
    if err:
        result["reason"] = f"m3u8失败:{err}"
        return result
    if "#EXTM3U" not in text:
        result["reason"] = "非m3u8"
        return result

    name = extract_name_from_text(text, m3u8_url)
    name_source = "extinf" if name else ""

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
        n2 = extract_name_from_text(text, sub)
        if n2:
            name, name_source = n2, "extinf"

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

    segment = pick_first_segment(text, current_url)
    if not segment:
        if "#EXTINF" in text:
            result.update({"playable": True, "verify_level": "playlist",
                           "reason": "仅列表级(无分片可验)"})
            return result
        result["reason"] = "无分片"
        return result

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
            chunk = await r.content.read(4096)
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


# ---------- 扫描单个目标 ----------

async def scan_target(session, target, settings, semaphore, channel_map):
    name = target.get("name", "未命名")
    base_url = target["base_url"]
    start = int(target.get("start", 1))
    end = int(target.get("end", 100))
    referer = target.get("referer", "")
    user_agent = target.get("user_agent") or settings.get("user_agent") or DEFAULT_UA
    timeout = settings.get("timeout", 10)

    headers = {"User-Agent": user_agent, "Accept": "*/*",
               "Connection": "keep-alive"}
    if referer:
        headers["Referer"] = referer

    try:
        template, note = resolve_template(target)
    except ValueError as e:
        print(f"[{name}] 模板解析失败: {e}")
        return {"name": name, "template": "", "total": 0, "valid": 0,
                "inferred": False, "channels": [], "error": str(e)}

    inferred = "自动推断" in note
    print(f"[{name}] 模板: {template}")
    print(f"[{name}] 说明: {note}")

    urls = ([template.replace("{id}", str(i)) for i in range(start, end + 1)]
            if "{id}" in template else [base_url])

    print(f"[{name}] 候选数: {len(urls)}")

    async def limited(u):
        async with semaphore:
            return await deep_validate(session, u, headers, timeout, channel_map)

    results = await asyncio.gather(*[limited(u) for u in urls])

    valid = []
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

    seg_count = sum(1 for v in valid if v["verify_level"] == "segment")
    print(f"[{name}] 可播: {len(valid)}/{len(urls)} (分片级 {seg_count})")

    return {"name": name, "template": template, "total": len(urls),
            "valid": len(valid), "inferred": inferred, "channels": valid}


# ---------- 主流程 ----------

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

    async with aiohttp.ClientSession() as session:
        target_results = []
        for t in targets:
            try:
                r = await scan_target(session, t, settings, semaphore, channel_map)
            except Exception as e:
                print(f"[{t.get('name', '?')}] 扫描异常: {e}")
                r = {"name": t.get("name", "?"), "template": "",
                     "total": 0, "valid": 0, "inferred": False,
                     "channels": [], "error": str(e)}
            target_results.append(r)

    all_channels, seen = [], set()
    for r in target_results:
        for ch in r["channels"]:
            if ch["url"] not in seen:
                seen.add(ch["url"])
                all_channels.append(ch)

    if args.strict:
        output_channels = [c for c in all_channels if c["verify_level"] == "segment"]
        print(f"[*] strict 模式: {len(all_channels)} -> {len(output_channels)}")
    else:
        output_channels = all_channels

    by_source, by_level = {}, {}
    for c in all_channels:
        by_source[c["name_source"]] = by_source.get(c["name_source"], 0) + 1
        by_level[c["verify_level"]] = by_level.get(c["verify_level"], 0) + 1

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    lines = ["#EXTM3U"]
    for c in output_channels:
        lines.append(f'#EXTINF:-1 group-title="{c["source"]}",{c["name"]}')
        if c.get("referer"):
            lines.append(f'#EXTVLCOPT:http-referrer={c["referer"]}')
        if c.get("user_agent") and c["user_agent"] != DEFAULT_UA:
            lines.append(f'#EXTVLCOPT:http-user-agent={c["user_agent"]}')
        lines.append(c["url"])

    with open(args.output, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    report = {
        "scan_time": datetime.utcnow().isoformat() + "Z",
        "settings": settings,
        "strict_mode": args.strict,
        "targets": [
            {"name": r["name"], "template": r["template"],
             "total": r["total"], "valid": r["valid"],
             "inferred": r["inferred"],
             **({"error": r["error"]} if r.get("error") else {})}
            for r in target_results
        ],
        "total_scanned": sum(r["total"] for r in target_results),
        "total_valid": len(all_channels),
        "total_output": len(output_channels),
        "by_source": by_source,
        "by_verify_level": by_level,
        "channels": all_channels,
    }
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("=" * 55)
    print(f"[OK] 可用频道: {len(all_channels)}")
    print(f"[OK] 输出频道: {len(output_channels)}")
    print(f"[OK] 名字来源: {by_source}")
    print(f"[OK] 验证级别: {by_level}")
    print(f"[OK] 播放列表: {args.output}")
    print(f"[OK] 扫描报告: {args.report}")


if __name__ == "__main__":
    asyncio.run(main())
