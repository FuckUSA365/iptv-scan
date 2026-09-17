"""
m3u 播放列表验证器
支持输入：本地 m3u 文件路径 或 http(s) URL（含 GitHub raw）
输出：过滤后的可播 m3u + 详细验证报告
"""
import re
import asyncio
import aiohttp
import json
import argparse
import time
from urllib.parse import urljoin, urlparse
from pathlib import Path
from datetime import datetime


DEFAULT_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/120.0.0.0 Safari/537.36")

VALID_CT_PREFIX = ("video/", "audio/", "application/octet-stream",
                   "application/vnd.apple.mpegurl", "binary/octet-stream")


async def fetch_source(session, source, timeout):
    if source.startswith(("http://", "https://")):
        headers = {"User-Agent": DEFAULT_UA, "Accept": "*/*"}
        async with session.get(source, headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=True) as r:
            if r.status != 200:
                raise RuntimeError(f"拉取失败 HTTP{r.status}: {source}")
            data = await r.read()
            return data.decode("utf-8", errors="ignore"), "url"
    else:
        p = Path(source)
        if not p.exists():
            raise FileNotFoundError(f"本地文件不存在: {source}")
        return p.read_text(encoding="utf-8"), "local"


def parse_m3u(text):
    channels = []
    current = _new_channel()

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#EXTM3U"):
            continue
        if line.startswith("#EXTINF"):
            g = re.search(r'group-title="([^"]*)"', line)
            name = line.rsplit(",", 1)[-1].strip() if "," in line else ""
            current["group"] = g.group(1) if g else ""
            current["name"] = name
            current["extinf_raw"] = line
        elif line.startswith("#EXTVLCOPT:http-referrer="):
            current["referer"] = line.split("=", 1)[1].strip()
        elif line.startswith("#EXTVLCOPT:http-user-agent="):
            current["user_agent"] = line.split("=", 1)[1].strip()
        elif line.startswith("#EXTVLCOPT:"):
            current["extra_opts"].append(line)
        elif line.startswith("#"):
            continue
        else:
            current["url"] = line
            if current["url"]:
                channels.append(current)
            current = _new_channel()

    return channels


def _new_channel():
    return {"name": "", "url": "", "referer": None,
            "user_agent": None, "group": "",
            "extinf_raw": "", "extra_opts": []}


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


async def deep_validate(session, channel, default_timeout=10):
    url = channel["url"]
    result = {"playable": False, "verify_level": None, "reason": "",
              "segment_size": 0, "segment_time": 0.0, "resolved_name": ""}

    headers = {"User-Agent": channel.get("user_agent") or DEFAULT_UA,
               "Accept": "*/*", "Connection": "keep-alive"}
    if channel.get("referer"):
        headers["Referer"] = channel["referer"]

    text, err = await fetch_text(session, url, headers, default_timeout)
    if err:
        result["reason"] = f"m3u8失败:{err}"
        return result
    if "#EXTM3U" not in text:
        result["reason"] = "非m3u8"
        return result

    name = extract_name_from_text(text, url)
    current_url = url

    if "#EXT-X-STREAM-INF" in text:
        sub = pick_sub_playlist(text, url)
        if not sub:
            result["reason"] = "master无子列表"
            return result
        sub_text, err = await fetch_text(session, sub, headers, default_timeout)
        if err:
            result["reason"] = f"子列表失败:{err}"
            return result
        text, current_url = sub_text, sub
        n2 = extract_name_from_text(text, sub)
        if n2:
            name = n2

    result["resolved_name"] = name

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
                timeout=aiohttp.ClientTimeout(total=default_timeout)) as r:
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


def write_m3u(channels, output_path, keep_opts=True):
    lines = ["#EXTM3U"]
    for c in channels:
        group = c.get("group") or "已验证"
        name = c.get("name") or c.get("url", "unknown")
        lines.append(f'#EXTINF:-1 group-title="{group}",{name}')
        if keep_opts:
            if c.get("referer"):
                lines.append(f'#EXTVLCOPT:http-referrer={c["referer"]}')
            if c.get("user_agent") and c["user_agent"] != DEFAULT_UA:
                lines.append(f'#EXTVLCOPT:http-user-agent={c["user_agent"]}')
        lines.append(c["url"])
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text("\n".join(lines), encoding="utf-8")


async def run_verify(args):
    concurrency = args.concurrency
    timeout = args.timeout
    semaphore = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession() as session:
        print(f"[*] 读取输入: {args.input}")
        try:
            text, kind = await fetch_source(session, args.input, timeout)
        except Exception as e:
            print(f"读取失败: {e}")
            return
        print(f"[*] 来源类型: {kind}")

        channels = parse_m3u(text)
        print(f"[*] 解析到 {len(channels)} 个频道")
        if not channels:
            print("未解析到任何频道")
            return

        async def limited(ch):
            async with semaphore:
                r = await deep_validate(session, ch, timeout)
                return ch, r

        print(f"[*] 并发验证中（并发数 {concurrency}）...")
        t0 = time.monotonic()
        results = await asyncio.gather(*[limited(c) for c in channels])
        elapsed = time.monotonic() - t0

    verified, failed = [], []
    for ch, r in results:
        merged = dict(ch)
        merged.update({
            "playable": r["playable"],
            "verify_level": r["verify_level"],
            "reason": r["reason"],
            "segment_size": r["segment_size"],
            "segment_time": r["segment_time"],
            "resolved_name": r["resolved_name"],
        })
        if r["resolved_name"] and (not ch["name"] or
                ch["name"].lower().startswith("fallback")):
            merged["name"] = r["resolved_name"]
        (verified if r["playable"] else failed).append(merged)

    if args.strict:
        output_channels = [c for c in verified if c["verify_level"] == "segment"]
    else:
        output_channels = verified

    write_m3u(output_channels, args.output)

    by_level = {}
    for c in verified:
        by_level[c["verify_level"]] = by_level.get(c["verify_level"], 0) + 1

    reason_stats = {}
    for c in failed:
        reason_stats[c["reason"]] = reason_stats.get(c["reason"], 0) + 1

    report = {
        "verify_time": datetime.utcnow().isoformat() + "Z",
        "input": args.input,
        "input_kind": kind,
        "strict_mode": args.strict,
        "concurrency": concurrency,
        "elapsed_seconds": round(elapsed, 2),
        "total": len(channels),
        "playable": len(verified),
        "failed": len(failed),
        "output": len(output_channels),
        "by_verify_level": by_level,
        "failure_reasons": reason_stats,
        "channels": [
            {
                "name": c["name"],
                "group": c["group"],
                "url": c["url"],
                "playable": c["playable"],
                "verify_level": c["verify_level"],
                "reason": c["reason"],
                "segment_time": c["segment_time"],
            } for c in (verified + failed)
        ],
    }
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=" * 55)
    print(f"[OK] 总数: {len(channels)}")
    print(f"[OK] 可播: {len(verified)}  (分片级 {by_level.get('segment', 0)} / "
          f"列表级 {by_level.get('playlist', 0)})")
    print(f"[OK] 不可播: {len(failed)}")
    if reason_stats:
        print(f"[OK] 失败原因: {reason_stats}")
    print(f"[OK] 输出: {args.output}  ({len(output_channels)} 个频道)")
    print(f"[OK] 报告: {args.report}")
    print(f"[OK] 耗时: {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="m3u 播放列表验证器")
    parser.add_argument("--input", required=True,
                        help="本地 m3u 路径 或 http(s) URL")
    parser.add_argument("--output", default="output/verified.m3u",
                        help="过滤后可播列表的输出路径")
    parser.add_argument("--report", default="output/verify_report.json",
                        help="验证报告输出路径")
    parser.add_argument("--concurrency", type=int, default=15)
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--strict", action="store_true",
                        help="只保留分片级验证通过的频道")
    args = parser.parse_args()
    asyncio.run(run_verify(args))


if __name__ == "__main__":
    main()
