"""상품 이미지 수집 파이프라인 — async(httpx · HTTP/2) · 멀티프로세스 · 스레드 미사용.
   시작코드부터 +1 순차 증가, 유효 상품 TARGET개의 메인 상품 이미지(mainImageUrl) 다운로드.
   Producer 1 프로세스(Redis rpush만) + Consumer P 프로세스(각자 asyncio 루프, 코루틴 C개).
   생산자-소비자는 오직 Redis 큐로만 통신. 저장 카운터는 Redis INCR(원자)로 정확히 TARGET개 보장.
   사용법: python3 main.py <START_CODE> <TARGET>  |  python3 main.py (실행 후 시작코드·개수 입력)
           (PROCESSES, CONCURRENCY는 환경변수)
   총 동시성 = PROCESSES × CONCURRENCY. 스레드는 한 개도 쓰지 않는다(asyncio 코루틴)."""
import asyncio
import os
import re
import sys
import time
from multiprocessing import Process
import httpx
import psutil                      # 자원 사용률(CPU/RAM/네트워크) 측정
import redis                       # producer / main: 동기
import redis.asyncio as aioredis   # consumer: 비동기

SAVE_DIR  = os.path.expanduser("~/images")
QUEUE     = "crawl:q"        # 작업 큐(상품코드 문자열)
SAVED     = "crawl:saved"    # 저장 카운터(원자 INCR)
STOP      = "crawl:stop"     # 정지 플래그
RL        = "crawl:rl"       # 429/403/5xx 차단·오류 카운터(동시성 천장 가시화)
FETCHED   = "crawl:fetched"  # 처리한 총 페이지 수(처리율·유효율 측정)
PRODUCED  = "crawl:produced" # 생산자가 큐에 넣은 코드 수(진행 표시용)
GO        = "crawl:go"       # 예열 후 '시작' 신호(이 신호 전까지 소비자 대기)
READY     = "crawl:ready"    # 예열을 끝낸 소비자 수
MAX_QUEUE = 400              # 큐 backpressure — 이만큼 차면 producer가 잠깐 쉼(메모리 보호)
TIMEOUT   = 5
HTTP2     = os.getenv("HTTP2", "1") == "1"   # HTTP/2 멀티플렉싱(t3.micro 실측: 켜는 게 빠름 — 핸드셰이크 절약)

# 수집 대상은 코드에 박지 않고 환경변수로 분리(대상이 바뀌어도 호스트만 바꾸면 재사용 가능).
TARGET_HOST = os.getenv("TARGET_HOST", "www.example.com")      # 수집 대상 호스트
PAGE_PATH   = os.getenv("TARGET_PAGE_PATH", "/goods/{}")       # 상품 상세 경로({}=상품코드)
IMAGE_FIELD = os.getenv("TARGET_IMAGE_FIELD", "mainImageUrl")  # 본문에서 이미지 URL을 담은 JSON 필드명
PAGE_URL    = f"https://{TARGET_HOST}{PAGE_PATH}"
WARMUP_URL  = f"https://{TARGET_HOST}/robots.txt"              # 측정 전 TLS 커넥션 예열용

# 터미널 색(퍼플) — 진행 표시 꾸밈용. 색 미지원 터미널이면 코드만 보일 뿐 동작엔 무관.
C_PURPLE = "\033[38;5;135m"
C_GRAY   = "\033[38;5;240m"
C_BOLD   = "\033[1m"
C_DIM    = "\033[2m"
C_RESET  = "\033[0m"

IMAGE_FIELD_BYTES = b'"' + IMAGE_FIELD.encode() + b'"'            # 빠른 substring 선거름용
MAIN = re.compile(b'"' + IMAGE_FIELD.encode() + rb'":"([^"]+)"')  # 본문에서 이미지 URL 추출(bytes 정규식)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": f"https://{TARGET_HOST}/",
    "Accept-Encoding": "gzip, deflate, br",
}


def valid_image(data, ctype):
    """매직바이트 + content-type로 진짜 이미지 확인(깨짐/에러페이지 거름)."""
    if len(data) < 1024:
        return False
    if ctype and not ctype.lower().startswith("image/"):
        return False
    return (data[:3] == b"\xff\xd8\xff"
            or data[:8] == b"\x89PNG\r\n\x1a\n"
            or (data[:4] == b"RIFF" and data[8:12] == b"WEBP"))


def ext_of(data):
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return ".webp"
    return ".jpg"


# ── Producer: 코드 순차 생성 → Redis 큐 (별도 프로세스, rpush만) ──
def producer(start):
    r = redis.Redis(decode_responses=True)
    code = start
    while not r.get(STOP):
        if r.llen(QUEUE) >= MAX_QUEUE:
            time.sleep(0.02)
            continue
        pipe = r.pipeline(transaction=False)   # 50개씩 묶어 round-trip 절감
        for _ in range(50):
            pipe.rpush(QUEUE, code)
            code += 1
        pipe.execute()
        r.incrby(PRODUCED, 50)                 # 생산량 가시화(진행 표시용)


# ── Consumer 코루틴: 큐 pop → 페이지 → mainImageUrl → 이미지 → 검증 → 저장 ──
async def feeder(r, mq, n_workers):
    """redis 큐 → 메모리 큐. 배치 lpop으로 redis 왕복 최소화
       (코루틴 수백 개가 redis 연결 1개로 lpop을 직렬화하던 병목 제거)."""
    while True:
        codes = await r.lpop(QUEUE, 200)        # 한 번에 200개(redis 6.2+)
        if not codes:
            if await r.get(STOP):
                for _ in range(n_workers):
                    await mq.put(None)          # poison pill로 워커 종료
                return
            await asyncio.sleep(0.003)
            continue
        for c in codes:
            await mq.put(c)


async def worker(client, r, mq, target):
    while True:
        no = await mq.get()
        if no is None:                          # poison pill → 종료
            return
        try:
            page = await client.get(PAGE_URL.format(no))
            await r.incr(FETCHED)
            if page.status_code != 200:
                await r.incr(RL)                 # 429/403/5xx = 차단·오류 → 가시화
                continue
            body = page.content                  # 이미지 URL은 HTML 끝부분(~120KB 지점) → 전체 본문에서 찾는다
            if IMAGE_FIELD_BYTES not in body:    # 빠른 substring 선거름 — 무효(빈 껍데기) 즉시 스킵
                continue
            m = MAIN.search(body)                # mainImageUrl 있는 유효 상품만 정규식
            if not m:
                continue                         # 무효 → 스킵
            img = await client.get(m.group(1).decode())
            if img.status_code != 200:
                await r.incr(RL)
                continue
            data = img.content
            if not valid_image(data, img.headers.get("content-type", "")):
                continue                         # 깨짐/비이미지 → 안 셈
            idx = await r.incr(SAVED)            # 원자 카운터 → 정확히 target개 보장
            if idx > target:
                await r.set(STOP, 1)
                return                           # 초과분 버림 + 정지 전파
            try:
                with open(os.path.join(SAVE_DIR, f"{no}{ext_of(data)}"), "wb") as f:
                    f.write(data)
            except OSError:
                await r.decr(SAVED)              # 저장 실패 → 슬롯 반납(파일=카운터 일치)
                continue
            if idx >= target:
                await r.set(STOP, 1)
                return                           # 마지막 한 장 → 정지
        except Exception:
            continue                             # 일시오류/타임아웃 → 다음 코드로


async def consume(target, concurrency):
    r = aioredis.Redis(decode_responses=True)
    mq = asyncio.Queue(maxsize=concurrency * 4)   # 메모리 큐(backpressure)
    limits = httpx.Limits(max_connections=concurrency,
                          max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(http2=HTTP2, headers=HEADERS, limits=limits,
                                 timeout=TIMEOUT, follow_redirects=True) as client:
        # 예열(측정 시간 밖): TLS 커넥션·이벤트루프를 미리 데워 시동 지연 제거
        await asyncio.gather(*[client.get(WARMUP_URL)
                               for _ in range(min(concurrency, 16))],
                             return_exceptions=True)
        await r.incr(READY)                       # 이 소비자 예열 완료
        while not await r.get(GO):                # main이 '시작'할 때까지 대기
            await asyncio.sleep(0.02)
        fed = asyncio.create_task(feeder(r, mq, concurrency))
        await asyncio.gather(*[worker(client, r, mq, target) for _ in range(concurrency)])
        fed.cancel()
    await r.aclose()


def consumer(target, concurrency):
    # 순수 asyncio 이벤트루프(단일 스레드). uvloop은 내부 스레드풀(~4개)을 띄워서
    # '스레드 금지' 규칙에 걸릴 소지가 있고, CPU(2 vCPU) 천장이라 속도 이득도 0이라 쓰지 않는다.
    asyncio.run(consume(target, concurrency))


if __name__ == "__main__":
    # 인자 파싱은 반드시 이 블록 안에서(Windows spawn 자식이 재파싱하다 죽는 것 방지).
    # 인자로 주면 그대로, 없으면 실행 중에 둘 다 입력받는다.
    #   python3 main.py 1002345678 200   (인자)   |   python3 main.py   (프롬프트 입력)
    if len(sys.argv) > 1:
        START_CODE = int(sys.argv[1])
        TARGET = int(sys.argv[2]) if len(sys.argv) > 2 else 200
    else:
        START_CODE = int(input("시작 코드 입력: ").strip())
        TARGET = int(input("개수 입력 (Enter=200): ").strip() or "200")
    PROCESSES   = int(os.getenv("PROCESSES", "2"))      # consumer 프로세스(2 vCPU 실측 최적)
    CONCURRENCY = int(os.getenv("CONCURRENCY", "128"))  # 프로세스당 코루틴 수

    os.makedirs(SAVE_DIR, exist_ok=True)
    for fn in os.listdir(SAVE_DIR):             # 폴더 비우고 시작 → 항상 새로 TARGET장
        try:
            os.remove(os.path.join(SAVE_DIR, fn))
        except OSError:
            pass
    r = redis.Redis(decode_responses=True)
    r.delete(QUEUE, SAVED, STOP, RL, FETCHED, PRODUCED, GO, READY)

    print(f"\n{C_BOLD}🛒 상품 이미지 수집기{C_RESET}   시작코드 {C_PURPLE}{START_CODE}{C_RESET} "
          f"→ 유효 상품 {C_PURPLE}{C_BOLD}{TARGET}{C_RESET}개")
    print(f"   {C_DIM}🏭 생산자 1  ─▶  📦 Redis 큐  ─▶  🔍 소비자 {PROCESSES}프로세스 × {CONCURRENCY}코루틴{C_RESET}\n")

    # 소비자를 미리 띄워 예열(프로세스·이벤트루프·TLS 커넥션) — 측정 타이머 밖
    cons = [Process(target=consumer, args=(TARGET, CONCURRENCY)) for _ in range(PROCESSES)]
    for c in cons:
        c.start()
    print(f"   {C_DIM}🔥 소비자 예열 중...{C_RESET}", end="", flush=True)
    while int(r.get(READY) or 0) < PROCESSES:
        time.sleep(0.05)
    print(f"\r   {C_PURPLE}🔥 예열 완료 — 풀스피드 대기 중{C_RESET}        ")
    input(f"\n   {C_BOLD}▶  시작하려면 Enter 를 누르세요...{C_RESET}")

    t0 = time.time()                            # ← 타이머 시작(예열은 측정 밖)
    r.set(GO, 1)                                 # 소비자 출발!
    prod = Process(target=producer, args=(START_CODE,))
    prod.start()
    # 종료는 '실제 저장된 파일 수'로 판정 — 카운터(INCR) 기준이면 슬롯 예약 직후·
    # write 직전에 terminate되어 마지막 1장이 잘리는 경합이 생긴다(파일 199/99 버그).
    psutil.cpu_percent()                        # 워밍업(첫 호출은 0 반환)
    nc0 = psutil.net_io_counters()
    net_base = nc0.bytes_sent + nc0.bytes_recv
    last_net, last_net_t = net_base, t0
    ram_total_mb = psutil.virtual_memory().total / (1024 * 1024)
    cpu_hist, ram_hist, net_total_mb, max_net = [], [], 0.0, 0.0
    last, first = 0.0, True
    while len(os.listdir(SAVE_DIR)) < TARGET and any(c.is_alive() for c in cons):
        time.sleep(0.05)
        now = time.time()
        if now - last >= 0.25:                  # 0.25초마다 진행+자원 2줄 갱신(매 작업 print는 X)
            last = now
            saved = int(r.get(SAVED) or 0)
            fetched = int(r.get(FETCHED) or 0)
            produced = int(r.get(PRODUCED) or 0)
            qlen = r.llen(QUEUE)
            invalid = max(0, fetched - saved)   # 페이지는 받았지만 저장 안 된 것(빈 껍데기 = 무효)
            el = now - t0
            rate = fetched / el if el > 0 else 0
            cpu = psutil.cpu_percent()          # 직전 호출 이후 평균 CPU%(2 vCPU 기준)
            ram = psutil.virtual_memory().percent
            nc = psutil.net_io_counters()
            net_now = nc.bytes_sent + nc.bytes_recv
            net_rate = (net_now - last_net) / (now - last_net_t) / 1e6 if now > last_net_t else 0
            last_net, last_net_t = net_now, now
            cpu_hist.append(cpu); ram_hist.append(ram)
            max_net = max(max_net, net_rate)
            net_total_mb = (net_now - net_base) / 1e6
            filled = min(24, saved * 24 // TARGET)
            bar = C_PURPLE + "█" * filled + C_GRAY + "·" * (24 - filled) + C_RESET
            if not first:
                print("\033[2F", end="")         # 커서 2줄 위 + 줄 시작(두 줄 제자리 갱신)
            first = False
            print(f"  [{bar}] {C_BOLD}{saved:>4}{C_RESET}/{TARGET}  "
                  f"🏭{produced:<6} 📦{qlen:<4} 🔍{fetched:<6} ✗{invalid:<6}\033[K")
            print(f"  💻CPU {C_PURPLE}{cpu:>3.0f}%{C_RESET}  🧠RAM {ram:>3.0f}%  "
                  f"🌐{net_rate:>4.1f}MB/s  ⚡{C_PURPLE}{rate:>3.0f}p/s{C_RESET}  ⏱{el:4.1f}s\033[K",
                  flush=True)
    dt = time.time() - t0
    r.set(STOP, 1)
    for c in cons:
        c.terminate()
    prod.terminate()

    saved = min(int(r.get(SAVED) or 0), TARGET)
    n_files = len(os.listdir(SAVE_DIR))
    blocked = int(r.get(RL) or 0)
    fetched = int(r.get(FETCHED) or 0)
    rate = fetched / dt if dt > 0 else 0
    avg_cpu = sum(cpu_hist) / len(cpu_hist) if cpu_hist else 0
    max_cpu = max(cpu_hist) if cpu_hist else 0
    avg_ram = sum(ram_hist) / len(ram_hist) if ram_hist else 0
    max_ram = max(ram_hist) if ram_hist else 0
    print(f"\n{C_BOLD}{C_PURPLE}🎉 {saved}장 수집 완료!{C_RESET}  {C_BOLD}{dt:.2f}s{C_RESET}  ·  "
          f"📦 {fetched}페이지  ⚡ {rate:.0f}p/s  ·  유효율 {100 * saved / max(fetched, 1):.1f}%  ·  "
          f"✗무효 {fetched - saved}  🚫차단 {blocked}")
    print(f"   {C_DIM}📁 {SAVE_DIR}  (파일 {n_files}개){C_RESET}")
    print(f"\n{C_PURPLE}━━━━━━━━━━━━━ 자원 벤치마크 ━━━━━━━━━━━━━{C_RESET}")
    print(f"   💻 CPU      평균 {avg_cpu:>4.0f}%   최대 {max_cpu:>4.0f}%    (vCPU 2개)")
    print(f"   🧠 RAM      평균 {avg_ram:>4.0f}%   최대 {max_ram:>4.0f}%    (총 {ram_total_mb:.0f} MB)")
    print(f"   🌐 네트워크   총 {net_total_mb:>6.1f} MB   최대 {max_net:>4.1f} MB/s")
    print(f"{C_PURPLE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{C_RESET}", flush=True)
    os._exit(0)                                 # multiprocessing 정리 행 방지 — 강제 종료
