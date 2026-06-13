"""상품 이미지 수집 파이프라인 — 멀티프로세스 (스레드·코루틴 미사용).

Producer 1개가 상품코드를 +1씩 만들어 Redis 큐에 넣고,
Consumer N개가 큐에서 꺼내 페이지를 받아 이미지를 다운로드한다.
동시성은 '프로세스 수'로만 낸다(각 프로세스는 동기 1요청).
수집 대상은 환경변수(TARGET_HOST)로 분리 — 특정 사이트에 묶이지 않게.

사용법: python3 main.py [START_CODE] [TARGET]
        env: TARGET_HOST(필수), PROCESSES=32, HTTP2=0
"""
import os
import re
import sys
import time
from multiprocessing import Process

import httpx
import psutil
import redis


# ────────────────────────── 설정 ──────────────────────────
# 수집 대상은 코드에 박지 않고 환경변수로 분리(대상이 바뀌어도 호스트만 바꾸면 재사용 가능).
TARGET_HOST = os.getenv("TARGET_HOST", "www.example.com")      # 수집 대상 호스트
PAGE_PATH   = os.getenv("TARGET_PAGE_PATH", "/goods/{}")       # 상품 상세 경로({}=상품코드)
IMAGE_FIELD = os.getenv("TARGET_IMAGE_FIELD", "mainImageUrl")  # 본문에서 이미지 URL을 담은 JSON 필드명

SAVE_DIR  = os.path.expanduser("~/images")
TIMEOUT   = 5
MAX_QUEUE = 2000                                 # 큐 backpressure(소비자보다 빨리 차면 producer 쉼)
PROCESSES = int(os.getenv("PROCESSES", "32"))    # 동시성 = 프로세스 수 (t3.micro RAM 1GB에서 실측 최적)
HTTP2     = os.getenv("HTTP2", "0") == "1"        # 동기 1요청이라 보통 HTTP/1.1이 가벼움

PAGE_URL   = f"https://{TARGET_HOST}{PAGE_PATH}"
WARMUP_URL = f"https://{TARGET_HOST}/robots.txt"  # 측정 전 TLS 커넥션 예열용
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": f"https://{TARGET_HOST}/",
    "Accept-Encoding": "gzip, deflate, br",
}

# 대상 사이트가 없는 코드에도 200(빈 껍데기)을 주므로 '이미지 필드 존재'로 유효 상품을 판정한다.
IMAGE_FIELD_BYTES = b'"' + IMAGE_FIELD.encode() + b'"'         # 빠른 substring 선거름용
IMAGE_URL_RE = re.compile(b'"' + IMAGE_FIELD.encode() + rb'":"([^"]+)"')

# Redis 키 — 프로세스 간 공유 상태(생산자·소비자는 오직 Redis로만 통신).
K_QUEUE    = "crawl:q"
K_SAVED    = "crawl:saved"     # 저장 성공 카운터(원자 INCR)
K_STOP     = "crawl:stop"
K_BLOCKED  = "crawl:rl"
K_FETCHED  = "crawl:fetched"
K_PRODUCED = "crawl:produced"
K_GO       = "crawl:go"        # 예열 후 '출발' 신호
K_READY    = "crawl:ready"
ALL_KEYS   = (K_QUEUE, K_SAVED, K_STOP, K_BLOCKED, K_FETCHED,
              K_PRODUCED, K_GO, K_READY)

C_PURPLE, C_GRAY = "\033[38;5;135m", "\033[38;5;240m"
C_BOLD, C_DIM, C_RESET = "\033[1m", "\033[2m", "\033[0m"


# ────────────────────────── 이미지 헬퍼 ──────────────────────────
def is_valid_image(data: bytes, content_type: str) -> bool:
    """매직바이트 + content-type으로 진짜 이미지인지 검사(깨짐/에러페이지 거름)."""
    if len(data) < 1024:
        return False
    if content_type and not content_type.lower().startswith("image/"):
        return False
    return (data[:3] == b"\xff\xd8\xff"
            or data[:8] == b"\x89PNG\r\n\x1a\n"
            or (data[:4] == b"RIFF" and data[8:12] == b"WEBP"))


def image_extension(data: bytes) -> str:
    """매직바이트로 확장자 결정 → 내용과 확장자가 항상 일치."""
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return ".webp"
    return ".jpg"


def new_redis() -> redis.Redis:
    return redis.Redis(decode_responses=True)


# ────────────────────────── Producer ──────────────────────────
def producer(start_code: int) -> None:
    """start_code부터 +1씩 코드를 만들어 Redis 큐에 넣는다. STOP이 서면 종료."""
    r = new_redis()
    code = start_code
    while not r.get(K_STOP):
        if r.llen(K_QUEUE) >= MAX_QUEUE:
            time.sleep(0.02)
            continue
        pipe = r.pipeline(transaction=False)       # 50개씩 묶어 Redis 왕복 절감
        for _ in range(50):
            pipe.rpush(K_QUEUE, code)
            code += 1
        pipe.execute()
        r.incrby(K_PRODUCED, 50)


# ────────────────────────── Consumer ──────────────────────────
def save_one(client: httpx.Client, r: redis.Redis, code: str, target: int):
    """코드 하나 처리: 페이지→이미지URL→이미지→검증→저장.
       반환: True(저장) / False(무효·실패) / "stop"(목표 도달)."""
    page = client.get(PAGE_URL.format(code))
    r.incr(K_FETCHED)
    if page.status_code != 200:
        r.incr(K_BLOCKED)
        return False

    body = page.content
    if IMAGE_FIELD_BYTES not in body:              # 무효(빈 껍데기) 빠른 스킵
        return False
    match = IMAGE_URL_RE.search(body)
    if not match:
        return False

    img = client.get(match.group(1).decode())
    if img.status_code != 200:
        r.incr(K_BLOCKED)
        return False
    data = img.content
    if not is_valid_image(data, img.headers.get("content-type", "")):
        return False

    slot = r.incr(K_SAVED)                          # 원자적 슬롯 예약 → 정확히 target개만 저장
    if slot > target:
        r.set(K_STOP, 1)
        return "stop"
    try:
        with open(os.path.join(SAVE_DIR, f"{code}{image_extension(data)}"), "wb") as f:
            f.write(data)
    except OSError:
        r.decr(K_SAVED)                            # 저장 실패 → 슬롯 반납(파일=카운터 일치)
        return False
    return "stop" if slot >= target else True


def consumer(target: int) -> None:
    """큐에서 코드를 꺼내 save_one을 반복(별도 프로세스). STOP이 서면 종료."""
    r = new_redis()
    client = httpx.Client(http2=HTTP2, headers=HEADERS,
                          timeout=TIMEOUT, follow_redirects=True)
    try:
        client.get(WARMUP_URL)                     # 예열: TLS 커넥션 미리(측정 밖)
    except Exception:
        pass
    r.incr(K_READY)
    while not r.get(K_GO):                          # main의 '출발' 신호까지 대기
        time.sleep(0.02)

    while not r.get(K_STOP):
        code = r.lpop(K_QUEUE)
        if code is None:
            time.sleep(0.003)
            continue
        try:
            if save_one(client, r, code, target) == "stop":
                break
        except Exception:
            continue                               # 일시 오류·타임아웃 → 다음 코드
    client.close()


# ────────────────────── 실행 준비/종료 헬퍼 ──────────────────────
def read_args():
    """인자(START_CODE, TARGET)를 읽는다. 없으면 입력받음(main에서만 호출)."""
    if len(sys.argv) > 1:
        start_code = int(sys.argv[1])
        target = int(sys.argv[2]) if len(sys.argv) > 2 else 200
    else:
        start_code = int(input("시작 코드 입력: ").strip())
        target = int(input("개수 입력 (Enter=200): ").strip() or "200")
    return start_code, target


def reset_state(r: redis.Redis) -> None:
    """저장 폴더를 비우고 Redis 공유 상태를 초기화."""
    os.makedirs(SAVE_DIR, exist_ok=True)
    for name in os.listdir(SAVE_DIR):
        try:
            os.remove(os.path.join(SAVE_DIR, name))
        except OSError:
            pass
    r.delete(*ALL_KEYS)


def start_consumers(target: int):
    """consumer 프로세스 PROCESSES개를 띄운다(측정 타이머 밖)."""
    procs = [Process(target=consumer, args=(target,)) for _ in range(PROCESSES)]
    for p in procs:
        p.start()
    return procs


def wait_until_ready(r: redis.Redis) -> None:
    """모든 consumer가 준비(READY)될 때까지 대기."""
    while int(r.get(K_READY) or 0) < PROCESSES:
        time.sleep(0.05)


def stop_all(r: redis.Redis, consumers, producer_proc) -> None:
    """정지 신호를 세우고 모든 자식 프로세스를 종료."""
    r.set(K_STOP, 1)
    for p in consumers:
        p.terminate()
    producer_proc.terminate()


# ────────────────────── 진행·자원 모니터 ──────────────────────
class ResourceMonitor:
    """실행 중 CPU/RAM/네트워크를 표본 수집하고 진행 상황을 2줄로 갱신."""

    def __init__(self, target: int, t0: float):
        self.target, self.t0 = target, t0
        self.cpu_samples, self.ram_samples = [], []
        self.max_net_mbps = 0.0
        self.net_total_mb = 0.0
        self.ram_total_mb = psutil.virtual_memory().total / (1024 * 1024)
        psutil.cpu_percent()                       # 첫 호출은 0 → 미리 보정
        net = psutil.net_io_counters()
        self._net_base = net.bytes_sent + net.bytes_recv
        self._last_net, self._last_net_t = self._net_base, t0
        self._first_render = True

    def update(self, r: redis.Redis) -> None:
        now = time.time()
        saved = int(r.get(K_SAVED) or 0)
        fetched = int(r.get(K_FETCHED) or 0)
        produced = int(r.get(K_PRODUCED) or 0)
        qlen = r.llen(K_QUEUE)
        invalid = max(0, fetched - saved)
        elapsed = now - self.t0
        page_rate = fetched / elapsed if elapsed > 0 else 0

        cpu = psutil.cpu_percent()
        ram = psutil.virtual_memory().percent
        net = psutil.net_io_counters()
        net_now = net.bytes_sent + net.bytes_recv
        net_mbps = ((net_now - self._last_net) / (now - self._last_net_t) / 1e6
                    if now > self._last_net_t else 0)
        self._last_net, self._last_net_t = net_now, now

        self.cpu_samples.append(cpu)
        self.ram_samples.append(ram)
        self.max_net_mbps = max(self.max_net_mbps, net_mbps)
        self.net_total_mb = (net_now - self._net_base) / 1e6

        self._render(saved, produced, qlen, fetched, invalid,
                     cpu, ram, net_mbps, page_rate, elapsed)

    def _render(self, saved, produced, qlen, fetched, invalid,
                cpu, ram, net_mbps, page_rate, elapsed) -> None:
        filled = min(24, saved * 24 // self.target)
        bar = C_PURPLE + "█" * filled + C_GRAY + "·" * (24 - filled) + C_RESET
        if not self._first_render:
            print("\033[2F", end="")               # 커서 2줄 위로 → 제자리 갱신
        self._first_render = False
        print(f"  [{bar}] {C_BOLD}{saved:>4}{C_RESET}/{self.target}  "
              f"🏭{produced:<6} 📦{qlen:<4} 🔍{fetched:<6} ✗{invalid:<6}\033[K")
        print(f"  💻CPU {C_PURPLE}{cpu:>3.0f}%{C_RESET}  🧠RAM {ram:>3.0f}%  "
              f"🌐{net_mbps:>4.1f}MB/s  ⚡{C_PURPLE}{page_rate:>3.0f}p/s{C_RESET}  "
              f"⏱{elapsed:4.1f}s\033[K", flush=True)


def run_until_done(r: redis.Redis, consumers, monitor: ResourceMonitor) -> None:
    """종료 판정은 카운터가 아니라 '실제 저장된 파일 수'로 한다(199개 경합 방지)."""
    last_render = 0.0
    while (len(os.listdir(SAVE_DIR)) < monitor.target
           and any(p.is_alive() for p in consumers)):
        time.sleep(0.05)
        if time.time() - last_render >= 0.25:      # 0.25초마다만 갱신(측정 부하 최소화)
            last_render = time.time()
            monitor.update(r)


# ────────────────────────── 출력 ──────────────────────────
def print_header(start_code: int, target: int) -> None:
    print(f"\n{C_BOLD}🛒 상품 이미지 수집기 (멀티프로세스){C_RESET}   "
          f"시작코드 {C_PURPLE}{start_code}{C_RESET} → 유효 상품 "
          f"{C_PURPLE}{C_BOLD}{target}{C_RESET}개")
    print(f"   {C_DIM}🏭 생산자 1  ─▶  📦 Redis 큐  ─▶  "
          f"🔍 소비자 {PROCESSES}프로세스 (각자 동기 1요청){C_RESET}\n")


def print_summary(r: redis.Redis, monitor: ResourceMonitor, elapsed: float) -> None:
    saved = min(int(r.get(K_SAVED) or 0), monitor.target)
    files = len(os.listdir(SAVE_DIR))
    blocked = int(r.get(K_BLOCKED) or 0)
    fetched = int(r.get(K_FETCHED) or 0)
    page_rate = fetched / elapsed if elapsed > 0 else 0
    avg = lambda xs: sum(xs) / len(xs) if xs else 0

    print(f"\n{C_BOLD}{C_PURPLE}🎉 {saved}장 수집 완료!{C_RESET}  "
          f"{C_BOLD}{elapsed:.2f}s{C_RESET}  ·  📦 {fetched}페이지  "
          f"⚡ {page_rate:.0f}p/s  ·  유효율 {100 * saved / max(fetched, 1):.1f}%  ·  "
          f"✗무효 {fetched - saved}  🚫차단 {blocked}")
    print(f"   {C_DIM}📁 {SAVE_DIR}  (파일 {files}개){C_RESET}")
    print(f"\n{C_PURPLE}━━━━━━━━━━━━━ 자원 벤치마크 ━━━━━━━━━━━━━{C_RESET}")
    print(f"   💻 CPU      평균 {avg(monitor.cpu_samples):>4.0f}%   "
          f"최대 {max(monitor.cpu_samples or [0]):>4.0f}%    (vCPU 2개)")
    print(f"   🧠 RAM      평균 {avg(monitor.ram_samples):>4.0f}%   "
          f"최대 {max(monitor.ram_samples or [0]):>4.0f}%    (총 {monitor.ram_total_mb:.0f} MB)")
    print(f"   🌐 네트워크   총 {monitor.net_total_mb:>6.1f} MB   "
          f"최대 {monitor.max_net_mbps:>4.1f} MB/s")
    print(f"{C_PURPLE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{C_RESET}", flush=True)


# ────────────────────────── 메인 흐름 ──────────────────────────
def main() -> None:
    start_code, target = read_args()
    r = new_redis()
    reset_state(r)
    print_header(start_code, target)

    consumers = start_consumers(target)            # 측정 전: 프로세스 spawn + 예열
    print(f"   {C_DIM}🔥 소비자 {PROCESSES}개 예열 중...{C_RESET}", end="", flush=True)
    wait_until_ready(r)
    print(f"\r   {C_PURPLE}🔥 예열 완료 — 풀스피드 대기 중{C_RESET}            ")
    input(f"\n   {C_BOLD}▶  시작하려면 Enter 를 누르세요...{C_RESET}")

    t0 = time.time()                               # ← 타이머 시작(예열은 이미 끝남)
    r.set(K_GO, 1)
    producer_proc = Process(target=producer, args=(start_code,))
    producer_proc.start()

    monitor = ResourceMonitor(target, t0)
    run_until_done(r, consumers, monitor)
    elapsed = time.time() - t0

    stop_all(r, consumers, producer_proc)
    print_summary(r, monitor, elapsed)
    os._exit(0)                                    # multiprocessing 정리 행(멈춤) 방지


if __name__ == "__main__":
    main()
