<h1 align="center">고처리량 상품 이미지 수집 파이프라인</h1>

<p align="center"><b>광명융합기술교육원 5조</b> · 데이터 파이프라인 팀 미션</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python_3-3776AB?logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Redis-FF4438?logo=redis&logoColor=white" alt="Redis">
  <img src="https://img.shields.io/badge/httpx_HTTP%2F2-2A6DB0?logo=python&logoColor=white" alt="httpx">
  <img src="https://img.shields.io/badge/multiprocessing-4B8BBE?logo=python&logoColor=white" alt="multiprocessing">
  <img src="https://img.shields.io/badge/AWS_EC2-FF9900?logo=amazonec2&logoColor=white" alt="AWS EC2">
  <img src="https://img.shields.io/badge/Amazon_Linux-232F3E?logo=amazonlinux&logoColor=white" alt="Amazon Linux">
</p>

> 단일 EC2 **t3.micro** 한 대에서 상품 상세 페이지를 순회하며 대표 이미지를 **최대 처리율로** 수집하는 생산자–소비자 파이프라인입니다.
> 동시성은 **멀티프로세스**(스레드·코루틴 미사용)로만 내고, 생산자와 소비자는 **Redis 메시지 큐**로만 통신합니다.
> 3인 팀 속도전 미션에서 **채택된 구현**(가장 빠른 버전)을 정리한 저장소입니다.

> **참고** — 수집 대상은 과제에서 지정됐고, 공개 저장소에서는 특정 사이트에 묶이지 않도록 대상 정보를 환경변수(`TARGET_HOST`)로 분리했습니다.

<details open>
<summary><b>목차</b></summary>
<br>

* [1. 미션 개요](#1-미션-개요)
* [2. 팀 — 5조](#2-팀--5조)
* [3. 아키텍처](#3-아키텍처)
* [4. 실행 환경 — t3.micro의 물리적 한계](#4-실행-환경--t3micro의-물리적-한계)
* [5. 엔지니어링: 병목은 어디인가](#5-엔지니어링-병목은-어디인가)
* [6. 최적화: 프로세스 수 조율 · 벤치마크](#6-최적화-프로세스-수-조율--벤치마크)
* [7. 확장성 · 유지보수](#7-확장성--유지보수)
* [8. 실행](#8-실행)
* [9. 디렉터리 구조](#9-디렉터리-구조)

</details>

---

## 1. 미션 개요

시작 상품코드에서 **+1씩 순차 증가**하며 유효한 상품 N개(기본 200)의 대표 이미지를 다운로드합니다. 대상 사이트는 존재하지 않는 코드에도 HTTP 200(빈 껍데기)을 돌려주기 때문에, **"유효 = 응답 본문에 이미지 필드가 존재함"**으로 판정합니다. 큐에 적재된 N개 이상의 상품 이미지를 모두 받으면 시스템이 정상 종료합니다.

필수 조건은 둘이었습니다 — **① 생산자–소비자 분리(Redis로만 통신)**, **② Redis 메시지 큐**. 이 위에서 *제한된 t3.micro 한 대로 얼마나 빠르게* 받느냐가 평가 기준이었습니다.

## 2. 팀 — 5조

3인이 각자 파이프라인을 구현해 같은 조건에서 속도를 겨뤘고, **가장 빠른 구현을 팀 제출본으로 채택**했습니다. 이 저장소는 그 채택된 구현입니다.

| 멤버 | 역할 | 비고 |
| :---: | :--- | :--- |
| <a href="https://github.com/SanghyeokLee-KR"><img src="https://github.com/SanghyeokLee-KR.png" width="64"></a><br>**[이상혁](https://github.com/SanghyeokLee-KR)**<br>*(조장)* | 파이프라인 설계·구현 | **채택된 구현 (본 저장소)** — 멀티프로세스 + Redis 큐, 프로세스 수 튜닝·벤치마크 |
| <a href="https://github.com/nohhyunju0212"><img src="https://github.com/nohhyunju0212.png" width="64"></a><br>**[노현주](https://github.com/nohhyunju0212)** | 5조 팀원 | 속도전 공동 진행 |
| <a href="https://github.com/adieud99"><img src="https://github.com/adieud99.png" width="64"></a><br>**[김연동](https://github.com/adieud99)** | 5조 팀원 | 속도전 공동 진행 |

## 3. 아키텍처

단일 흐름 스크립트로는 한계가 있어, **확장 가능한 파이프라인**으로 설계했습니다.

<p align="center"><img src="docs/01-script-to-architecture.jpg" width="900" alt="스크립트에서 시스템 아키텍처로"></p>

생산자와 소비자를 **완전히 분리**해 비동기적으로 독립 작동시키고,

<p align="center"><img src="docs/02-producer-consumer.jpg" width="900" alt="생산자·소비자 분리"></p>

둘 사이는 **Redis 큐**로만 통신합니다. 생산자는 상품 코드를 큐에 적재하고, 소비자는 준비되는 대로 꺼내(Pop) 처리합니다.

<p align="center"><img src="docs/03-redis-queue.jpg" width="900" alt="Redis 메시지 큐"></p>

```
[Producer 1] ──(상품코드 +1)──▶ [Redis 큐] ──▶ [Consumer × N 프로세스] ──▶ 디스크
```

- 동시성은 **프로세스 수**로만 — 각 프로세스는 동기로 한 번에 한 요청(스레드 0, 코루틴 0).
- Redis `INCR`로 슬롯을 원자적으로 예약해 **정확히 N개**만 저장.
- 종료는 카운터가 아니라 **실제 디스크에 쌓인 파일 수**로 판정(§5의 199/200 경합 방지).

## 4. 실행 환경 — t3.micro의 물리적 한계

모든 파이프라인은 **엄격히 제한된 인프라**(vCPU 2 · RAM 1GB) 위에서 돌아야 했습니다. 브라우저 없이 순수 HTTP만 쓰기 때문에, 병목은 RAM이 아니라 **CPU(TLS 핸드셰이크)·네트워크·CPU 크레딧**으로 옮겨갑니다.

<p align="center"><img src="docs/04-t3micro-limits.jpg" width="900" alt="t3.micro 물리적 한계"></p>

## 5. 엔지니어링: 병목은 어디인가

처리율을 올리려면 병목이 **Network I/O인지 CPU인지** 먼저 가려야 했습니다.

<p align="center"><img src="docs/05-bottleneck.jpg" width="900" alt="병목 찾기"></p>

실측으로 내린 결론:

- **진짜 천장은 코드가 아니라 단일 IP의 rate limit(약 290 page/s)이었습니다.** 프로세스 수·HTTP 버전·동시성을 다 바꿔봐도 처리율은 같은 천장에서 막혔고, 429(차단)는 거의 0 — 끊는 게 아니라 IP 단위로 throttle을 거는 방식이었습니다.
- **소요 시간 ≈ (N ÷ 유효율) ÷ 290.** 유효율(시작 시드가 얼마나 조밀한가)이 시간을 지배합니다. "몇 초 컷" 기록은 코드 실력이 아니라 운 좋은 시드의 결과입니다.
- **멀티프로세스(HTTP/1.1)가 async(HTTP/2)보다 빨랐습니다.** 프로세스당 동기 1요청이 HTTP/2 멀티플렉싱(서버 동시 스트림 제한)보다 동시성 효율이 좋았고, HTTP/2 + 고프로세스 조합은 메모리가 터져 OOM이 났습니다.
- **199/200 버그 해결**: 종료 판정을 카운터 → 파일 수 기준으로 바꿨습니다. 카운터 `INCR` 직후·파일 write 직전에 종료돼 마지막 한 장이 잘리던 경합을 없앴습니다.

## 6. 최적화: 프로세스 수 조율 · 벤치마크

무작정 프로세스를 많이 띄우는 게 정답이 아니었습니다. 처리율이 더 안 오르는 **무릎(knee)** 지점이 최적값입니다.

<p align="center"><img src="docs/06-process-tuning.jpg" width="900" alt="프로세스 수 최적화"></p>

t3.micro(RAM 1GB)에서 프로세스 수(`PROCESSES`)별 실측 (유효율 ~11% 시드 기준):

| PROCESSES | 소요 | RAM(최대) | 메모 |
| :---: | :---: | :---: | :--- |
| 24 | 7.8s | 72% | 여유 |
| 28 | 6.7s | 79% | |
| **32** | **6.0s** | **87%** | ✅ 최적(안전) |
| 36–40 | 불안정 | 90–99% | OOM 칼날 — 스왑 터지면 폭망 |

처리율 천장이 IP인 이상, 32 이상으로 늘려도 이득 없이 OOM 위험만 커집니다. 측정 정확도를 위해 **프로세스 spawn과 TLS 커넥션 예열을 타이머 밖**으로 빼고 'GO' 신호 뒤부터 시간을 쟀습니다.

대표 실행 요약 (P32):

```
🎉 200장 수집 완료!  6.0s  ·  📦 1820페이지  ⚡ 290p/s  ·  유효율 11.0%  ✗무효 1620  🚫차단 0
━━━━━━━━━━━━━ 자원 벤치마크 ━━━━━━━━━━━━━
   💻 CPU      평균   94%   최대  100%    (vCPU 2개)
   🧠 RAM      평균   80%   최대   87%    (총 1009 MB)
   🌐 네트워크   총   ~210 MB   최대 ~30 MB/s
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```
> 소요 시간은 시작 시드의 유효율에 따라 달라집니다(위 시간 모델). 위 수치는 대표 실행 기준입니다.

## 7. 확장성 · 유지보수

요구사항 변경은 필연이라, 기존 시스템을 무너뜨리지 않고 새 기능을 끼워 넣을 수 있게 **모듈화**했습니다. 수집 대상(호스트·경로·이미지 필드)은 코드에 박지 않고 **환경변수로 주입**합니다.

<p align="center"><img src="docs/07-scalability.jpg" width="900" alt="확장성·유지보수"></p>

## 8. 실행

```bash
pip install -r requirements.txt
# Redis 가 localhost:6379 에 떠 있어야 합니다.

export TARGET_HOST="<수집 대상 호스트>"   # 상품 상세가 https://HOST/goods/<코드> 형태라고 가정
export PROCESSES=32                        # 동시성(=프로세스 수)
python3 main.py <START_CODE> <N>           # 인자 없이 실행하면 시작코드·개수를 입력받습니다
```

| 변수 | 기본값 | 설명 |
|---|---|---|
| `TARGET_HOST` | `www.example.com` | 수집 대상 호스트 |
| `TARGET_PAGE_PATH` | `/goods/{}` | 상품 상세 경로(`{}` = 상품코드) |
| `TARGET_IMAGE_FIELD` | `mainImageUrl` | 본문에서 이미지 URL을 담은 JSON 필드명 |
| `PROCESSES` | `32` | 소비자 프로세스 수(=동시성) |
| `HTTP2` | `0` | `1`이면 HTTP/2 사용 |

## 9. 디렉터리 구조

```
high-throughput-image-pipeline/
├── main.py            # 파이프라인 (producer/consumer · Redis 큐 · 자원 모니터)
├── requirements.txt   # httpx[http2] · redis · psutil
├── docs/              # 아키텍처 다이어그램 (팀 미션 브리프 발췌)
└── README.md
```

---

<p align="center"><sub>아키텍처 다이어그램은 팀 미션 브리프(NotebookLM 제작) 슬라이드에서 발췌했습니다.</sub></p>
