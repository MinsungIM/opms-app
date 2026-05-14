# syntax=docker/dockerfile:1.7
# opms: Node 20 + Python 3.11 (prophet/numpy/pandas/psycopg2) + Chromium 단일 컨테이너

# === Stage 1: Node 빌드 (vite client + esbuild server bundle) ===
FROM node:20-bookworm-slim AS node-builder
WORKDIR /app

# Cache: package*.json 변경 없으면 npm ci layer 재사용
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci

COPY . .
RUN npm run build

# === Stage 2: 런타임 ===
FROM node:20-bookworm-slim AS runtime
WORKDIR /app

ENV NODE_ENV=production \
    PYTHONUNBUFFERED=1 \
    PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium \
    CHROMIUM_PATH=/usr/bin/chromium

# 시스템 패키지: Python, Chromium, postgres 클라이언트 lib, 한글 폰트
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 \
      python3-venv \
      build-essential \
      libpq-dev \
      chromium \
      fonts-liberation \
      fonts-noto-cjk \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Python venv에 prophet 등 설치 (PEP 668 우회 + 깔끔한 격리)
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir \
      "numpy>=2.4.3" \
      "pandas>=3.0.1" \
      "prophet>=1.3.0" \
      "psycopg2-binary>=2.9.11"

# Node 프로덕션 의존성만
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --omit=dev

# 빌드 결과
COPY --from=node-builder /app/dist ./dist

# 런타임에 필요한 Python 스크립트
COPY server/python ./server/python

# 빌드 정리: build-essential은 prophet 컴파일 끝나면 제거 (이미지 감량)
RUN apt-get purge -y build-essential libpq-dev \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Non-root 사용자
RUN useradd -r -u 10001 -g nogroup app \
    && chown -R app:nogroup /app /opt/venv
USER app

EXPOSE 5000

# 컨테이너 헬스체크 (compose에서 의존성 정의에 활용)
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:5000/').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))" || exit 1

CMD ["node", "dist/index.cjs"]
