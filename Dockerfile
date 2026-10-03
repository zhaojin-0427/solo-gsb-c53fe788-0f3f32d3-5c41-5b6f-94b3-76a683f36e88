FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

ENV REGISTRY_DATA_DIR=/data \
    REGISTRY_UPLOAD_LEASE_SECONDS=3600 \
    REGISTRY_GC_GRACE_SECONDS=3600

VOLUME /data
EXPOSE 8000

# 可多进程共享同一数据目录：SQLite WAL 串行元数据写，flock 文件锁串行
# manifest 发布与 GC 的跨进程临界区（见 app/coord.py）
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
