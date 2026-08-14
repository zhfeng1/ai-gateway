FROM px.zhhf.de/python:3.12-slim

ARG APP_COMMIT=unknown

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    APP_COMMIT=${APP_COMMIT}

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 20000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "20000", "--proxy-headers"]
