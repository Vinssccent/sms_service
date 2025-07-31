FROM python:3.10-slim-bullseye
WORKDIR /app
RUN pip install --no-cache-dir --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY alembic.ini .
COPY alembic /app/alembic
COPY seed_scripts /app/seed_scripts
COPY data /app/data
COPY ./src /app/src

# --- НОВОЕ: Копируем папку с HTML-шаблонами внутрь контейнера ---
COPY templates /app/templates

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
