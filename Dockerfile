FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

#CMD ["python", "main.py"] 修改為gunicorn

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]
