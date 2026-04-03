FROM python:3.9
WORKDIR /app
COPY requirements-py39.txt .
RUN pip install -r requirements-py39.txt
COPY . .
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
