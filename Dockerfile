FROM python:3.11-slim
WORKDIR /app
COPY ./requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade -r requirements.txt
COPY . /app
CMD ["python", "programme.py"]