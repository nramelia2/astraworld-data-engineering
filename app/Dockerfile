FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install pandas sqlalchemy pymysql

CMD ["python", "script/load_raw_data.py"]