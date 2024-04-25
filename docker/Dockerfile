FROM python:3.12-alpine

WORKDIR .
COPY requirements.txt requirements.txt

RUN pip install --upgrade pip

RUN pip install -r requirements.txt
COPY ../ .

ENV FLASK_APP=app_main.py
ENV TZ=America/Sao_Paulo

ENV PYDB_ORCL_NAME=db_origin
ENV PYDB_ORCL_HOST=127.0.0.1
ENV PYDB_ORCL_PORT=5432
ENV PYDB_ORCL_USER=admin
ENV PYDB_ORCL_PWD=P123456,

ENV PYDB_ORCL_NAME=db_dest
ENV PYDB_ORCL_HOST=127.0.0.1
ENV PYDB_ORCL_PORT=2000
ENV PYDB_ORCL_USER=admin
ENV PYDB_ORCL_PWD=admin

ENV PYDB_LOGGING_FILE_MODE=a
ENV PYDB_LOGGING_FILE_PATH=/tmp/pydbrave.log
ENV PYDB_LOGGING_LEVEL=debug

EXPOSE 5000
CMD ["python", "-m" , "flask", "run", "--host=0.0.0.0", "--port=5000"]
