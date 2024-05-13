FROM python:3.12-alpine

WORKDIR .
COPY requirements.txt requirements.txt

# install access to SQLServer
RUN apk update
RUN apk add curl
RUN apk add make
RUN apk add gcc
RUN apk add g++
RUN curl -O https://download.microsoft.com/download/1/f/f/1fffb537-26ab-4947-a46a-7a45c27f6f77/msodbcsql18_18.2.2.1-1_amd64.apk && \
    curl -O https://download.microsoft.com/download/1/f/f/1fffb537-26ab-4947-a46a-7a45c27f6f77/mssql-tools18_18.2.1.1-1_amd64.apk && \
    apk add --allow-untrusted msodbcsql18_18.2.2.1-1_amd64.apk && \
    apk add --allow-untrusted mssql-tools18_18.2.1.1-1_amd64.apk
RUN apk add unixodbc-dev
ENV PATH="$PATH:/opt/mssql-tools/bin"

# install Oracle client
RUN apk --no-cache add libaio libnsl libc6-compat curl && \
    cd /tmp && \
    curl -o instantclient-basiclite.zip https://download.oracle.com/otn_software/linux/instantclient/2114000/instantclient-basic-linux.x64-21.14.0.0.0dbru.zip -SL && \
    unzip instantclient-basiclite.zip && \
    mv instantclient*/ /usr/lib/instantclient && \
    rm instantclient-basiclite.zip && \
    ln -s /usr/lib/instantclient/libclntsh.so.21.1 /usr/lib/libclntsh.so && \
    ln -s /usr/lib/instantclient/libocci.so.21.1 /usr/lib/libocci.so && \
    ln -s /usr/lib/instantclient/libociicus.so /usr/lib/libociicus.so && \
    ln -s /usr/lib/instantclient/libnnz21.so /usr/lib/libnnz21.so && \
    ln -s /usr/lib/libnsl.so.2 /usr/lib/libnsl.so.1 && \
    ln -s /lib/libc.so.6 /usr/lib/libresolv.so.2 && \
    ln -s /lib64/ld-linux-x86-64.so.2 /usr/lib/ld-linux-x86-64.so.2
ENV LD_LIBRARY_PATH /usr/lib/instantclient

RUN pip install --upgrade pip

RUN pip install -r requirements.txt
COPY . .

ENV FLASK_APP=app_main.py
ENV TZ=America/Sao_Paulo

ENV PYDB_LOGGING_FILE_MODE=a
ENV PYDB_LOGGING_FILE_PATH=/tmp/pydbref.log
ENV PYDB_LOGGING_LEVEL=debug

EXPOSE 1521
EXPOSE 5432
EXPOSE 5000
CMD ["python", "-m" , "flask", "run", "--host=0.0.0.0", "--port=5000"]
