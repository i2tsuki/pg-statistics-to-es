FROM library/python:3.6.8-jessie

RUN apt-get -yq update && \
    apt-get -yq install ca-certificates && \
    mkdir -p /pg-statistics-to-es

ADD . /pg-statistics-to-es

WORKDIR /pg-statistics-to-es

RUN pip3 install -r requirements.txt && \
    pip3 install pyinstaller certifi
RUN pyinstaller --onefile ./pg_statistics_to_es/main.py -n pg-statistics-to-es
