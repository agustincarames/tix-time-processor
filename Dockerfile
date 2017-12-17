FROM python:3.4.7-slim
MAINTAINER Facundo Martinez <fnmartinez88@gmail.com>
ENV PROCESSOR_TYPE=STANDALONE
ENV CELERY_BEAT_SCHEDULE_DIR=/tmp/celerybeat-schedule.d
ENV CELERY_LOG_LEVEL=INFO
ENV DEBIAN_FRONTEND noninteractive

RUN mkdir -p /root/tix-time-processor/processor
RUN mkdir -p $CELERY_BEAT_SCHEDULE_DIR
COPY processor /root/tix-time-processor/processor
COPY setup.py /root/tix-time-processor
COPY requirements.txt /root/tix-time-processor
COPY run.sh /root/tix-time-processor
WORKDIR /root/tix-time-processor
RUN pip3 install -r requirements.txt

ENTRYPOINT ["./run.sh"]