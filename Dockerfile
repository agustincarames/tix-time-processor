FROM ubuntu:xenial
MAINTAINER Facundo Martinez <fnmartinez88@gmail.com>
ARG PROCESSOR_TYPE=STANDALONE
ARG CELERY_BEAT_SCHEDULE_DIR=/tmp/celerybeat-schedule.d
ARG CELERY_LOG_LEVEL=INFO
ENV PROCESSOR_TYPE=$PROCESSOR_TYPE
ENV CELERY_BEAT_SCHEDULE_DIR=$CELERY_BEAT_SCHEDULE_DIR
ENV CELERY_LOG_LEVEL=$CELERY_LOG_LEVEL
ENV DEBIAN_FRONTEND noninteractive

# Installing basic utils for software language installation and logging
RUN apt-get update -y \
	&& apt-get install -y \
			software-properties-common \
			curl

RUN apt-get update \
    && apt-get install -y \
    curl \
    python3 \
    python3-dev \
    python3-pip

RUN mkdir /root/tix-time-processor
RUN mkdir /tmp/celerybeat-schedule.d
COPY processor/ /root/tix-time-processor
COPY setup.py /root/tix-time-processor
COPY requirements.txt /root/tix-time-processor
RUN pip3 install -r requirements.txt

ENTRYPOINT ["./run.sh"]