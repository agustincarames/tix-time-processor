FROM python:3-slim

# Install app dependencies
RUN apt-get update && apt-get install -y \
	gcc

WORKDIR /root/tix-time-processor
COPY requirements.txt .
COPY setup.py .
COPY run.sh .
RUN pip install -r requirements.txt

# Bundle app source
ENV PROCESSOR_TYPE=STANDALONE
ENV CELERY_BEAT_SCHEDULE_DIR=/tmp/celerybeat-schedule.d
ENV CELERY_LOG_LEVEL=INFO
ENV DEBIAN_FRONTEND noninteractive

RUN mkdir -p $CELERY_BEAT_SCHEDULE_DIR

WORKDIR /root/tix-time-processor
COPY processor processor/
COPY wait-for-it.sh .
CMD ["./run.sh"]
