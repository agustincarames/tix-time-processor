FROM python:3-slim

# Install app dependencies
RUN apt-get update && apt-get install -y \
	gcc

WORKDIR /root/tix-time-processor
COPY requirements.txt .
COPY setup.py .
RUN pip install -r requirements.txt

# Bundle app source
ENV DEBIAN_FRONTEND noninteractive

WORKDIR /root/tix-time-processor
COPY processor processor/
COPY main.py .
COPY wait-for-it.sh .
COPY run.sh .
CMD ["./run.sh"]
