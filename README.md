# tix-time-processing
[![Build Status](https://travis-ci.org/TiX-measurements/tix-time-processor.svg?branch=master)](https://travis-ci.org/TiX-measurements/tix-time-processor)
[![codecov](https://codecov.io/gh/TiX-measurements/tix-time-processor/branch/master/graph/badge.svg)](https://codecov.io/gh/TiX-measurements/tix-time-processor)

This is the `tix-time-processor` microservice. It's main task is to recurrently process the data packets of every 
installation for each user, and post the data collected to the `tix-api`. It alsa uses the IP to AS service, to localize
the source of the packets.

It uses a Celery app to schedule the processing every 10 minutes. That is why it should be connected to the RabbitMQ 
service in the server.

## Installation 
**Currently in dev. Installation instructions are a wishful thinkng right now**  

The `tix-time-processor` is intended to be used a Docker container. So to use it, you just need to download the latest image 
and run it with docker.

It is necessary to create two volumes. One which will contain the reports from the `tix-time-condenser` 
