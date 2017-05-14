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

It is necessary to create two volumes. One which will contain the reports from the `tix-time-condenser`. That is how this two 
micro-services communicate with each other.   
The other volume is the one which will contain the scheduler file.
  
## Configurations

The `tix-time-processor` uses a handful of environment variables to run. Most of them have defaults which are the bare 
minimum to be executed. But some of the most important not.

  * `TIX_REPORTS_BASE_PATH`: The path, either in the container or in the machine where the reports are found. (**Default**: '/tmp/reports')
  * `TIX_RABBITMQ_USER`: RabbitMQ user ()needed by Celery) (**Default**: 'guest')
  * `TIX_RABBITMQ_PASS`: RabbitMQ password (needed by Celery) (**Default**: 'guest')
  * `TIX_RABBITMQ_HOST`: RabbitMQ host (needed by Celery) (**Default**: 'localhost')
  * `TIX_RABBITMQ_PORT`: RabbitMQ port (needed by Celery) (**Default**: 5672)
  * `TIX_API_SSL`: If this environment variable has any value whatsoever (i.e.: `True`, `False`, `-1`, `sarasa`) then 
   HTTPS protocol communication with the API will be enabled. If left undefined or empty, it will use HTTP alone.  (**Default**: _Empty_)
  * `TIX_API_HOST`: The API host where is located (**Default**: 'localhost')
  * `TIX_API_PORT`: The API port in the host where is located(**Default**: 80 for HTTP, 443 for HTTPS)
  * `TIX_API_USER`: The API username for the `tix-time-processor` that is used to authenticate. If left empty, the POST 
  request wont be effectuated and the result will be left in the failed results' directory for the installation. (**Default**: _Epmty_)
  * `TIX_API_PASSWORD`: The API password for the `tix-time-processor` that is used to authenticate. If left empty, the 
  POST request wont be effectuated and the result will be left in the failed results' directory for the installation. (**Default**: _Empty_)
    
## How to run it

This a Celery scheduled app and has three modes of running.

  * As a standalone app
  * As a scheduler
  * As a worker
  
When the service is run in standalone mode, it will schedule and execute the tasks on its own. But since this is not the 
method preferred for production environment given the many complications it would have as a distributed service, the beat
and worker mode exist. The scheduler mode will serialize the tasks and queue them in the RabbitMQ Service. While the 
workers will take each of the serialized tasks and execute them. It is important to note that while many workers instances
may exists at the same time, only one scheduler instance must exists, because having more than one may derive in concurrency
problems. This is the same reason why in productive environments it is advised not to use the standalone mode.

To run it as a Celery app, outside the Docker container the following commands are available:

As a stand-alone app
```
$> celery -A processor.tasks worker -B [-s celerybeat_scheduler_file_path] [-l celery_log_level]
```

As a scheduler app
```
$> celery -A processor.tasks beat [-s celerybeat_scheduler_file_path] [-l celery_log_level]
```

As a worker app
```
$> celery -A processor.tasks worker [-l celery_log_level] 
```

If you want to use it as a Docker Container, you should create a volume or use the volume that the `tix-time-condenser` 
is using to drop the report files, and the volume that is used to store the scheduler file

```
$> docker volume create --name ReportsVolume
$> docker volume create --name SchedulerVolume
```

After that, you should simply need to run

```
$> docker run -e PROCESSOR_TYPE=<PROCESSOR_TYPE> -v ReportsVolume:/tmp/reports -v SchedulerVolume:/tmp/celerybeat-schedule.d -dit tixmeasurements/tix-time-processor:latest-<type>
```

Where the environment variable `PROCESSOR_TYPE` might take the following values:

  * `STANDALONE` (**This is the default value**)
  * `BEAT`
  * `WORKER`

This works for both, the standalone and the scheduler / beat type.

For the worker type, you should omit the SchedulerVolume, like this

```
$> docker run -e PROCESSOR_TYPE=WORKER -v ReportsVolume:/tmp/reports -dit tixmeasurements/tix-time-processor:latest-<type>
```

There are other configuration variables available for the Docker Container besides `PROCESSOR_TYPE`

  * `CELERY_BEAT_SCHEDULE_DIR`: The directory where the Celery Beat schedule file will be stored. (**Default**: /tmp/celerybeat-schedule.d)
  * `CELERY_LOG_LEVEL`: The logging level for the Celery app. (**Default**: INFO)
