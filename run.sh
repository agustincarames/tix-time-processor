#!/usr/bin/env bash

if [ "${PROCESSOR_TYPE}" == "STANDALONE" ] ;
then
    echo "Standalone processor started"
    celery -A processor.tasks worker -B -s ${CELERY_BEAT_SCHEDULE_DIR}/celerybeat-scheduler -l ${CELERY_LOG_LEVEL}
elif [ "${PROCESSOR_TYPE}" == "BEAT" ] ;
then
    echo "Beat processor started"
    celery -A processor.tasks beat -s ${CELERY_BEAT_SCHEDULE_DIR}/celerybeat-scheduler -l ${CELERY_LOG_LEVEL}
elif [ "${PROCESSOR_TYPE}" == "WORKER" ] ;
then
    echo "Worker processor started"
    celery -A processor.tasks worker -l ${CELERY_LOG_LEVEL}
else
    echo "Unknown processor type. Stopping"
fi
