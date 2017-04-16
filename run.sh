#!/usr/bin/env bash

if [ "${PROCESSOR_TYPE}" == "STANDALONE" ] ;
then
    echo "Standalone processor started"
    celery -A processor worker -B -s ${CELERY_BEAT_SCHEDULE_DIR}/celerybeat-schedule -l ${CELERY_LOG_LEVEL}
elif [ "${PROCESSOR_TYPE}" == "BEAT" ] ;
then
    echo "Beat processor started"
    celery -A processor beat -s ${CELERY_BEAT_SCHEDULE_DIR}/celerybeat-schedule -l ${CELERY_LOG_LEVEL}
elif [ "${PROCESSOR_TYPE}" == "WORKER" ] ;
then
    echo "Worker processor started"
    celery -A processor worker -l ${CELERY_LOG_LEVEL}
else
    echo "Unknown processor type. Stopping"
fi
