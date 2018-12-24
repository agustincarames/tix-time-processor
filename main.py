import traceback
import logging
import pika

from processor import reports
from processor import report_parser
from processor import api_communication
from processor import analysis
from processor import RABBITMQ_USER, RABBITMQ_PASS, RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_INCOMING_QUEUE

tasks_logger = logging.getLogger(__name__)

def process_measures(channel, method, properties, body):
    logger = tasks_logger.getChild('process_measures')
    current_reports = report_parser.Report.loads(body)
    ip, observations = reports.ReportHandler.collect_observations(current_reports)
    delivery_tag = method.delivery_tag
    if ip is None and observations is None:
        logger.error('Rejecting tag {} with no requeue, message {}', delivery_tag, body)
        channel.basic_reject(delivery_tag, requeue=False)
        return

    user_id = current_reports[0].user_id
    installation_id = current_reports[0].installation_id
    logger.info('Analyzing tag {} with {} observations for IP {}, user {}, installation {}'.format(delivery_tag,
                                                                                                   len(observations),
                                                                                                   ip,
                                                                                                   user_id,
                                                                                                   installation_id))
    analyzer = analysis.Analyzer(observations)
    results = analyzer.get_results()
    if api_communication.post_results(ip, results, user_id, installation_id):
        channel.basic_ack(delivery_tag)
    else:
        logger.error('Could not post tag {} results to API, rejecting with requeue', delivery_tag)
        channel.basic_reject(delivery_tag, requeue=True)
        

if __name__ == '__main__':
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host = RABBITMQ_HOST,
        port = RABBITMQ_PORT,
        credentials = credentials
    )
    connection = pika.BlockingConnection(parameters=parameters)
    channel = connection.channel()
    try:
        channel.queue_declare(queue=RABBITMQ_INCOMING_QUEUE, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(process_measures, queue=RABBITMQ_INCOMING_QUEUE)
        channel.start_consuming()
    except:
        tasks_logger.error('Exception caught {}'.format(traceback.format_exc()))
    finally:
        channel.cancel()
        connection.close()
