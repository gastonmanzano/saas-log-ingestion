from app.ingestion.saas_ingestor import SaasIngestor
from app.ingestion.saas_consumer import SaasConsumer
import schedule
import multiprocessing

def run_ingestor(time):
    ingestor = SaasIngestor()

    schedule.every(time).seconds.do(ingestor.run)
   
    while True:
        schedule.run_pending()


def run_consumer():
    consumer = SaasConsumer()
    consumer.run()

if __name__ == '__main__':
    p1 = multiprocessing.Process(target=run_ingestor, args=(10,))
    p2 = multiprocessing.Process(target=run_consumer)
    p1.start()
    p2.start()