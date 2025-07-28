from app.ingestion.saas_ingestor import SaasIngestor

def run():
    ingestor = SaasIngestor()
    ingestor.run()

if __name__ == '__main__':
    run()