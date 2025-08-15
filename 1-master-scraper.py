from multiprocessing import Process
from scrapers.drogal.drogal_scraper import main as drogal
from scrapers.drogaven.drogaven_scraper import main as drogaven
from scrapers.drogaraia.drogaraia_scraper import main as drogaraia

if __name__ == "__main__":
    # Create a Process for each imported scraper function
    drogal_process = Process(target=drogal)
    drogaven_process = Process(target=drogaven)
    drogaraia_process = Process(target=drogaraia)

    print("Starting all scrapers...")
    
    # Start all processes
    drogal_process.start()
    drogaven_process.start()
    drogaraia_process.start()

    # Wait for all processes to complete
    drogal_process.join()
    drogaven_process.join()
    drogaraia_process.join()

    print("All scrapers have finished. JSON files are ready.")

