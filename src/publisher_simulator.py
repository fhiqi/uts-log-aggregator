import requests
import json
import time
import uuid
import random
from datetime import datetime, timezone
import logging
from typing import Dict

# Konfigurasi Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PublisherSimulator")

# --- Konfigurasi Simulasi ---
AGGREGATOR_URL = "http://log-aggregator:8080/publish"
TOTAL_EVENTS_TO_SEND = 5000
DUPLICATION_RATE = 0.20  # 20% duplikasi
BATCH_SIZE = 100
MAX_RETRIES = 5
RETRY_DELAY = 1  # seconds

# --- Utility Functions ---
def generate_unique_event(topic_prefix="test.topic", source="sim-pub") -> Dict:
    """Menghasilkan satu event unik dengan event_id baru."""
    return {
        "topic": f"{topic_prefix}.log",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        "source": source,
        "payload": {"data": f"Log message at {datetime.now().isoformat()}"}
    }

def simulate_send():
    """Simulasi pengiriman event batch dengan duplikasi."""
    
    unique_events_count = int(TOTAL_EVENTS_TO_SEND / (1 + DUPLICATION_RATE))
    num_duplicates = TOTAL_EVENTS_TO_SEND - unique_events_count
    
    logger.info(f"Target: Send {TOTAL_EVENTS_TO_SEND} events total.")
    logger.info(f"Target: {unique_events_count} unique events, {num_duplicates} duplicates.")

    unique_events = [generate_unique_event() for _ in range(unique_events_count)]
    all_events_to_send = unique_events[:]
    
    # 1. Menambahkan Duplikasi (At-Least-Once Simulation)
    duplicate_events = random.sample(unique_events, min(num_duplicates, unique_events_count))
    # Mengambil random event dari daftar unique untuk dijadikan duplikat
    
    all_events_to_send.extend(duplicate_events)
    random.shuffle(all_events_to_send)
    
    logger.info(f"Total events list created: {len(all_events_to_send)}")

    # 2. Pengiriman Batch
    start_time = time.monotonic()
    
    for i in range(0, len(all_events_to_send), BATCH_SIZE):
        batch = all_events_to_send[i:i + BATCH_SIZE]
        
        # Format batch untuk API
        payload = {"events": batch}
        
        retries = 0
        while retries < MAX_RETRIES:
            try:
                response = requests.post(AGGREGATOR_URL, json=payload, timeout=5)
                
                if response.status_code == 202:
                    logger.info(f"Batch {i//BATCH_SIZE + 1} of {len(all_events_to_send)//BATCH_SIZE}: ACCEPTED. Count: {len(batch)}")
                    break # Berhasil
                
                elif response.status_code == 503:
                    logger.warning(f"Batch {i//BATCH_SIZE + 1}: Queue full (503). Retrying in {RETRY_DELAY}s.")
                    retries += 1
                    time.sleep(RETRY_DELAY)
                
                else:
                    logger.error(f"Batch {i//BATCH_SIZE + 1}: Failed with status {response.status_code}. Response: {response.text}")
                    break # Kegagalan non-recoverable
                    
            except requests.exceptions.ConnectionError:
                logger.error(f"Batch {i//BATCH_SIZE + 1}: Connection Error. Aggregator not ready. Retrying in {RETRY_DELAY}s.")
                retries += 1
                time.sleep(RETRY_DELAY)
            except Exception as e:
                logger.error(f"Batch {i//BATCH_SIZE + 1}: Unexpected error: {e}")
                break
        
        if retries == MAX_RETRIES:
            logger.error(f"Batch {i//BATCH_SIZE + 1} failed after {MAX_RETRIES} retries. Stopping simulation.")
            break
        
        # Memberi jeda kecil antar batch
        time.sleep(0.01)
            
    end_time = time.monotonic()
    
    # 3. Hasil
    logger.info("-" * 40)
    logger.info("FINAL SIMULATION RESULTS:")
    logger.info(f"Total Events Sent: {len(all_events_to_send)}")
    logger.info(f"Expected Unique Events: {unique_events_count}")
    logger.info(f"Expected Duplicates Dropped: {num_duplicates}")
    logger.info(f"Time Taken: {round(end_time - start_time, 2)} seconds")
    logger.info("-" * 40)

def wait_for_aggregator():
    """Menunggu Aggregator siap sebelum memulai simulasi."""
    logger.info(f"Waiting for Aggregator to be ready at {AGGREGATOR_URL.replace('/publish', '/stats')}...")
    retries = 0
    while retries < MAX_RETRIES * 2: # Beri waktu lebih lama
        try:
            response = requests.get(AGGREGATOR_URL.replace('/publish', '/stats'), timeout=1)
            if response.status_code == 200:
                logger.info("Aggregator is ready!")
                return True
        except requests.exceptions.ConnectionError:
            pass
        except Exception as e:
            logger.warning(f"Error checking aggregator status: {e}. Retrying.")

        retries += 1
        time.sleep(RETRY_DELAY * 2) # Jeda lebih lama saat startup awal
        
    logger.error("Aggregator did not become ready. Exiting simulator.")
    return False

if __name__ == "__main__":
    if wait_for_aggregator():
        simulate_send()
