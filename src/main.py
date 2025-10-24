import asyncio
import logging
import time
import json
from contextlib import asynccontextmanager
import os
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from typing import List, Dict, Optional
import aiosqlite
from datetime import datetime
from pathlib import Path

# --- Konfigurasi ---
STATS_KEY = "system_metrics"
DB_DIR = Path("./data")
DB_FILE = DB_DIR / "deduplication_store.sqlite"
QUEUE_MAX_SIZE = 10000
CONSUMER_DELAY = 0.05
LOG_LEVEL = logging.INFO

# Konfigurasi Logging
logging.basicConfig(level=LOG_LEVEL,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AggregatorService")

# --- Model Data Pydantic ---

class Event(BaseModel):
    topic: str = Field(..., description="Topic event, e.g., 'user.login'")
    event_id: str = Field(..., description="Unique, collision-resistant ID for the event.")
    timestamp: datetime = Field(..., description="Timestamp in ISO8601 format.")
    source: str = Field(..., description="Source service or application.")
    payload: Dict = Field(..., description="Event data payload.")

class EventBatch(BaseModel):
    events: List[Event]

# --- State Global ---
class AppState:
    def __init__(self):
        self.db_conn: Optional[aiosqlite.Connection] = None
        self.message_queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
        self.start_time: float = time.monotonic()
        self.stats: Dict[str, any] = {
            "received": 0,
            "unique_processed": 0,
            "duplicate_dropped": 0,
            "topics_processed": {},
        }
        self.processed_events: List[Dict] = []
        self.consumer_task: Optional[asyncio.Task] = None

app_state = AppState()

# --- Database Logic (aiosqlite) ---

async def load_stats_from_db():
    """Memuat metrik statistik dari DB saat startup."""
    try:
        cursor = await app_state.db_conn.execute("SELECT value FROM metadata WHERE key = ?", (STATS_KEY,))
        row = await cursor.fetchone()
        await cursor.close()

        if row:
            app_state.stats = json.loads(row[0])
            logger.info("STARTUP: Statistik operasional dimuat dari disk.")
    except Exception as e:
        logger.warning(f"STARTUP: Gagal memuat statistik. Menggunakan nilai default. {e}")
        
async def save_stats_to_db():
    """Menyimpan metrik statistik ke DB saat shutdown."""
    if app_state.db_conn:
        try:
            stats_json = json.dumps(app_state.stats)
            await app_state.db_conn.execute(
                "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", 
                (STATS_KEY, stats_json)
            )
            # Commit diperlukan untuk menyimpan metrik sebelum koneksi ditutup
            await app_state.db_conn.commit() 
            logger.info("SHUTDOWN: Statistik operasional disimpan ke disk.")
        except Exception as e:
            logger.error(f"SHUTDOWN FAILED: Gagal menyimpan statistik ke DB: {e}")

async def reset_db_state():
    """Mereset semua state dan metrik di database."""
    logger.warning("RESETTING: Menghapus semua event dan metrik operasional...")

    await app_state.db_conn.execute("DELETE FROM processed_ids")

    app_state.stats = {
        "received": 0,
        "unique_processed": 0,
        "duplicate_dropped": 0,
        "topics_processed": {},
    }
    app_state.processed_events = []
    app_state.start_time = time.monotonic() 
    
    await save_stats_to_db()
    logger.warning("RESETTING: Semua state telah direset ke nol.")
    
    
async def init_db_connection():
    """Menginisialisasi koneksi database aiosqlite yang bersifat asinkron."""
    try:
        os.makedirs(DB_DIR, exist_ok=True)
        # Menggunakan check_same_thread=False
        conn = await aiosqlite.connect(DB_FILE, check_same_thread=False)
        app_state.db_conn = conn
        
        # --- PERBAIKAN DURABILITAS KRUSIAL (PRAGMA) ---
        # 1. Mengaktifkan WAL (Write-Ahead Logging) untuk concurrency & durabilitas
        await conn.execute("PRAGMA journal_mode = WAL") 
        # 2. Memaksa sinkronisasi I/O ke disk sebelum commit selesai (untuk mengatasi Volume Docker)
        await conn.execute("PRAGMA synchronous = FULL")
        # --- END PERBAIKAN ---

        # Membuat tabel deduplication store
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_ids (
                key TEXT PRIMARY KEY,
                topic TEXT,
                timestamp TEXT
            )
        """)
        # Membuat tabel metadata
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await conn.commit()
        
        await load_stats_from_db()
        
        logger.info(f"STARTUP: Deduplication Store (aiosqlite) ready. Database file: {DB_FILE}")
    except Exception as e:
        logger.error(f"STARTUP FAILED: Gagal menginisialisasi aiosqlite di {DB_FILE}: {e}")
        raise

async def close_db_connection():
    """Menutup koneksi database saat shutdown."""
    if app_state.db_conn:
        await save_stats_to_db()
        await app_state.db_conn.close()
        logger.info("SHUTDOWN: Deduplication Store connection closed.")

async def check_and_mark_idempotent(topic: str, event_id: str, timestamp: datetime) -> bool:
    """Memeriksa idempotency. Menggunakan INSERT OR IGNORE."""
    
    dedup_key = f"{topic}:{event_id}"
    conn = app_state.db_conn
    
    try:
        # INSERT OR IGNORE adalah operasi atomic check-and-set
        cursor = await conn.execute(
            """INSERT OR IGNORE INTO processed_ids (key, topic, timestamp) VALUES (?, ?, ?)""",
            (dedup_key, topic, timestamp.isoformat())
        )
        
        is_unique = cursor.rowcount > 0
        await cursor.close()
        
        if is_unique:
            # Commit hanya jika INSERT berhasil (key baru)
            await conn.commit()
            return True
        else:
            # Jika INSERT diabaikan (key sudah ada, duplikat)
            app_state.stats['duplicate_dropped'] += 1
            logger.warning(f"CONSUMER: Dropped DUPLICATE event: {dedup_key}")
            return False
            
    except Exception as e:
        logger.error(f"CONSUMER ERROR: Gagal menyimpan kunci deduplikasi {dedup_key}: {e}")
        return False


# --- Consumer Logic ---

async def consumer_worker():
    """Worker asinkron yang memproses event dari message_queue."""
    logger.info("CONSUMER: Worker started, waiting for events...")
    
    while True:
        try:
            event = await app_state.message_queue.get()
            
            is_unique = await check_and_mark_idempotent(
                event.topic,
                event.event_id,
                event.timestamp
            )
            
            if is_unique:
                app_state.stats['unique_processed'] += 1
                app_state.stats['topics_processed'][event.topic] = \
                    app_state.stats['topics_processed'].get(event.topic, 0) + 1
                
                app_state.processed_events.append(event.model_dump())
                
                logger.info(f"CONSUMER: Processed unique event: {event.topic}:{event.event_id}")

            app_state.message_queue.task_done()
            
            await asyncio.sleep(CONSUMER_DELAY) 

        except asyncio.CancelledError:
            logger.info("CONSUMER: Worker task cancelled.")
            break
        except Exception as e:
            logger.error(f"CONSUMER ERROR: Exception during event processing: {e}")
            await asyncio.sleep(1)


# --- FastAPI Application Lifecycle Hooks ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Event Lifecycle Manager: Startup dan Shutdown."""
    
    await init_db_connection()
    app_state.consumer_task = asyncio.create_task(consumer_worker())
    
    yield
    
    if app_state.consumer_task:
        app_state.consumer_task.cancel()
        try:
            await app_state.consumer_task
        except asyncio.CancelledError:
            pass
    
    await close_db_connection()
    logger.info("SHUTDOWN: Aggregator service gracefully stopped.")


app = FastAPI(lifespan=lifespan)

# --- API Endpoints ---

@app.post("/publish", status_code=202)
async def publish_event(batch: EventBatch, request: Request):
    """Menerima batch event. Memasukkan event ke internal message queue."""
    if app_state.message_queue.full():
        raise HTTPException(status_code=503, detail="Queue full, try again later.")
    
    current_received = 0
    for event in batch.events:
        try:
            await app_state.message_queue.put(event)
            app_state.stats['received'] += 1
            current_received += 1
        except Exception as e:
            logger.error(f"API Error: Failed to put event to queue: {e}")
    
    return {"status": "accepted", "count": current_received}

@app.post("/reset-stats", status_code=200)
@app.get("/reset-stats", status_code=200)
async def reset_stats_endpoint():
    """Mereset semua statistik operasional dan state deduplikasi."""
    await reset_db_state()
    return {"status": "success", "message": "Statistik operasional dan kunci deduplikasi di database telah direset. Harap restart container untuk membersihkan cache in-memory."}


@app.get("/stats")
async def get_stats():
    """Mengembalikan metrik operasional dan performa sistem."""
    current_time = time.monotonic()
    uptime = current_time - app_state.start_time
    
    return {
        "received": app_state.stats['received'],
        "queue_size": app_state.message_queue.qsize(),
        "unique_processed": app_state.stats['unique_processed'],
        "duplicate_dropped": app_state.stats['duplicate_dropped'],
        "topics_processed": app_state.stats['topics_processed'],
        "uptime_seconds": round(uptime, 2)
    }

@app.get("/events")
async def get_processed_events(topic: Optional[str] = None) -> List[Dict]:
    """Mengembalikan daftar event unik yang telah diproses."""
    if topic:
        return [
            event for event in app_state.processed_events 
            if event.get('topic') == topic
        ]
    return app_state.processed_events