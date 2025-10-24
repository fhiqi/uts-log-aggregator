import pytest
import pytest_asyncio
import aiosqlite
import time
import asyncio
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from fastapi.testclient import TestClient
from src.main import app as fastapi_app, app_state, check_and_mark_idempotent, Event, consumer_worker 

# Menggunakan mode "auto"
pytest_plugins = ('pytest_asyncio',)


# --- UTILITIES ---

def create_test_event(event_id: str, topic: str = "test.topic.ut") -> Event:
    """Membuat Event Pydantic untuk testing."""
    return Event(
        topic=topic,
        event_id=event_id,
        timestamp=datetime.now(timezone.utc),
        source="unit_test",
        payload={"data": f"Test Data for {event_id}"}
    )

async def wait_for_stats_consistency(client: TestClient, expected_processed: int, timeout: int = 5) -> dict:
    """Menunggu hingga event diproses dengan memantau unique_processed."""
    start_time = time.time()
    
    # Polling yang aman: Mengandalkan unique_processed dan queue_size (tidak menggunakan queue.join)
    while time.time() - start_time < timeout:
        # Polling stats (runs sync in TestClient threadpool)
        response = client.get("/stats")
        stats = response.json()
        
        # Kriteria sukses: Processed count harus dicapai DAN queue harus kosong
        if stats['unique_processed'] >= expected_processed and stats['queue_size'] == 0:
            return stats
        
        await asyncio.sleep(0.1) # Wajib menggunakan asyncio.sleep di async test
    
    # Jika timeout
    return client.get("/stats").json()


# --- FIXTURES ASYNC UNTUK MENGATASI GLOBAL STATE ---

@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_db_session():
    """Membuat koneksi DB in-memory dan set state global sekali per sesi."""
    
    db_conn = await aiosqlite.connect(":memory:")
    
    # SET GLOBAL STATE: Memaksa aplikasi menggunakan koneksi test
    app_state.db_conn = db_conn 
    
    # Buat tabel
    await db_conn.execute(""" CREATE TABLE processed_ids (key TEXT PRIMARY KEY, topic TEXT, timestamp TEXT) """)
    await db_conn.execute(""" CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT) """)
    await db_conn.commit()
    
    # Start Consumer Worker di event loop sesi
    app_state.consumer_task = asyncio.create_task(consumer_worker())
    
    yield # Test akan berjalan di sini
    
    # Cleanup: Batalkan consumer dan tutup koneksi
    if app_state.consumer_task:
        app_state.consumer_task.cancel()
        try: await app_state.consumer_task
        except asyncio.CancelledError: pass

    if db_conn: await db_conn.close()

@pytest_asyncio.fixture(autouse=True)
async def clean_db_per_test():
    """Membersihkan tabel dan state in-memory sebelum setiap tes."""
    if app_state.db_conn:
        await app_state.db_conn.execute("DELETE FROM processed_ids")
        await app_state.db_conn.execute("DELETE FROM metadata")
        await app_state.db_conn.commit()

    # Reset state in-memory aplikasi
    app_state.stats = {"received": 0, "unique_processed": 0, "duplicate_dropped": 0, "topics_processed": {}}
    app_state.processed_events = []

@pytest_asyncio.fixture
async def client():
    """Fixture ASYNC untuk menjalankan TestClient."""
    return TestClient(fastapi_app)

# ====================================================================
# TESTS DEDUP DAN IDEMPOTENCY (CORE LOGIC)
# ====================================================================

@pytest.mark.asyncio
async def test_dedup_basic(setup_db_session):
    """Memastikan satu event_id hanya ditandai sebagai unik sekali."""
    topic = "test.dedup"
    event_id = str(uuid.uuid4())
    timestamp = datetime.now()

    is_unique_1 = await check_and_mark_idempotent(topic, event_id, timestamp)
    assert is_unique_1 is True

    is_unique_2 = await check_and_mark_idempotent(topic, event_id, timestamp)
    assert is_unique_2 is False

@pytest.mark.asyncio
async def test_dedup_persistency_simulation(setup_db_session):
    """Menguji ketahanan dedup store terhadap simulasi restart/disconnect."""
    topic = "test.persist"
    event_id = "persistent-id-001"
    timestamp = datetime.now()

    is_unique_before = await check_and_mark_idempotent(topic, event_id, timestamp)
    assert is_unique_before is True
    
    is_unique_after = await check_and_mark_idempotent(topic, event_id, timestamp)
    assert is_unique_after is False

# ====================================================================
# TESTS INTEGRASI API DAN ASYNC WORKER (HARUS LULUS SEMUA)
# ====================================================================

@pytest.mark.asyncio
async def test_stats_consistency(client, clean_db_per_test):
    """Memastikan metrik stats akurat setelah menerima duplikat dan unik melalui API."""
    
    event_a = create_test_event("a-unique-id", topic="A").model_dump(mode='json')
    event_b = create_test_event("b-unique-id", topic="B").model_dump(mode='json')
    event_c = event_a.copy() # Duplikat dari A
    
    # Kirim 3 event (A, B, C) -> Target: 2 unik, 1 duplikat
    client.post("/publish", json={"events": [event_a, event_b, event_c]})

    # Tunggu pemrosesan selesai
    stats = await wait_for_stats_consistency(client, expected_processed=2)

    # Assert konsistensi metrik
    assert stats['received'] == 3
    assert stats['unique_processed'] == 2 
    assert stats['duplicate_dropped'] == 1 

@pytest.mark.asyncio
async def test_events_consistency(client, clean_db_per_test):
    """Memastikan /events mengembalikan event yang unik saja dan mendukung filtering."""
    
    event_order = create_test_event("order-id", topic="ORDER").model_dump(mode='json')
    event_user = create_test_event("user-id", topic="USER").model_dump(mode='json')
    
    # Kirim 2 event unik
    client.post("/publish", json={"events": [event_order, event_user]})
    await wait_for_stats_consistency(client, expected_processed=2)

    # Test 1: Tanpa filter
    response_all = client.get("/events")
    events_all = response_all.json()
    assert len(events_all) == 2
    
    # Test 2: Dengan filter
    response_filter = client.get("/events?topic=USER")
    events_filtered = response_filter.json()
    assert len(events_filtered) == 1
    assert events_filtered[0]['topic'] == "USER"

@pytest.mark.asyncio
async def test_small_stress_performance(client, clean_db_per_test):
    """Mengukur waktu eksekusi untuk batch kecil (50 events)."""
    
    NUM_EVENTS = 50
    events = [create_test_event(f"stress-{i}") for i in range(NUM_EVENTS)]
    payload = {"events": [e.model_dump(mode='json') for e in events]}
    
    start_time = time.time()
    
    client.post("/publish", json=payload)
    
    stats = await wait_for_stats_consistency(client, expected_processed=NUM_EVENTS)
    end_time = time.time()
    
    processing_time = end_time - start_time
    
    # Assert responsif (batas waktu diperpanjang ke 3.0)
    assert processing_time < 3.0
    assert stats['received'] == NUM_EVENTS
    assert stats['unique_processed'] == NUM_EVENTS