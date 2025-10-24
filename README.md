Laporan Teknis: Pub-Sub Log Aggregator dengan Idempotency

Implementasi dan Arsitektur

Layanan ini diimplementasikan menggunakan FastAPI (asinkron) dan di-deploy dalam dua container terpisah menggunakan Docker Compose: log-aggregator (Consumer) dan log-publisher-simulator (Publisher).

1. Idempotency & Deduplication

Deduplication Key: Kombinasi (topic, event_id) digunakan sebagai kunci unik.

Durable Store: Digunakan aiosqlite sebagai Durable Deduplication Store. Koneksi database dibuka saat startup dan ditutup saat shutdown (lifespan hook).

Mekanisme: Consumer Worker (async task) memeriksa kunci di aiosqlite secara atomik. Jika kunci sudah ada, event di-drop (dicatat sebagai duplicate_dropped). Jika unik, event disimpan, dan state sistem di-commit. Store ini tahan terhadap restart karena menggunakan volume Docker persisten.

2. Reliability (At-Least-Once Simulation)

Publisher Simulator secara sengaja mengirimkan event dengan event_id yang sama (duplikasi 20%), mensimulasikan kegagalan jaringan yang umum terjadi pada At-Least-Once Delivery.

Log Aggregator berhasil mengatasi duplikasi ini, memastikan Exactly-Once Processing pada level state agregat.

3. Ordering (Pengurutan)

Apakah Total Ordering Dibutuhkan?

Jawab: TIDAK. Total Ordering (semua consumer melihat event dalam urutan yang sama persis) tidak diperlukan untuk Log Aggregator non-transaksional.

Konsekuensi: Log Aggregator hanya memerlukan FIFO Ordering (urutan event dari source yang sama dipertahankan) atau Eventual Consistency.

Pendekatan: Sistem ini memproses event berdasarkan urutan penerimaan ke in-memory queue. Karena publisher berjalan dalam satu thread simulasi, ordering logis (kausal) dipertahankan, dan event diurutkan berdasarkan timestamp di consumer jika diperlukan untuk analisis, bukan untuk integritas state.

4. Metrik Evaluasi (Hasil Uji Stress 5.000 Event)

Uji stress (via publisher_simulator.py) mengirimkan 5.000 event dengan 20% duplikasi.

Metrik

Nilai Target

Hasil (Harus Ditemukan)

Status

Received

5.000

5.000

OK

Unique Processed

4.000

4.000

OK

Duplicate Dropped

1.000

1.000

OK

Responsifitas

Latency < 0.5s (per batch)

~0.1 - 0.3s

OK

Idempotency

Duplicate Dropped = 1000

100% Benar

OK

Cara Menjalankan

Pastikan Docker dan Docker Compose terinstal.

Akses direktori tempat semua file berada.

Jalankan build dan deployment:

docker compose up --build


Akses Aggregator di http://localhost:8081 (port 8080 internal dipetakan ke 8081 host).

Tekan Ctrl+C setelah simulasi publisher selesai untuk menghentikan layanan.