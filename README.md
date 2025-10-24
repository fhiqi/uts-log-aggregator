Proyek ini mengimplementasikan layanan Log Aggregator terdistribusi yang menjamin Exactly-Once Processing meskipun Publisher beroperasi dalam mode At-Least-Once.
Deduplikasi event dicapai melalui Idempotent Consumer yang menggunakan aiosqlite sebagai Durable Deduplication Store, menjamin persistensi state antar restart container (simulasi crash).

### Cara Build dan Run
Sistem dibangun menggunakan Docker Compose yang menjalankan dua layanan:
1. log-aggregator: Layanan utama (FastAPI Consumer) yang melakukan deduplikasi.
2. ublisher-simulator: Layanan klien yang mengirim 5.000 event (20% duplikat) ke Aggregator.

Pastikan Docker dan Docker Compose telah terinstall dan berikut langkah-langkah build dan run:
1. Clone Repository

``` 
git clone https://github.com/fhiqi/uts-log-aggregator.git 
cd uts-log-aggregator
```
2. Jalankan Sistem 
```
docker compose up --build
```

### Endpoint Verifikasi
* http://localhost:8081/stats (GET): Menampilkan metrik statistik (Received, Unique Processed, Dropped).
* http://localhost:8081/events (GET): Menampilkan daftar event yang berhasil diproses dan disimpan secara unik.
* http://localhost:8081/publish (POST): Endpoint utama penerima event (digunakan oleh publisher-simulator).

### Desain dan Idempotency
* Publisher Simulator beroperasi dalam mode At-Least-Once Delivery, di mana event dapat dikirim berulang kali (duplikasi data).
* Consumer Aggregator memiliki logic check_and_mark_idempotent yang bersifat atomic dan durable.

