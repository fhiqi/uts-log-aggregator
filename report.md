Mengimplementasikan layanan Log Aggregator yang menjamin Exactly-Once Processing dalam lingkungan terdistribusi yang diasumsikan menggunakan At-Least-Once Delivery.

### Arsitektur Sistem
Sistem ini mengadopsi pola arsitektur Publish/Subscribe (Pub/Sub) di mana Publisher hanya mengirim event ke endpoint API tanpa perlu tahu bagaimana Consumer memprosesnya. Layanan Aggregator menggunakan asyncio dan FastAPI untuk menjalankan Consumer Worker secara concurrent. Worker ini menarik event dari asyncio.Queue (buffer in-memory), memungkinkan thread API tetap responsif.

### Exactly-Once Processing (Idempotency)
Exactly-Once Processing dicapai melalui implementasi Idempotent Consumer Logic, yang merupakan solusi praktis terhadap At-Least-Once Delivery yang secara inheren menghasilkan duplikasi.

### Consistency
Sistem mengadopsi model Eventual Consistency, consumer hanya menjamin bahwa state akhir (total count log) akan benar. Jika ada duplikasi atau retry, logic Idempotent akan memastikan event hanya berkontribusi sekali. Idempotency yang dipadukan dengan Durable Store membantu state sistem untuk converge ke nilai yang benar, meskipun ada kegagalan.

### Ordering
Untuk Log Aggregator, Total Ordering tidak diperlukan. Consumer Worker memproses queue secara FIFO, mempertahankan order pengiriman (delivery order) event dari API, yang cukup untuk tujuan log aggregation non-transaksional.


Link youtube: https://youtu.be/35eu5VBKRtc