# Tahap 1: Base Image
FROM python:3.11-slim

# Tahap 2: Setup Direktori Kerja
WORKDIR /app

# Tahap 3: Setup User Non-Root (Hanya Dibuat)
# Membuat user non-root (appuser)
# Instalasi akan dilakukan sebagai root, switching user di akhir
RUN adduser --disabled-password --gecos '' appuser

# Tahap 4: Dependency Caching & Instalasi (SEBAGAI ROOT - Solusi Final)
# Root user (default) digunakan untuk memastikan instalasi berhasil di path sistem global.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Tahap 5: Setup Direktori Data dan Izin
# Membuat direktori /app/data dan menetapkan ownership kepada appuser
RUN mkdir -p /app/data && chown -R appuser:appuser /app/data

# Menetapkan ownership /app kepada appuser, termasuk semua kode yang akan disalin.
RUN chown -R appuser:appuser /app

# Tahap 6: Menyalin Kode Aplikasi dan Beralih User
# Salin folder src/ dan tests/ secara utuh.
# Pastikan src/main.py, etc., berada di folder src/
COPY src/ ./src/ 
COPY tests/ ./tests/ 
# COPY publisher_simulator.py .

# Beralih ke user non-root (appuser) untuk alasan keamanan
USER appuser

# Tahap 7: Konfigurasi Startup
EXPOSE 8080

# PERINTAH AKHIR: Menggunakan 'python -m uvicorn' dengan resolusi modul yang benar ('src.main:app')
# Ini akan berhasil karena uvicorn sekarang berada di path sistem global.
CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080", "--lifespan", "on"]