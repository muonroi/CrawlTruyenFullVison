# Sử dụng base image Python
FROM python:3.11-slim

# Cài đặt các dependencies cần thiết cho Playwright
RUN apt-get update && apt-get install -y \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Sao chép các file requirements trước để tận dụng Docker cache
COPY requirements.txt requirements.txt
COPY requirements-dev.txt requirements-dev.txt

# Cài đặt các thư viện Python
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r requirements-dev.txt

# Cài đặt trình duyệt cho Playwright
RUN playwright install --with-deps chromium

# Sao chép toàn bộ code của ứng dụng vào thư mục làm việc
COPY . .

# Mặc định, không chạy command gì. Command sẽ được override trong docker-compose.yml
CMD ["bash"]
