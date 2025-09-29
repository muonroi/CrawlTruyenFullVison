# Sử dụng base image Python
FROM python:3.11-slim

# Cài đặt các dependencies cần thiết cho Playwright
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
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
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libcairo2 \
    libatspi2.0-0 \
    libfontconfig1 \
    libfreetype6 \
    fonts-unifont \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Sao chép các file requirements trước để tận dụng Docker cache
COPY requirements.txt requirements.txt
# Cài đặt các thư viện Python
RUN pip install --no-cache-dir -r requirements.txt

# Cài đặt Docker Compose V2 plugin
RUN COMPOSE_VERSION=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep 'tag_name' | cut -d'"' -f4) && \
    DOCKER_CONFIG=${DOCKER_CONFIG:-/usr/lib/docker} && \
    mkdir -p $DOCKER_CONFIG/cli-plugins && \
    curl -L "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64" -o $DOCKER_CONFIG/cli-plugins/docker-compose && \
    chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose

# Cài đặt trình duyệt cho Playwright
RUN python -m playwright install chromium

# Sao chép toàn bộ code của ứng dụng vào thư mục làm việc
COPY . .

# Mặc định, không chạy command gì. Command sẽ được override trong docker-compose.yml
CMD ["bash"]
