FROM golang:1.23-bullseye AS golang

FROM python:3.11-slim
# Install git (needed for cloning repos)
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Copy Go toolchain from golang image
COPY --from=golang /usr/local/go /usr/local/go
ENV PATH="/usr/local/go/bin:${PATH}"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONIOENCODING="utf-8"
CMD ["python", "-u", "main.py"]
