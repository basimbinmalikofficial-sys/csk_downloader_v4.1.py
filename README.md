# CSK Downloader v4.1

A powerful Python-based downloader utility for downloading media and files from various sources with advanced features.

## 📥 Overview

CSK Downloader is a versatile Python script designed to download files, videos, and media from multiple sources with support for resuming, retry logic, and concurrent downloads.

## ✨ Features

- **Multi-Source Support** - Download from HTTP, HTTPS, FTP
- **Resume Downloads** - Continue interrupted downloads
- **Concurrent Downloads** - Download multiple files simultaneously
- **Progress Tracking** - Real-time download progress
- **Retry Logic** - Automatic retry on failure
- **Proxy Support** - Use HTTP/HTTPS proxies
- **Bandwidth Limiting** - Control download speed
- **File Validation** - Verify file integrity with checksums
- **Custom Headers** - Support for custom HTTP headers
- **Batch Download** - Download multiple files from a list

## 📋 Requirements

- Python 3.6+
- requests library
- urllib3

## 🚀 Installation

```bash
git clone https://github.com/basimbinmalikofficial-sys/csk_downloader_v4.1.py.git
cd csk_downloader_v4.1.py
pip install -r requirements.txt
```

## 💻 Usage

### Basic Download
```bash
python3 csk_downloader_v4.1.py -u "https://example.com/file.zip"
```

### With Output Path
```bash
python3 csk_downloader_v4.1.py -u "URL" -o /path/to/save/
```

### Batch Download
```bash
python3 csk_downloader_v4.1.py -f urls.txt --batch
```

### With Proxy
```bash
python3 csk_downloader_v4.1.py -u "URL" -p "http://proxy:port"
```

### Limit Bandwidth
```bash
python3 csk_downloader_v4.1.py -u "URL" --speed-limit 500k
```

### Python API
```python
from csk_downloader import Downloader

downloader = Downloader()
downloader.download("https://example.com/file.zip")
```

## 🔧 Configuration

Create `config.ini`:

```ini
[download]
output_dir = ./downloads
concurrent_downloads = 3
timeout = 30
retries = 3

[bandwidth]
enable_limiting = false
speed_limit = unlimited

[proxy]
enabled = false
http = http://proxy:port
https = https://proxy:port
```

## 📊 Command Options

```
-u, --url URL              File URL to download
-f, --file FILE            File containing list of URLs
-o, --output DIR           Output directory
-c, --concurrent NUM       Number of concurrent downloads
-p, --proxy PROXY          Proxy address
--speed-limit SPEED        Bandwidth limit (e.g., 500k, 1m)
--timeout SECONDS          Connection timeout
--retries COUNT            Number of retries
--verify-ssl              Verify SSL certificates
--show-progress           Show download progress
--batch                    Batch download mode
--help                    Show help message
```

## 📝 Batch Download File Format

```
https://example.com/file1.zip
https://example.com/file2.tar.gz
https://example.com/file3.exe
```

## 🐛 Troubleshooting

### Connection Timeout
- Increase timeout value: `--timeout 60`
- Check internet connection
- Try with a proxy

### SSL Certificate Error
- Disable SSL verification: `--verify-ssl false`
- Update certificates: `pip install --upgrade certifi`

### Slow Downloads
- Use fewer concurrent downloads
- Check bandwidth limit settings
- Try different proxy

## 📝 License

MIT License

---

**Version**: 4.1  
**Last Updated**: 2024  
**Status**: Active
