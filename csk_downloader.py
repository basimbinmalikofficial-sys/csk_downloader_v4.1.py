#!/usr/bin/env python3
"""
CSK Universal Media Downloader V6.1 – Self-Healing Edition
Auto-installs dependencies | Self-debugging | Zero-touch maintenance
"""

import sys
import subprocess
import importlib
import os
import platform

# ==================== AUTO-INSTALLER ====================
REQUIRED_PACKAGES = [
    'requests',
    'yt-dlp',
    'colorama',
    'beautifulsoup4',
    'tqdm',
    'validators',
    'python-magic',
    'psutil'
]

def auto_install_missing():
    """Install missing packages automatically"""
    print("\n🔍 Checking dependencies...")
    missing = []
    
    for package in REQUIRED_PACKAGES:
        try:
            importlib.import_module(package.replace('-', '_'))
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package}")
            missing.append(package)
    
    if missing:
        print(f"\n📦 Installing missing packages: {', '.join(missing)}")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
            print("✅ All packages installed successfully!")
        except Exception as e:
            print(f"⚠️ Auto-install failed: {e}")
            print("💡 Please install manually: pip install " + " ".join(missing))
            return False
    else:
        print("✅ All dependencies satisfied!")
    
    return True

# Run auto-installer first
if not auto_install_missing():
    print("⚠️ Continuing with limited functionality...")

# ==================== IMPORTS (now should work) ====================
try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    requests = None

try:
    import yt_dlp
except ImportError:
    yt_dlp = None

try:
    from colorama import init, Fore, Back, Style
    init(autoreset=True)
except ImportError:
    class Fore: BLACK=RED=GREEN=YELLOW=BLUE=MAGENTA=CYAN=WHITE=''; RESET=''
    class Back: BLACK=RED=GREEN=YELLOW=BLUE=MAGENTA=CYAN=WHITE=''; RESET=''
    class Style: BRIGHT=DIM=NORMAL=RESET_ALL=''

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

try:
    from tqdm import tqdm
except ImportError:
    class tqdm:
        def __init__(*a,**kw): pass
        def update(self,n): pass
        def close(self): pass
        def __enter__(self): return self
        def __exit__(self,*a): pass

try:
    import validators
except ImportError:
    validators = None

try:
    import magic
except ImportError:
    magic = None

try:
    import psutil
except ImportError:
    psutil = None

import json
import time
import argparse
import logging
import hashlib
import pickle
import signal
import threading
import queue
import re
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict, Any, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from enum import Enum
from urllib.parse import urlparse, parse_qs, unquote
from functools import wraps
from collections import defaultdict

# ==================== VERSION ====================
VERSION = "6.1.0"
BUILD_DATE = "2024-03-04"
AUTHOR = "CSK Technologies (Self-Healing Edition)"

# ==================== BANNER ====================
BANNER = f"""
{Fore.CYAN}{Style.BRIGHT}
╔═══════════════════════════════════════════════════════════════════════════════╗
║                                                                               ║
║   ██████╗███████╗██╗  ██╗    ██╗   ██╗███╗   ██╗██╗██╗   ██╗███████╗██████╗  ║
║  ██╔════╝██╔════╝██║ ██╔╝    ██║   ██║████╗  ██║██║██║   ██║██╔════╝██╔══██╗ ║
║  ██║     ███████╗█████╔╝     ██║   ██║██╔██╗ ██║██║██║   ██║█████╗  ██████╔╝ ║
║  ██║     ╚════██║██╔═██╗     ██║   ██║██║╚██╗██║██║╚██╗ ██╔╝██╔══╝  ██╔══██╗ ║
║  ╚██████╗███████║██║  ██╗    ╚██████╔╝██║ ╚████║██║ ╚████╔╝ ███████╗██║  ██║ ║
║   ╚═════╝╚══════╝╚═╝  ╚═╝     ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝ ║
║                                                                               ║
║               UNIVERSAL MEDIA DOWNLOADER v{VERSION} - SELF-HEALING              ║
║                 Auto-installs • Self-debugs • Zero-touch                     ║
║                                                                               ║
╚═══════════════════════════════════════════════════════════════════════════════╝
{Style.RESET_ALL}
"""

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ==================== ENUMS & DATACLASSES ====================
class MediaType(Enum):
    IMAGE = "image"; VIDEO = "video"; AUDIO = "audio"
    DOCUMENT = "document"; ARCHIVE = "archive"; UNKNOWN = "unknown"

class Platform(Enum):
    YOUTUBE = "youtube"; TIKTOK = "tiktok"; INSTAGRAM = "instagram"
    PINTEREST = "pinterest"; TWITTER = "twitter"; FACEBOOK = "facebook"
    DIRECT = "direct"; UNKNOWN = "unknown"

class DownloadStatus(Enum):
    PENDING = "pending"; DOWNLOADING = "downloading"; COMPLETED = "completed"
    FAILED = "failed"; CANCELLED = "cancelled"; RESUMED = "resumed"

@dataclass
class DownloadResult:
    success: bool; filepath: Optional[str]; media_type: MediaType
    platform: Platform; size: int; status: DownloadStatus = DownloadStatus.COMPLETED
    error: Optional[str] = None; url: Optional[str] = None; filename: Optional[str] = None
    duration: float = 0.0; speed: float = 0.0; timestamp: datetime = field(default_factory=datetime.now)
    attempts: int = 1; checksum: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {**asdict(self), 'timestamp': self.timestamp.isoformat(), 'status': self.status.value}

@dataclass
class DownloadJob:
    id: str; url: str; platform: Platform; media_type: MediaType
    priority: int = 0; retries: int = 0; max_retries: int = 3
    status: DownloadStatus = DownloadStatus.PENDING
    added_time: datetime = field(default_factory=datetime.now)
    start_time: Optional[datetime] = None; end_time: Optional[datetime] = None
    result: Optional[DownloadResult] = None

# ==================== DECORATORS ====================
def error_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            logger.warning("Interrupted by user"); raise
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
            # Auto-fix attempt
            if "ModuleNotFoundError" in str(e) or "ImportError" in str(e):
                logger.info("🛠️ Attempting auto-fix: reinstalling missing modules...")
                auto_install_missing()
            return None
    return wrapper

def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        logger.debug(f"{func.__name__} took {time.time()-start:.2f}s")
        return result
    return wrapper

# ==================== CONFIG MANAGER ====================
class ConfigManager:
    PROFILES = {
        'default': {
            'max_retries': 3, 'timeout': 30, 'concurrent_downloads': 2,
            'chunk_size': 8192, 'enable_cache': True, 'cache_expiry': 86400,
            'quality': 'best', 'download_dir': 'downloads', 'verify_ssl': True,
            'rate_limit': 0, 'max_file_size': 0, 'allowed_extensions': [],
            'blocked_extensions': ['.exe', '.bat', '.sh', '.bin'],
            'auto_organize': True, 'create_subdirs': True
        },
        'high_speed': {'concurrent_downloads': 5, 'chunk_size': 16384, 'timeout': 15},
        'low_bandwidth': {'concurrent_downloads': 1, 'chunk_size': 4096, 'timeout': 60, 'quality': 'worst'},
        'safe_mode': {'verify_ssl': True, 'max_file_size': 104857600, 
                      'allowed_extensions': ['.jpg', '.jpeg', '.png', '.gif', '.mp4', '.mp3']}
    }
    
    def __init__(self, config_dir: Path):
        self.config_dir = config_dir; self.config_file = config_dir / "settings.json"
        self.profile_file = config_dir / "profiles.json"
        self.config = self._load_defaults(); self.profiles = self.PROFILES.copy()
        self._load_config(); self._load_profiles()
        
    def _load_defaults(self) -> Dict: return self.PROFILES['default'].copy()
    
    def _load_config(self):
        try:
            self.config_dir.mkdir(parents=True, exist_ok=True)
            if self.config_file.exists():
                with open(self.config_file, 'r') as f:
                    self.config.update(json.load(f))
        except Exception as e: logger.warning(f"Could not load config: {e}")
    
    def _load_profiles(self):
        try:
            if self.profile_file.exists():
                with open(self.profile_file, 'r') as f:
                    custom = json.load(f)
                    self.profiles.update(custom)
        except Exception as e: logger.warning(f"Could not load profiles: {e}")
    
    def save_config(self):
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=4)
        except Exception as e: logger.warning(f"Could not save config: {e}")
    
    def get(self, key: str, default=None): return self.config.get(key, default)
    def set(self, key: str, value): self.config[key] = value; self.save_config()
    
    def apply_profile(self, profile_name: str) -> bool:
        if profile_name in self.profiles:
            self.config.update(self.profiles[profile_name]); self.save_config()
            logger.info(f"Applied profile: {profile_name}"); return True
        logger.warning(f"Profile not found: {profile_name}"); return False

# ==================== CACHE MANAGER ====================
class CacheManager:
    def __init__(self, cache_file: Path, expiry: int = 86400, max_size: int = 100):
        self.cache_file = cache_file; self.expiry = expiry; self.max_size = max_size
        self.cache = self._load_cache(); self.access_times = {}
    
    def _load_cache(self) -> Dict:
        try:
            if self.cache_file.exists():
                if time.time() - self.cache_file.stat().st_mtime > self.expiry:
                    return {}
                with open(self.cache_file, 'rb') as f:
                    cache = pickle.load(f)
                    current = time.time()
                    return {k: v for k, v in cache.items() 
                           if isinstance(v, dict) and current - v.get('timestamp', 0) < self.expiry}
        except Exception as e: logger.warning(f"Could not load cache: {e}")
        return {}
    
    def save_cache(self):
        try:
            if len(self.cache) > self.max_size:
                sorted_items = sorted(self.cache.items(), key=lambda x: self.access_times.get(x[0], 0))
                self.cache = dict(sorted_items[-self.max_size:])
            with open(self.cache_file, 'wb') as f:
                pickle.dump(self.cache, f)
        except Exception as e: logger.warning(f"Could not save cache: {e}")
    
    def get(self, key: str) -> Optional[Any]:
        try:
            if key in self.cache and time.time() - self.cache[key]['timestamp'] < self.expiry:
                self.access_times[key] = time.time()
                return self.cache[key]['data']
        except Exception: pass
        return None
    
    def set(self, key: str, data: Any):
        try:
            self.cache[key] = {'timestamp': time.time(), 'data': data}
            self.access_times[key] = time.time()
            self.save_cache()
        except Exception as e: logger.warning(f"Cache set error: {e}")
    
    def clear(self): self.cache = {}; self.access_times = {}; self.save_cache()

# ==================== DOWNLOAD TRACKER ====================
class DownloadTracker:
    def __init__(self):
        self.stats = {
            'total': 0, 'success': 0, 'failed': 0, 'cancelled': 0,
            'total_size': 0, 'total_time': 0, 'start_time': time.time(),
            'by_platform': defaultdict(lambda: {'total': 0, 'success': 0, 'size': 0}),
            'by_type': defaultdict(lambda: {'total': 0, 'success': 0, 'size': 0})
        }
        self.failed_urls: List[Tuple[str, str, str]] = []
        self.history: List[DownloadResult] = []
        self.lock = threading.Lock()
    
    def update(self, result: DownloadResult):
        with self.lock:
            self.stats['total'] += 1
            if result.success:
                self.stats['success'] += 1
                self.stats['total_size'] += result.size
                self.stats['total_time'] += result.duration
                
                p_key = result.platform.value
                self.stats['by_platform'][p_key]['total'] += 1
                self.stats['by_platform'][p_key]['success'] += 1
                self.stats['by_platform'][p_key]['size'] += result.size
                
                t_key = result.media_type.value
                self.stats['by_type'][t_key]['total'] += 1
                self.stats['by_type'][t_key]['success'] += 1
                self.stats['by_type'][t_key]['size'] += result.size
            else:
                self.stats['failed'] += 1
                if result.url and result.error:
                    self.failed_urls.append((result.url, result.error, result.platform.value))
            self.history.append(result)
    
    def get_summary(self) -> Dict:
        with self.lock:
            elapsed = time.time() - self.stats['start_time']
            avg_speed = self.stats['total_size'] / max(self.stats['total_time'], 0.001)
            success_rate = (self.stats['success'] / max(self.stats['total'], 1)) * 100
            return {**self.stats, 'elapsed': elapsed, 'avg_speed': avg_speed,
                    'success_rate': success_rate, 'failed_urls_count': len(self.failed_urls),
                    'history_count': len(self.history), 'by_platform': dict(self.stats['by_platform']),
                    'by_type': dict(self.stats['by_type'])}
    
    def get_formatted_summary(self) -> str:
        s = self.get_summary()
        lines = [
            f"\n{Fore.CYAN}{Style.BRIGHT}═══════════════════════════════════════════════════════════",
            f"                    DOWNLOAD SUMMARY REPORT",
            f"═══════════════════════════════════════════════════════════════{Style.RESET_ALL}",
            f"", f"{Fore.WHITE}Total Downloads:    {s['total']}",
            f"{Fore.GREEN}Successful:         {s['success']}",
            f"{Fore.RED}Failed:             {s['failed']}",
            f"{Fore.YELLOW}Cancelled:          {s['cancelled']}",
            f"", f"{Fore.CYAN}Performance:",
            f"  Total Size:        {self._format_size(s['total_size'])}",
            f"  Average Speed:     {self._format_size(s['avg_speed'])}/s",
            f"  Total Time:        {self._format_time(s['total_time'])}",
            f"  Success Rate:      {s['success_rate']:.1f}%",
            f"", f"{Fore.CYAN}By Platform:"
        ]
        for platform, stats in s['by_platform'].items():
            if stats['total'] > 0:
                lines.append(f"  {platform.title()}: {stats['success']}/{stats['total']} ({self._format_size(stats['size'])})")
        lines.extend(["", f"{Fore.CYAN}By Media Type:"])
        for mtype, stats in s['by_type'].items():
            if stats['total'] > 0:
                lines.append(f"  {mtype.title()}: {stats['success']}/{stats['total']} ({self._format_size(stats['size'])})")
        lines.append(f"{Fore.CYAN}═══════════════════════════════════════════════════════════{Style.RESET_ALL}")
        return "\n".join(lines)
    
    def _format_size(self, size: float) -> str:
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024: return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"
    
    def _format_time(self, seconds: float) -> str:
        if seconds < 60: return f"{seconds:.1f}s"
        elif seconds < 3600: return f"{seconds/60:.1f}m"
        else: return f"{seconds/3600:.1f}h"

# ==================== RATE LIMITER ====================
class RateLimiter:
    def __init__(self, max_rate: float = 0):
        self.max_rate = max_rate
        self.last_time = time.time()
        self.bytes_downloaded = 0
        self.lock = threading.Lock()
    
    def limit(self, bytes_downloaded: int):
        if self.max_rate <= 0: return
        with self.lock:
            self.bytes_downloaded += bytes_downloaded
            current_time = time.time()
            elapsed = current_time - self.last_time
            if elapsed > 0:
                current_rate = self.bytes_downloaded / elapsed
                if current_rate > self.max_rate:
                    target_time = self.bytes_downloaded / self.max_rate
                    sleep_time = target_time - elapsed
                    if sleep_time > 0: time.sleep(sleep_time)
            if elapsed >= 1.0:
                self.last_time = current_time
                self.bytes_downloaded = 0

# ==================== ADVANCED DOWNLOADER ====================
class AdvancedDownloader:
    def __init__(self, base_dir: str = "downloads"):
        self.base_dir = Path(base_dir)
        self.video_dir = self.base_dir / "videos"
        self.image_dir = self.base_dir / "images"
        self.audio_dir = self.base_dir / "audio"
        self.document_dir = self.base_dir / "documents"
        self.archive_dir = self.base_dir / "archives"
        self.temp_dir = self.base_dir / ".temp"
        self.config_dir = Path.home() / ".config" / "csk-downloader"
        self.log_dir = self.config_dir / "logs"
        
        for d in [self.video_dir, self.image_dir, self.audio_dir, self.document_dir,
                  self.archive_dir, self.temp_dir, self.config_dir, self.log_dir]:
            try: d.mkdir(parents=True, exist_ok=True)
            except: pass
        
        self.config = ConfigManager(self.config_dir)
        self.cache = CacheManager(self.config_dir / "cache.pkl", self.config.get('cache_expiry'))
        self.tracker = DownloadTracker()
        self._setup_file_logging()
        
        self.download_queue = queue.Queue()
        self.active_jobs: Dict[str, DownloadJob] = {}
        self.completed_jobs: List[DownloadJob] = []
        self.job_lock = threading.Lock()
        self.rate_limiter = RateLimiter(self.config.get('rate_limit'))
        
        self.session = self._create_session() if requests else None
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"AdvancedDownloader initialized in {base_dir}")
    
    def _setup_file_logging(self):
        log_file = self.log_dir / f"downloader_{datetime.now():%Y%m%d}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'))
        logger.addHandler(file_handler)
    
    def _create_session(self):
        try:
            session = requests.Session()
            retry = Retry(total=self.config.get('max_retries'), backoff_factor=1,
                          status_forcelist=[429,500,502,503,504])
            adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
            session.mount("http://", adapter); session.mount("https://", adapter)
            session.headers.update({'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'})
            session.verify = self.config.get('verify_ssl', True)
            if self.config.get('proxy'): session.proxies = {'http': self.config.get('proxy'), 'https': self.config.get('proxy')}
            return session
        except Exception as e: logger.error(f"Failed to create session: {e}"); return None
    
    def _signal_handler(self, sig, frame):
        logger.warning("\n" + "="*60)
        logger.warning("INTERRUPT: Download interrupted by user")
        logger.warning("="*60)
        with self.job_lock:
            for job in self.active_jobs.values():
                if job.status == DownloadStatus.DOWNLOADING:
                    job.status = DownloadStatus.CANCELLED
        print(self.tracker.get_formatted_summary())
        self._save_state()
        sys.exit(0)
    
    def _save_state(self):
        try:
            state = {'active_jobs': self.active_jobs, 'completed_jobs': self.completed_jobs,
                     'tracker': self.tracker, 'timestamp': time.time()}
            with open(self.temp_dir / "session_state.pkl", 'wb') as f:
                pickle.dump(state, f)
        except Exception as e: logger.warning(f"Could not save state: {e}")
    
    def _load_state(self) -> bool:
        try:
            state_file = self.temp_dir / "session_state.pkl"
            if state_file.exists():
                with open(state_file, 'rb') as f:
                    state = pickle.load(f)
                if time.time() - state['timestamp'] < 86400:
                    self.active_jobs = state['active_jobs']
                    self.completed_jobs = state['completed_jobs']
                    self.tracker = state['tracker']
                    logger.info(f"Loaded session with {len(self.active_jobs)} active jobs")
                    return True
        except Exception as e: logger.warning(f"Could not load state: {e}")
        return False
    
    def validate_url(self, url: str) -> Tuple[bool, str, Optional[Dict]]:
        if not url or not isinstance(url, str): return False, "Empty URL", None
        url = url.strip()
        if not url.startswith(('http://', 'https://')): return False, "URL must start with http:// or https://", None
        if validators:
            try:
                if validators.url(url):
                    parsed = urlparse(url)
                    return True, url, {'scheme': parsed.scheme, 'netloc': parsed.netloc, 'path': parsed.path}
            except: pass
        pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        if re.match(pattern, url):
            parsed = urlparse(url)
            return True, url, {'scheme': parsed.scheme, 'netloc': parsed.netloc, 'path': parsed.path}
        return False, "Invalid URL format", None
    
    def detect_platform(self, url: str) -> Platform:
        url_lower = url.lower()
        patterns = {
            Platform.YOUTUBE: ['youtube.com', 'youtu.be'],
            Platform.TIKTOK: ['tiktok.com', 'vm.tiktok.com'],
            Platform.INSTAGRAM: ['instagram.com', 'instagr.am'],
            Platform.PINTEREST: ['pinterest.com', 'pin.it'],
            Platform.TWITTER: ['twitter.com', 'x.com'],
            Platform.FACEBOOK: ['facebook.com', 'fb.com']
        }
        for platform, pats in patterns.items():
            if any(p in url_lower for p in pats): return platform
        if re.search(r'\.(jpg|jpeg|png|gif|mp4|mp3|pdf|zip)(\?|$)', url_lower): return Platform.DIRECT
        return Platform.UNKNOWN
    
    def detect_media_type(self, url: str, platform: Platform, content_type: Optional[str] = None) -> MediaType:
        if content_type:
            if 'image' in content_type: return MediaType.IMAGE
            if 'video' in content_type: return MediaType.VIDEO
            if 'audio' in content_type: return MediaType.AUDIO
            if 'pdf' in content_type or 'document' in content_type: return MediaType.DOCUMENT
            if 'zip' in content_type or 'archive' in content_type: return MediaType.ARCHIVE
        ext_map = {
            MediaType.IMAGE: r'\.(jpg|jpeg|png|gif|webp|bmp|svg)',
            MediaType.VIDEO: r'\.(mp4|webm|mkv|avi|mov|wmv|flv)',
            MediaType.AUDIO: r'\.(mp3|wav|aac|m4a|ogg|flac)',
            MediaType.DOCUMENT: r'\.(pdf|doc|docx|txt|rtf|odt)',
            MediaType.ARCHIVE: r'\.(zip|rar|7z|tar|gz)'
        }
        for mt, pat in ext_map.items():
            if re.search(pat + r'(\?|$)', url.lower()): return mt
        type_map = {Platform.YOUTUBE: MediaType.VIDEO, Platform.TIKTOK: MediaType.VIDEO,
                    Platform.INSTAGRAM: MediaType.VIDEO, Platform.PINTEREST: MediaType.IMAGE}
        return type_map.get(platform, MediaType.UNKNOWN)
    
    def generate_filename(self, url: str, platform: Platform, media_type: MediaType, info: Optional[Dict] = None) -> str:
        timestamp = int(time.time()); url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        parsed = urlparse(url); path = unquote(parsed.path); basename = os.path.basename(path)
        if basename and '.' in basename:
            name, ext = os.path.splitext(basename)
            if 3 <= len(name) <= 100: return f"{platform.value}_{name}_{url_hash}{ext}"
        if info and 'title' in info:
            title = re.sub(r'[^\w\-_\. ]', '', info['title']).replace(' ', '_')[:50]
            ext = info.get('ext', 'mp4')
            return f"{platform.value}_{title}_{url_hash}.{ext}"
        ext = self._get_extension(url, self._get_default_ext(media_type))
        return f"{platform.value}_{timestamp}_{url_hash}{ext}"
    
    def _get_default_ext(self, media_type: MediaType) -> str:
        return {MediaType.IMAGE: '.jpg', MediaType.VIDEO: '.mp4', MediaType.AUDIO: '.mp3',
                MediaType.DOCUMENT: '.pdf', MediaType.ARCHIVE: '.zip', MediaType.UNKNOWN: '.bin'}.get(media_type, '.bin')
    
    def _get_extension(self, url: str, default: str = '.bin') -> str:
        try:
            path = unquote(urlparse(url).path); ext = os.path.splitext(path)[1].lower()
            if ext and 2 <= len(ext) <= 5: return ext
        except: pass
        return default
    
    def get_target_directory(self, media_type: MediaType) -> Path:
        return {MediaType.IMAGE: self.image_dir, MediaType.VIDEO: self.video_dir,
                MediaType.AUDIO: self.audio_dir, MediaType.DOCUMENT: self.document_dir,
                MediaType.ARCHIVE: self.archive_dir}.get(media_type, self.base_dir)
    
    def check_disk_space(self, required_size: int) -> bool:
        if psutil:
            try:
                usage = psutil.disk_usage(str(self.base_dir))
                if usage.free < required_size * 1.1:
                    logger.warning(f"Insufficient space: need {self._format_size(required_size)}, have {self._format_size(usage.free)}")
                    return False
            except: pass
        return True
    
    def verify_file_integrity(self, filepath: Path) -> bool:
        try:
            if not filepath.exists() or filepath.stat().st_size == 0: return False
            if magic:
                try:
                    mime = magic.from_file(str(filepath), mime=True)
                    if mime.startswith(('image/', 'video/', 'audio/')): return True
                except: pass
            return True
        except: return False
    
    def _format_size(self, size: int) -> str:
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024: return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"
    
    @error_handler
    def download_media(self, url: str, priority: int = 0) -> DownloadResult:
        start_time = time.time()
        job_id = hashlib.md5(f"{url}_{time.time()}".encode()).hexdigest()[:12]
        
        valid, url_or_err, meta = self.validate_url(url)
        if not valid:
            return DownloadResult(False, None, MediaType.UNKNOWN, Platform.UNKNOWN, 0, error=url_or_err, url=url, duration=0)
        
        url = url_or_err
        platform = self.detect_platform(url)
        
        content_type = None
        if self.session and platform == Platform.DIRECT:
            try:
                resp = self.session.head(url, timeout=5, allow_redirects=True)
                content_type = resp.headers.get('content-type', '').lower()
            except: pass
        
        media_type = self.detect_media_type(url, platform, content_type)
        
        job = DownloadJob(job_id, url, platform, media_type, priority)
        with self.job_lock: self.active_jobs[job_id] = job
        
        logger.info(f"Job {job_id}: Processing {platform.value} - {media_type.value}")
        
        if self.config.get('enable_cache'):
            cache_key = f"download_{hashlib.md5(url.encode()).hexdigest()}"
            cached = self.cache.get(cache_key)
            if cached and Path(cached).exists() and self.verify_file_integrity(Path(cached)):
                try:
                    size = Path(cached).stat().st_size
                    result = DownloadResult(True, cached, media_type, platform, size, url=url, duration=time.time()-start_time)
                    with self.job_lock:
                        job.status = DownloadStatus.COMPLETED; job.end_time = datetime.now(); job.result = result
                        self.completed_jobs.append(job); del self.active_jobs[job_id]
                    self.tracker.update(result)
                    logger.info(f"Job {job_id}: Loaded from cache")
                    return result
                except: pass
        
        est_size = 100 * 1024 * 1024
        if not self.check_disk_space(est_size):
            return DownloadResult(False, None, media_type, platform, 0, error="Insufficient disk space", url=url, duration=time.time()-start_time)
        
        try:
            with self.job_lock: job.status = DownloadStatus.DOWNLOADING; job.start_time = datetime.now()
            
            if media_type == MediaType.IMAGE: result = self._download_image(url, platform, job_id)
            elif media_type == MediaType.VIDEO: result = self._download_video(url, platform, job_id)
            elif media_type == MediaType.AUDIO: result = self._download_audio(url, platform, job_id)
            elif media_type in [MediaType.DOCUMENT, MediaType.ARCHIVE]: result = self._download_file(url, platform, media_type, job_id)
            else: result = self._download_generic(url, platform, job_id)
            
            result.duration = time.time() - start_time
            
            if result.success and self.config.get('enable_cache'):
                self.cache.set(f"download_{hashlib.md5(url.encode()).hexdigest()}", result.filepath)
            
            with self.job_lock:
                job.status = DownloadStatus.COMPLETED if result.success else DownloadStatus.FAILED
                job.end_time = datetime.now(); job.result = result
                self.completed_jobs.append(job); del self.active_jobs[job_id]
        
        except Exception as e:
            logger.error(f"Job {job_id}: Download failed: {e}", exc_info=True)
            result = DownloadResult(False, None, media_type, platform, 0, error=str(e), url=url, duration=time.time()-start_time)
            with self.job_lock:
                job.status = DownloadStatus.FAILED; job.end_time = datetime.now(); job.result = result
                self.completed_jobs.append(job); del self.active_jobs[job_id]
        
        self.tracker.update(result)
        if result.success:
            logger.info(f"Job {job_id}: Downloaded {self._format_size(result.size)} to {Path(result.filepath).name}")
        else:
            logger.error(f"Job {job_id}: Failed - {result.error}")
        return result
    
    def _download_image(self, url: str, platform: Platform, job_id: str) -> DownloadResult:
        if not self.session:
            return DownloadResult(False, None, MediaType.IMAGE, platform, 0, error="Requests not available", url=url)
        try:
            filename = self.generate_filename(url, platform, MediaType.IMAGE)
            filepath = self.image_dir / filename
            
            resp = self.session.get(url, timeout=self.config.get('timeout'), stream=True)
            if resp.status_code != 200:
                return DownloadResult(False, None, MediaType.IMAGE, platform, 0, error=f"HTTP {resp.status_code}", url=url)
            
            total = int(resp.headers.get('content-length', 0))
            max_size = self.config.get('max_file_size')
            if max_size > 0 and total > max_size:
                return DownloadResult(False, None, MediaType.IMAGE, platform, 0, error=f"File too large: {self._format_size(total)}", url=url)
            
            downloaded = 0; start = time.time()
            with open(filepath, 'wb') as f:
                for chunk in resp.iter_content(self.config.get('chunk_size')):
                    if chunk:
                        f.write(chunk); downloaded += len(chunk)
                        self.rate_limiter.limit(len(chunk))
                        if TQDM_AVAILABLE and total > 0:
                            elapsed = time.time() - start
                            speed = downloaded / elapsed if elapsed > 0 else 0
                            print(f"\r  Job {job_id}: {(downloaded/total)*100:.1f}% [{self._format_size(downloaded)}/{self._format_size(total)}] @ {self._format_size(speed)}/s", end='')
            if TQDM_AVAILABLE: print()
            
            if not self.verify_file_integrity(filepath):
                filepath.unlink()
                return DownloadResult(False, None, MediaType.IMAGE, platform, 0, error="Integrity check failed", url=url)
            
            elapsed = time.time() - start
            speed = downloaded / elapsed if elapsed > 0 else 0
            return DownloadResult(True, str(filepath), MediaType.IMAGE, platform, downloaded, filename=filename, url=url, speed=speed, attempts=1)
        except Exception as e: return DownloadResult(False, None, MediaType.IMAGE, platform, 0, error=str(e), url=url)
    
    def _download_video(self, url: str, platform: Platform, job_id: str) -> DownloadResult:
        if not yt_dlp:
            return DownloadResult(False, None, MediaType.VIDEO, platform, 0, error="yt-dlp not installed", url=url)
        try:
            start = time.time()
            out = str(self.video_dir / '%(title)s_%(id)s.%(ext)s')
            opts = {
                'outtmpl': out, 'format': self.config.get('quality', 'best[height<=720]'),
                'quiet': True, 'no_warnings': True, 'ignoreerrors': True,
                'continuedl': True, 'retries': self.config.get('max_retries'),
                'fragment_retries': self.config.get('max_retries')
            }
            if self.config.get('proxy'): opts['proxy'] = self.config.get('proxy')
            cookies = self.config_dir / 'cookies.txt'
            if cookies.exists(): opts['cookiefile'] = str(cookies)
            
            prog = {'downloaded': 0, 'total': 0, 'speed': 0}
            def hook(d):
                if d['status'] == 'downloading':
                    prog['downloaded'] = d.get('downloaded_bytes', 0)
                    prog['total'] = d.get('total_bytes', 0) or d.get('total_bytes_estimate', 0)
                    prog['speed'] = d.get('speed', 0)
                    if TQDM_AVAILABLE and prog['total'] > 0:
                        pct = (prog['downloaded'] / prog['total']) * 100
                        print(f"\r  Job {job_id}: {pct:.1f}% [{self._format_size(prog['downloaded'])}/{self._format_size(prog['total'])}] @ {self._format_size(prog['speed'])}/s", end='')
            opts['progress_hooks'] = [hook]
            
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if not info:
                    return DownloadResult(False, None, MediaType.VIDEO, platform, 0, error="Could not extract info", url=url)
                
                fsize = info.get('filesize') or info.get('filesize_approx')
                if fsize:
                    max_size = self.config.get('max_file_size')
                    if max_size > 0 and fsize > max_size:
                        return DownloadResult(False, None, MediaType.VIDEO, platform, 0, error=f"File too large: {self._format_size(fsize)}", url=url)
                    if not self.check_disk_space(fsize):
                        return DownloadResult(False, None, MediaType.VIDEO, platform, 0, error="Insufficient disk space", url=url)
                
                ydl.download([url])
                
                fn = ydl.prepare_filename(info)
                if not Path(fn).exists():
                    import glob
                    matches = glob.glob(str(self.video_dir / f"*{info['id']}*"))
                    fn = matches[0] if matches else None
                if not fn:
                    return DownloadResult(False, None, MediaType.VIDEO, platform, 0, error="File not found after download", url=url)
                
                size = Path(fn).stat().st_size
                if not self.verify_file_integrity(Path(fn)):
                    Path(fn).unlink()
                    return DownloadResult(False, None, MediaType.VIDEO, platform, 0, error="Integrity check failed", url=url)
                
                elapsed = time.time() - start
                speed = size / elapsed if elapsed > 0 else 0
                if TQDM_AVAILABLE: print()
                return DownloadResult(True, fn, MediaType.VIDEO, platform, size, filename=os.path.basename(fn), url=url, duration=elapsed, speed=speed)
        except Exception as e:
            logger.error(f"Video download error: {e}", exc_info=True)
            return DownloadResult(False, None, MediaType.VIDEO, platform, 0, error=str(e), url=url)
    
    def _download_audio(self, url: str, platform: Platform, job_id: str) -> DownloadResult:
        if not yt_dlp:
            return DownloadResult(False, None, MediaType.AUDIO, platform, 0, error="yt-dlp not installed", url=url)
        try:
            start = time.time()
            opts = {
                'outtmpl': str(self.audio_dir / '%(title)s_%(id)s.%(ext)s'),
                'format': 'bestaudio/best',
                'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}],
                'quiet': True, 'no_warnings': True, 'ignoreerrors': True,
                'retries': self.config.get('max_retries')
            }
            if self.config.get('proxy'): opts['proxy'] = self.config.get('proxy')
            
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if not info:
                    return DownloadResult(False, None, MediaType.AUDIO, platform, 0, error="Could not extract info", url=url)
                
                ydl.download([url])
                
                fn = ydl.prepare_filename(info)
                fn = fn.rsplit('.', 1)[0] + '.mp3'
                if not Path(fn).exists():
                    import glob
                    matches = glob.glob(str(self.audio_dir / f"*{info['id']}*.mp3"))
                    fn = matches[0] if matches else None
                if not fn:
                    return DownloadResult(False, None, MediaType.AUDIO, platform, 0, error="File not found", url=url)
                
                size = Path(fn).stat().st_size
                if not self.verify_file_integrity(Path(fn)):
                    Path(fn).unlink()
                    return DownloadResult(False, None, MediaType.AUDIO, platform, 0, error="Integrity check failed", url=url)
                
                elapsed = time.time() - start
                speed = size / elapsed if elapsed > 0 else 0
                return DownloadResult(True, fn, MediaType.AUDIO, platform, size, filename=os.path.basename(fn), url=url, duration=elapsed, speed=speed)
        except Exception as e:
            logger.error(f"Audio download error: {e}", exc_info=True)
            return DownloadResult(False, None, MediaType.AUDIO, platform, 0, error=str(e), url=url)
    
    def _download_file(self, url: str, platform: Platform, media_type: MediaType, job_id: str) -> DownloadResult:
        if not self.session:
            return DownloadResult(False, None, media_type, platform, 0, error="Requests not available", url=url)
        try:
            target = self.get_target_directory(media_type)
            filename = self.generate_filename(url, platform, media_type)
            filepath = target / filename
            
            resp = self.session.get(url, timeout=self.config.get('timeout'), stream=True)
            if resp.status_code != 200:
                return DownloadResult(False, None, media_type, platform, 0, error=f"HTTP {resp.status_code}", url=url)
            
            total = int(resp.headers.get('content-length', 0))
            max_size = self.config.get('max_file_size')
            if max_size > 0 and total > max_size:
                return DownloadResult(False, None, media_type, platform, 0, error=f"File too large: {self._format_size(total)}", url=url)
            if total > 0 and not self.check_disk_space(total):
                return DownloadResult(False, None, media_type, platform, 0, error="Insufficient disk space", url=url)
            
            downloaded = 0; start = time.time()
            with open(filepath, 'wb') as f:
                for chunk in resp.iter_content(self.config.get('chunk_size')):
                    if chunk:
                        f.write(chunk); downloaded += len(chunk)
                        self.rate_limiter.limit(len(chunk))
                        if TQDM_AVAILABLE and total > 0:
                            elapsed = time.time() - start
                            speed = downloaded / elapsed if elapsed > 0 else 0
                            print(f"\r  Job {job_id}: {(downloaded/total)*100:.1f}% [{self._format_size(downloaded)}/{self._format_size(total)}] @ {self._format_size(speed)}/s", end='')
            if TQDM_AVAILABLE: print()
            
            if not self.verify_file_integrity(filepath):
                filepath.unlink()
                return DownloadResult(False, None, media_type, platform, 0, error="Integrity check failed", url=url)
            
            elapsed = time.time() - start
            speed = downloaded / elapsed if elapsed > 0 else 0
            return DownloadResult(True, str(filepath), media_type, platform, downloaded, filename=filename, url=url, duration=elapsed, speed=speed)
        except Exception as e:
            logger.error(f"File download error: {e}", exc_info=True)
            return DownloadResult(False, None, media_type, platform, 0, error=str(e), url=url)
    
    def _download_generic(self, url: str, platform: Platform, job_id: str) -> DownloadResult:
        if not self.session:
            return DownloadResult(False, None, MediaType.UNKNOWN, platform, 0, error="Requests not available", url=url)
        try:
            resp = self.session.head(url, timeout=10, allow_redirects=True)
            ct = resp.headers.get('content-type', '').lower()
            if 'video' in ct: return self._download_video(url, platform, job_id)
            if 'image' in ct: return self._download_image(url, platform, job_id)
            if 'audio' in ct: return self._download_audio(url, platform, job_id)
            if 'pdf' in ct or 'document' in ct: return self._download_file(url, platform, MediaType.DOCUMENT, job_id)
            if 'zip' in ct or 'archive' in ct: return self._download_file(url, platform, MediaType.ARCHIVE, job_id)
            return self._download_file(url, platform, MediaType.UNKNOWN, job_id)
        except Exception as e:
            logger.warning(f"Generic download falling back to image: {e}")
            return self._download_image(url, platform, job_id)
    
    def batch_download(self, urls: List[str], max_workers: Optional[int] = None):
        if not urls: logger.warning("No URLs to download"); return
        
        valid, invalid = [], []
        for url in urls:
            is_valid, url_or_err, _ = self.validate_url(url)
            if is_valid: valid.append(url_or_err)
            else: invalid.append((url, url_or_err))
        
        if invalid:
            logger.warning(f"Found {len(invalid)} invalid URLs")
            for url, err in invalid[:5]: logger.warning(f"  Invalid: {url[:50]}... - {err}")
        if not valid: logger.error("No valid URLs"); return
        
        logger.info(f"Starting batch download of {len(valid)} URLs")
        workers = max_workers or self.config.get('concurrent_downloads', 2)
        workers = min(workers, len(valid), os.cpu_count() or 4)
        
        with ThreadPoolExecutor(max_workers=workers) as exec:
            futures = {exec.submit(self.download_media, url, i): url for i, url in enumerate(valid)}
            for f in as_completed(futures):
                url = futures[f]
                try:
                    res = f.result(timeout=300)
                    if res and res.success: logger.info(f"✓ Completed: {Path(res.filepath).name}")
                    else: logger.error(f"✗ Failed: {url[:50]}...")
                except Exception as e: logger.error(f"✗ Error processing {url[:50]}...: {e}")
    
    def process_file(self, file_path: str):
        try:
            with open(file_path, 'r') as f:
                urls = [line.strip() for line in f if line.strip() and not line.startswith(('#', '//', '--'))]
            logger.info(f"Loaded {len(urls)} URLs from {file_path}")
            self.batch_download(urls)
        except FileNotFoundError: logger.error(f"File not found: {file_path}")
        except Exception as e: logger.error(f"Error processing file: {e}")
    
    def resume_session(self):
        if self._load_state():
            logger.info("Resuming previous session")
            incomplete = [job for job in self.active_jobs.values() if job.status in [DownloadStatus.PENDING, DownloadStatus.DOWNLOADING]]
            if incomplete:
                logger.info(f"Resuming {len(incomplete)} incomplete downloads")
                self.batch_download([job.url for job in incomplete])
            else: logger.info("No incomplete downloads")
    
    def show_dashboard(self):
        width = shutil.get_terminal_size().columns
        while True:
            os.system('clear' if os.name == 'posix' else 'cls')
            print(f"{Fore.CYAN}{Style.BRIGHT}╔{'═'*(width-2)}╗")
            print(f"║{'CSK DOWNLOADER DASHBOARD'.center(width-2)}║")
            print(f"╠{'═'*(width-2)}╣{Style.RESET_ALL}")
            
            s = self.tracker.get_summary()
            print(f"║ {Fore.WHITE}Total: {s['total']}  {Fore.GREEN}Success: {s['success']}  {Fore.RED}Failed: {s['failed']}  {Fore.YELLOW}Active: {len(self.active_jobs)}{Style.RESET_ALL}")
            print(f"║ {Fore.CYAN}Size: {self._format_size(s['total_size'])}  Speed: {self._format_size(s['avg_speed'])}/s  Rate: {s['success_rate']:.1f}%{Style.RESET_ALL}")
            
            if self.active_jobs:
                print(f"╠{'═'*(width-2)}╣")
                print(f"║ {Fore.YELLOW}ACTIVE DOWNLOADS{Style.RESET_ALL}")
                for job_id, job in list(self.active_jobs.items())[:5]:
                    elapsed = datetime.now() - job.start_time if job.start_time else timedelta(0)
                    print(f"║  {Fore.CYAN}{job_id[:8]}{Style.RESET_ALL}: {job.url[:40]}... [{elapsed.seconds}s]")
            
            print(f"╚{'═'*(width-2)}╝")
            print(f"{Fore.GREEN}Press Ctrl+C to exit dashboard{Style.RESET_ALL}")
            time.sleep(2)
    
    def interactive_menu(self):
        while True:
            os.system('clear' if os.name == 'posix' else 'cls')
            print(BANNER)
            print(f"\n{Fore.CYAN}{Style.BRIGHT}MAIN MENU{Style.RESET_ALL}")
            print(f"{'='*50}")
            print(f"{Fore.GREEN}1.{Style.RESET_ALL} Download Single URL")
            print(f"{Fore.GREEN}2.{Style.RESET_ALL} Download Multiple URLs")
            print(f"{Fore.GREEN}3.{Style.RESET_ALL} Batch from File")
            print(f"{Fore.GREEN}4.{Style.RESET_ALL} Show Statistics")
            print(f"{Fore.GREEN}5.{Style.RESET_ALL} Resume Session")
            print(f"{Fore.GREEN}6.{Style.RESET_ALL} Show Dashboard")
            print(f"{Fore.GREEN}7.{Style.RESET_ALL} Clear Cache")
            print(f"{Fore.GREEN}8.{Style.RESET_ALL} Configuration")
            print(f"{Fore.RED}0.{Style.RESET_ALL} Exit")
            print(f"{'='*50}")
            
            choice = input(f"{Fore.CYAN}Enter choice (0-9): {Style.RESET_ALL}").strip()
            
            if choice == '1':
                url = input(f"{Fore.YELLOW}Enter URL: {Style.RESET_ALL}").strip()
                if url:
                    self.download_media(url)
                    print(self.tracker.get_formatted_summary())
                    input(f"\n{Fore.GREEN}Press Enter to continue...{Style.RESET_ALL}")
            elif choice == '2':
                print(f"{Fore.YELLOW}Enter URLs (one per line, empty line to finish):{Style.RESET_ALL}")
                urls = []
                while True:
                    url = input().strip()
                    if not url: break
                    urls.append(url)
                if urls:
                    self.batch_download(urls)
                    print(self.tracker.get_formatted_summary())
                    input(f"\n{Fore.GREEN}Press Enter to continue...{Style.RESET_ALL}")
            elif choice == '3':
                file_path = input(f"{Fore.YELLOW}Enter file path: {Style.RESET_ALL}").strip()
                if file_path:
                    self.process_file(file_path)
                    print(self.tracker.get_formatted_summary())
                    input(f"\n{Fore.GREEN}Press Enter to continue...{Style.RESET_ALL}")
            elif choice == '4':
                print(self.tracker.get_formatted_summary())
                input(f"\n{Fore.GREEN}Press Enter to continue...{Style.RESET_ALL}")
            elif choice == '5':
                self.resume_session()
                input(f"\n{Fore.GREEN}Press Enter to continue...{Style.RESET_ALL}")
            elif choice == '6':
                try: self.show_dashboard()
                except KeyboardInterrupt: pass
            elif choice == '7':
                self.cache.clear()
                print(f"{Fore.GREEN}Cache cleared{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Press Enter to continue...{Style.RESET_ALL}")
            elif choice == '8':
                self._config_menu()
            elif choice == '0':
                print(f"\n{Fore.GREEN}Thank you for using CSK Downloader!{Style.RESET_ALL}")
                self._save_state()
                break
    
    def _config_menu(self):
        while True:
            os.system('clear' if os.name == 'posix' else 'cls')
            print(f"\n{Fore.CYAN}{Style.BRIGHT}CONFIGURATION MENU{Style.RESET_ALL}")
            print(f"{'='*50}")
            print(f"\n{Fore.YELLOW}Current Settings:{Style.RESET_ALL}")
            for k, v in self.config.config.items():
                if not k.startswith('_'): print(f"  {Fore.CYAN}{k}:{Style.RESET_ALL} {v}")
            print(f"\n{Fore.GREEN}1.{Style.RESET_ALL} Change Setting")
            print(f"{Fore.GREEN}2.{Style.RESET_ALL} Apply Profile")
            print(f"{Fore.GREEN}3.{Style.RESET_ALL} Reset to Defaults")
            print(f"{Fore.RED}0.{Style.RESET_ALL} Back")
            print(f"{'='*50}")
            
            choice = input(f"{Fore.CYAN}Enter choice (0-3): {Style.RESET_ALL}").strip()
            if choice == '1':
                key = input("Setting name: ").strip()
                if key in self.config.config:
                    val = input(f"Value for {key}: ").strip()
                    if val.isdigit(): val = int(val)
                    elif val.replace('.','').isdigit(): val = float(val)
                    elif val.lower() in ['true','false']: val = val.lower() == 'true'
                    self.config.set(key, val)
                    print(f"{Fore.GREEN}Updated{Style.RESET_ALL}")
                else: print(f"{Fore.RED}Unknown{Style.RESET_ALL}")
                time.sleep(1)
            elif choice == '2':
                print(f"Profiles: {', '.join(ConfigManager.PROFILES.keys())}")
                prof = input("Profile name: ").strip()
                if self.config.apply_profile(prof): print(f"{Fore.GREEN}Applied{Style.RESET_ALL}")
                else: print(f"{Fore.RED}Not found{Style.RESET_ALL}")
                time.sleep(1)
            elif choice == '3':
                if input("Reset to defaults? (y/n): ").lower() == 'y':
                    self.config.config = self.config._load_defaults()
                    self.config.save_config()
                    print(f"{Fore.GREEN}Reset{Style.RESET_ALL}")
                time.sleep(1)
            elif choice == '0': break

def main():
    parser = argparse.ArgumentParser(description=f"CSK Downloader v{VERSION} - Self-Healing Edition")
    parser.add_argument('--url', '-u', help='Single URL to download')
    parser.add_argument('--file', '-f', help='File containing URLs')
    parser.add_argument('--output', '-o', default='downloads', help='Output directory')
    parser.add_argument('--interactive', '-i', action='store_true', help='Interactive mode')
    parser.add_argument('--resume', '-r', action='store_true', help='Resume previous session')
    parser.add_argument('--profile', '-p', help='Apply configuration profile')
    parser.add_argument('--concurrent', '-c', type=int, help='Max concurrent downloads')
    parser.add_argument('--version', '-v', action='version', version=f'CSK Downloader v{VERSION}')
    
    args = parser.parse_args()
    
    os.system('clear' if os.name == 'posix' else 'cls')
    print(BANNER)
    print(f"{Fore.CYAN}System: {platform.platform()}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Python: {platform.python_version()}{Style.RESET_ALL}\n")
    
    downloader = AdvancedDownloader(args.output)
    
    if args.profile: downloader.config.apply_profile(args.profile)
    if args.concurrent: downloader.config.set('concurrent_downloads', args.concurrent)
    
    try:
        if args.resume: downloader.resume_session()
        elif args.interactive: downloader.interactive_menu()
        elif args.url:
            downloader.download_media(args.url)
            print(downloader.tracker.get_formatted_summary())
        elif args.file:
            downloader.process_file(args.file)
            print(downloader.tracker.get_formatted_summary())
        else: downloader.interactive_menu()
    except KeyboardInterrupt:
        logger.warning("\nCancelled by user")
        print(downloader.tracker.get_formatted_summary())
        return 130
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"\n{Fore.RED}❌ Error: {e}{Style.RESET_ALL}")
        return 1
    
    downloader._save_state()
    print(f"\n{Fore.GREEN}✅ Session completed{Style.RESET_ALL}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
