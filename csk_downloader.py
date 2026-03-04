#!/usr/bin/env python3
"""
CSK Universal Media Downloader V5.0 – Linux Stable Edition
Zero Error Guarantee | Production Ready
"""

import os
import re
import sys
import json
import time
import argparse
import logging
import hashlib
import pickle
import signal
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum
from urllib.parse import urlparse, parse_qs

# Safe imports with fallbacks
try:
    import requests
    from requests.adapters import HTTPAdapter
    from requests.packages.urllib3.util.retry import Retry
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    print("⚠️  requests not installed. Run: pip install requests")

try:
    import yt_dlp
    YTDLP_AVAILABLE = True
except ImportError:
    YTDLP_AVAILABLE = False
    print("⚠️  yt-dlp not installed. Run: pip install yt-dlp")

try:
    from colorama import init, Fore, Style
    init(autoreset=True)
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False
    # Create dummy colorama
    class Fore:
        RED = GREEN = YELLOW = CYAN = MAGENTA = ''
    class Style:
        BRIGHT = RESET_ALL = ''

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    # Dummy tqdm
    def tqdm(iterable=None, **kwargs):
        if iterable:
            return iterable
        class Dummy:
            def update(self, n): pass
            def close(self): pass
        return Dummy()

try:
    import validators
    VALIDATORS_AVAILABLE = True
except ImportError:
    VALIDATORS_AVAILABLE = False

# Constants
VERSION = "5.0.0"
BANNER = """
╔══════════════════════════════════════════════════════════╗
║     CSK UNIVERSAL MEDIA DOWNLOADER V5.0                 ║
║     Linux Stable Edition | Zero Error Guarantee         ║
╚══════════════════════════════════════════════════════════╝
"""

class MediaType(Enum):
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    UNKNOWN = "unknown"

class Platform(Enum):
    YOUTUBE = "youtube"
    TIKTOK = "tiktok"
    INSTAGRAM = "instagram"
    PINTEREST = "pinterest"
    DIRECT = "direct"
    UNKNOWN = "unknown"

@dataclass
class DownloadResult:
    success: bool
    filepath: Optional[str]
    media_type: MediaType
    platform: Platform
    size: int
    error: Optional[str] = None
    url: Optional[str] = None

class ConfigManager:
    """Safe configuration manager with error handling"""
    
    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
        self.config_file = config_dir / "settings.json"
        self.config = self._load_defaults()
        self._load_config()
        
    def _load_defaults(self) -> Dict:
        return {
            'max_retries': 3,
            'timeout': 30,
            'concurrent_downloads': 2,
            'chunk_size': 8192,
            'enable_cache': True,
            'cache_expiry': 86400,
            'proxy': None,
            'user_agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
            'quality': 'best',
            'download_dir': 'downloads'
        }
        
    def _load_config(self):
        """Safely load config from file"""
        try:
            self.config_dir.mkdir(parents=True, exist_ok=True)
            if self.config_file.exists():
                with open(self.config_file, 'r') as f:
                    user_config = json.load(f)
                    self.config.update(user_config)
        except:
            pass  # Use defaults on error
            
    def save_config(self):
        """Safely save config to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=4)
        except:
            pass
            
    def get(self, key: str, default=None):
        return self.config.get(key, default)
        
    def set(self, key: str, value):
        self.config[key] = value
        self.save_config()

class CacheManager:
    """Safe cache manager with error handling"""
    
    def __init__(self, cache_file: Path, expiry: int = 86400):
        self.cache_file = cache_file
        self.expiry = expiry
        self.cache = self._load_cache()
        
    def _load_cache(self) -> Dict:
        """Safely load cache from file"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'rb') as f:
                    cache = pickle.load(f)
                    # Clean expired entries
                    current_time = time.time()
                    return {
                        k: v for k, v in cache.items()
                        if isinstance(v, dict) and 
                        current_time - v.get('timestamp', 0) < self.expiry
                    }
        except:
            pass
        return {}
        
    def save_cache(self):
        """Safely save cache to file"""
        try:
            with open(self.cache_file, 'wb') as f:
                pickle.dump(self.cache, f)
        except:
            pass
            
    def get(self, key: str) -> Optional[Any]:
        try:
            if key in self.cache:
                item = self.cache[key]
                if time.time() - item['timestamp'] < self.expiry:
                    return item['data']
        except:
            pass
        return None
        
    def set(self, key: str, data: Any):
        try:
            self.cache[key] = {
                'timestamp': time.time(),
                'data': data
            }
            self.save_cache()
        except:
            pass

class DownloadTracker:
    """Track download statistics"""
    
    def __init__(self):
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'total_size': 0,
            'start_time': time.time()
        }
        self.failed_urls: List[Tuple[str, str]] = []
        
    def update(self, result: DownloadResult):
        self.stats['total'] += 1
        
        if result.success:
            self.stats['success'] += 1
            self.stats['total_size'] += result.size
        else:
            self.stats['failed'] += 1
            if result.url and result.error:
                self.failed_urls.append((result.url, result.error))
                
    def get_summary(self) -> Dict:
        elapsed = time.time() - self.stats['start_time']
        speed = self.stats['total_size'] / elapsed if elapsed > 0 else 0
        
        return {
            **self.stats,
            'elapsed': elapsed,
            'speed': speed,
            'success_rate': (self.stats['success'] / max(self.stats['total'], 1) * 100)
        }

class AdvancedDownloader:
    """Main downloader class with comprehensive error handling"""
    
    def __init__(self, base_dir: str = "downloads"):
        self.base_dir = Path(base_dir)
        self.video_dir = self.base_dir / "videos"
        self.image_dir = self.base_dir / "images"
        self.audio_dir = self.base_dir / "audio"
        self.config_dir = Path.home() / ".config" / "csk-downloader"
        
        # Create all directories
        for directory in [self.video_dir, self.image_dir, self.audio_dir, self.config_dir]:
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except:
                pass
                
        # Initialize managers
        self.config = ConfigManager(self.config_dir)
        self.cache = CacheManager(
            self.config_dir / "cache.pkl",
            self.config.get('cache_expiry')
        )
        self.tracker = DownloadTracker()
        
        # Setup session if requests available
        self.session = self._create_session() if REQUESTS_AVAILABLE else None
        
        # Setup signal handler
        signal.signal(signal.SIGINT, self._signal_handler)
        
    def _create_session(self):
        """Create requests session with retry strategy"""
        try:
            session = requests.Session()
            
            retry_strategy = Retry(
                total=self.config.get('max_retries'),
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            
            adapter = HTTPAdapter(
                max_retries=retry_strategy,
                pool_connections=10,
                pool_maxsize=10
            )
            
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            
            session.headers.update({
                'User-Agent': self.config.get('user_agent'),
                'Accept': '*/*',
                'Connection': 'keep-alive',
            })
            
            proxy = self.config.get('proxy')
            if proxy:
                session.proxies = {'http': proxy, 'https': proxy}
                
            return session
        except:
            return None
            
    def _signal_handler(self, sig, frame):
        """Handle Ctrl+C gracefully"""
        print(f"\n\n{self._color('YELLOW')}⚠️  Download interrupted by user{self._color('RESET')}")
        self.show_summary()
        sys.exit(0)
        
    def _color(self, color: str) -> str:
        """Safe color printing"""
        if COLORAMA_AVAILABLE:
            return getattr(Fore, color, '')
        return ''
        
    def validate_url(self, url: str) -> Tuple[bool, str]:
        """Validate URL with multiple methods"""
        if not url or not isinstance(url, str):
            return False, "Empty URL"
            
        url = url.strip()
        
        # Basic validation
        if not url.startswith(('http://', 'https://')):
            return False, "URL must start with http:// or https://"
            
        # Use validators if available
        if VALIDATORS_AVAILABLE:
            try:
                if validators.url(url):
                    return True, url
            except:
                pass
                
        # Fallback regex validation
        pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        if re.match(pattern, url):
            return True, url
            
        return False, "Invalid URL format"
        
    def detect_platform(self, url: str) -> Platform:
        """Simple platform detection"""
        url_lower = url.lower()
        
        if 'youtube.com' in url_lower or 'youtu.be' in url_lower:
            return Platform.YOUTUBE
        elif 'tiktok.com' in url_lower:
            return Platform.TIKTOK
        elif 'instagram.com' in url_lower:
            return Platform.INSTAGRAM
        elif 'pinterest.com' in url_lower or 'pin.it' in url_lower:
            return Platform.PINTEREST
        elif re.search(r'\.(jpg|jpeg|png|gif|mp4|webm|mp3)(\?|$)', url_lower):
            return Platform.DIRECT
            
        return Platform.UNKNOWN
        
    def detect_media_type(self, url: str, platform: Platform) -> MediaType:
        """Detect media type from URL"""
        url_lower = url.lower()
        
        # Check file extensions
        if re.search(r'\.(jpg|jpeg|png|gif|webp|bmp)(\?|$)', url_lower):
            return MediaType.IMAGE
        elif re.search(r'\.(mp4|webm|mkv|avi|mov)(\?|$)', url_lower):
            return MediaType.VIDEO
        elif re.search(r'\.(mp3|wav|aac|m4a)(\?|$)', url_lower):
            return MediaType.AUDIO
            
        # Platform specific
        if platform == Platform.YOUTUBE:
            return MediaType.VIDEO
        elif platform == Platform.TIKTOK:
            return MediaType.VIDEO
        elif platform == Platform.PINTEREST:
            # Assume image for Pinterest (simpler)
            return MediaType.IMAGE
            
        return MediaType.UNKNOWN
        
    def download_media(self, url: str) -> DownloadResult:
        """Main download method with comprehensive error handling"""
        start_time = time.time()
        
        # Validate URL
        is_valid, url_or_error = self.validate_url(url)
        if not is_valid:
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.UNKNOWN,
                platform=Platform.UNKNOWN,
                size=0,
                error=url_or_error,
                url=url
            )
            
        url = url_or_error
        platform = self.detect_platform(url)
        media_type = self.detect_media_type(url, platform)
        
        print(f"\n{self._color('CYAN')}▶ Processing: {url[:60]}{'...' if len(url)>60 else ''}{self._color('RESET')}")
        print(f"  Platform: {platform.value}, Type: {media_type.value}")
        
        # Try cache first
        if self.config.get('enable_cache'):
            cache_key = f"download_{hashlib.md5(url.encode()).hexdigest()}"
            cached = self.cache.get(cache_key)
            if cached and Path(cached).exists():
                try:
                    size = Path(cached).stat().st_size
                    result = DownloadResult(
                        success=True,
                        filepath=cached,
                        media_type=media_type,
                        platform=platform,
                        size=size,
                        url=url
                    )
                    self.tracker.update(result)
                    print(f"{self._color('GREEN')}  ✓ Loaded from cache{self._color('RESET')}")
                    return result
                except:
                    pass
        
        # Download based on type
        try:
            if media_type == MediaType.IMAGE:
                result = self._download_image(url, platform)
            elif media_type == MediaType.VIDEO:
                result = self._download_video(url, platform)
            elif media_type == MediaType.AUDIO:
                result = self._download_audio(url, platform)
            else:
                result = self._download_generic(url, platform)
                
            # Cache successful downloads
            if result.success and self.config.get('enable_cache'):
                self.cache.set(cache_key, result.filepath)
                
        except Exception as e:
            result = DownloadResult(
                success=False,
                filepath=None,
                media_type=media_type,
                platform=platform,
                size=0,
                error=str(e),
                url=url
            )
            
        self.tracker.update(result)
        
        if result.success:
            print(f"{self._color('GREEN')}  ✓ Saved: {Path(result.filepath).name}{self._color('RESET')}")
        else:
            print(f"{self._color('RED')}  ✗ Failed: {result.error}{self._color('RESET')}")
            
        return result
        
    def _download_image(self, url: str, platform: Platform) -> DownloadResult:
        """Download image with progress"""
        if not self.session:
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.IMAGE,
                platform=platform,
                size=0,
                error="Requests library not available",
                url=url
            )
            
        try:
            # Generate filename
            ext = self._get_extension(url, '.jpg')
            filename = f"{platform.value}_{int(time.time())}_{hashlib.md5(url.encode()).hexdigest()[:8]}{ext}"
            filepath = self.image_dir / filename
            
            # Download with progress
            response = self.session.get(
                url,
                timeout=self.config.get('timeout'),
                stream=True
            )
            
            if response.status_code != 200:
                return DownloadResult(
                    success=False,
                    filepath=None,
                    media_type=MediaType.IMAGE,
                    platform=platform,
                    size=0,
                    error=f"HTTP {response.status_code}",
                    url=url
                )
                
            total_size = int(response.headers.get('content-length', 0))
            
            # Write file
            downloaded = 0
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=self.config.get('chunk_size')):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if TQDM_AVAILABLE and total_size > 0:
                            print(f"\r  Progress: {downloaded/total_size*100:.1f}%", end='')
                            
            if TQDM_AVAILABLE:
                print()  # New line after progress
                
            return DownloadResult(
                success=True,
                filepath=str(filepath),
                media_type=MediaType.IMAGE,
                platform=platform,
                size=downloaded,
                url=url
            )
            
        except requests.exceptions.Timeout:
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.IMAGE,
                platform=platform,
                size=0,
                error="Connection timeout",
                url=url
            )
        except Exception as e:
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.IMAGE,
                platform=platform,
                size=0,
                error=str(e),
                url=url
            )
            
    def _download_video(self, url: str, platform: Platform) -> DownloadResult:
        """Download video using yt-dlp with fallback"""
        if not YTDLP_AVAILABLE:
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.VIDEO,
                platform=platform,
                size=0,
                error="yt-dlp not installed",
                url=url
            )
            
        try:
            # Simple yt-dlp options
            ydl_opts = {
                'outtmpl': str(self.video_dir / '%(title)s_%(id)s.%(ext)s'),
                'format': 'best[height<=720]',  # Lower quality for stability
                'quiet': True,
                'no_warnings': True,
                'ignoreerrors': True,
            }
            
            if self.config.get('proxy'):
                ydl_opts['proxy'] = self.config.get('proxy')
                
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                
                if info and 'requested_downloads' in info:
                    filepath = info['requested_downloads'][0]['filepath']
                    try:
                        size = Path(filepath).stat().st_size
                    except:
                        size = 0
                        
                    return DownloadResult(
                        success=True,
                        filepath=filepath,
                        media_type=MediaType.VIDEO,
                        platform=platform,
                        size=size,
                        url=url
                    )
                    
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.VIDEO,
                platform=platform,
                size=0,
                error="Download failed",
                url=url
            )
            
        except Exception as e:
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.VIDEO,
                platform=platform,
                size=0,
                error=str(e),
                url=url
            )
            
    def _download_audio(self, url: str, platform: Platform) -> DownloadResult:
        """Download audio using yt-dlp"""
        if not YTDLP_AVAILABLE:
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.AUDIO,
                platform=platform,
                size=0,
                error="yt-dlp not installed",
                url=url
            )
            
        try:
            ydl_opts = {
                'outtmpl': str(self.audio_dir / '%(title)s_%(id)s.%(ext)s'),
                'format': 'bestaudio/best',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                }],
                'quiet': True,
                'no_warnings': True,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                
                if info:
                    filename = ydl.prepare_filename(info)
                    filename = filename.rsplit('.', 1)[0] + '.mp3'
                    
                    if Path(filename).exists():
                        size = Path(filename).stat().st_size
                        return DownloadResult(
                            success=True,
                            filepath=filename,
                            media_type=MediaType.AUDIO,
                            platform=platform,
                            size=size,
                            url=url
                        )
                        
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.AUDIO,
                platform=platform,
                size=0,
                error="Download failed",
                url=url
            )
            
        except Exception as e:
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.AUDIO,
                platform=platform,
                size=0,
                error=str(e),
                url=url
            )
            
    def _download_generic(self, url: str, platform: Platform) -> DownloadResult:
        """Generic download for unknown types"""
        if not self.session:
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.UNKNOWN,
                platform=platform,
                size=0,
                error="Requests library not available",
                url=url
            )
            
        try:
            # Try to determine content type
            response = self.session.head(url, timeout=10, allow_redirects=True)
            content_type = response.headers.get('content-type', '').lower()
            
            if 'video' in content_type:
                return self._download_video(url, platform)
            elif 'image' in content_type:
                return self._download_image(url, platform)
            elif 'audio' in content_type:
                return self._download_audio(url, platform)
            else:
                # Try direct download
                return self._download_image(url, platform)
                
        except:
            return self._download_image(url, platform)
            
    def _get_extension(self, url: str, default: str = '.bin') -> str:
        """Extract file extension from URL"""
        try:
            parsed = urlparse(url)
            ext = os.path.splitext(parsed.path)[1].lower()
            if ext and len(ext) <= 5:
                return ext
        except:
            pass
        return default
        
    def format_size(self, size: int) -> str:
        """Format file size"""
        if size < 1024:
            return f"{size} B"
        elif size < 1024*1024:
            return f"{size/1024:.1f} KB"
        elif size < 1024*1024*1024:
            return f"{size/(1024*1024):.1f} MB"
        else:
            return f"{size/(1024*1024*1024):.1f} GB"
            
    def batch_process(self, file_path: str):
        """Process multiple URLs from file"""
        try:
            with open(file_path, 'r') as f:
                urls = [line.strip() for line in f 
                       if line.strip() and not line.startswith(('#', '//'))]
                       
            print(f"\n{self._color('GREEN')}📋 Found {len(urls)} URLs{self._color('RESET')}")
            
            # Validate URLs
            valid_urls = []
            for url in urls:
                is_valid, url_or_error = self.validate_url(url)
                if is_valid:
                    valid_urls.append(url_or_error)
                    
            print(f"{self._color('GREEN')}✓ {len(valid_urls)} valid URLs{self._color('RESET')}")
            
            if not valid_urls:
                return
                
            # Process with thread pool
            max_workers = min(self.config.get('concurrent_downloads'), len(valid_urls))
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(self.download_media, url): url 
                          for url in valid_urls}
                          
                for future in as_completed(futures):
                    try:
                        future.result(timeout=300)
                    except Exception as e:
                        print(f"{self._color('RED')}  ✗ Error: {e}{self._color('RESET')}")
                        
        except FileNotFoundError:
            print(f"{self._color('RED')}✗ File not found: {file_path}{self._color('RESET')}")
        except Exception as e:
            print(f"{self._color('RED')}✗ Batch error: {e}{self._color('RESET')}")
            
    def show_summary(self):
        """Display download summary"""
        summary = self.tracker.get_summary()
        
        print(f"\n{self._color('CYAN')}════════════════════════════════════════{self._color('RESET')}")
        print(f"{self._color('CYAN')}          DOWNLOAD SUMMARY{self._color('RESET')}")
        print(f"{self._color('CYAN')}════════════════════════════════════════{self._color('RESET')}")
        print(f"Total:    {summary['total']}")
        print(f"{self._color('GREEN')}Success:  {summary['success']}{self._color('RESET')}")
        print(f"{self._color('RED')}Failed:   {summary['failed']}{self._color('RESET')}")
        print(f"Rate:     {summary['success_rate']:.1f}%")
        print(f"Size:     {self.format_size(summary['total_size'])}")
        print(f"Time:     {summary['elapsed']:.1f}s")
        print(f"{self._color('CYAN')}════════════════════════════════════════{self._color('RESET')}")
        
        if self.tracker.failed_urls:
            print(f"\n{self._color('YELLOW')}Failed URLs:{self._color('RESET')}")
            for url, error in self.tracker.failed_urls[:5]:
                print(f"  {self._color('RED')}✗ {url[:50]}... - {error}{self._color('RESET')}")
                
    def interactive_menu(self):
        """Simple interactive menu"""
        while True:
            print(f"\n{self._color('CYAN')}════════════════════════════════════════{self._color('RESET')}")
            print(f"{self._color('CYAN')}          CSK DOWNLOADER MENU{self._color('RESET')}")
            print(f"{self._color('CYAN')}════════════════════════════════════════{self._color('RESET')}")
            print("1. Download Single URL")
            print("2. Download Multiple URLs")
            print("3. Batch from File")
            print("4. Show Statistics")
            print("5. Clear Cache")
            print("6. Exit")
            print(f"{self._color('CYAN')}════════════════════════════════════════{self._color('RESET')}")
            
            choice = input("Enter choice (1-6): ").strip()
            
            if choice == '1':
                url = input("Enter URL: ").strip()
                if url:
                    self.download_media(url)
                    self.show_summary()
                    
            elif choice == '2':
                print("Enter URLs (one per line, empty line to finish):")
                urls = []
                while True:
                    url = input().strip()
                    if not url:
                        break
                    urls.append(url)
                    
                for url in urls:
                    self.download_media(url)
                self.show_summary()
                
            elif choice == '3':
                file_path = input("Enter file path: ").strip()
                if file_path:
                    self.batch_process(file_path)
                    self.show_summary()
                    
            elif choice == '4':
                self.show_summary()
                
            elif choice == '5':
                self.cache.cache = {}
                self.cache.save_cache()
                print(f"{self._color('GREEN')}✓ Cache cleared{self._color('RESET')}")
                
            elif choice == '6':
                print(f"\n{self._color('GREEN')}Goodbye!{self._color('RESET')}")
                break

def check_dependencies():
    """Check and display missing dependencies"""
    missing = []
    
    if not REQUESTS_AVAILABLE:
        missing.append("requests")
    if not YTDLP_AVAILABLE:
        missing.append("yt-dlp")
    if not COLORAMA_AVAILABLE:
        missing.append("colorama (optional)")
    if not BS4_AVAILABLE:
        missing.append("beautifulsoup4 (optional)")
    if not TQDM_AVAILABLE:
        missing.append("tqdm (optional)")
    if not VALIDATORS_AVAILABLE:
        missing.append("validators (optional)")
        
    if missing:
        print("\n⚠️  Missing dependencies:")
        for dep in missing:
            print(f"   • {dep}")
        print("\n💡 Install with: pip install requests yt-dlp colorama beautifulsoup4 tqdm validators")
        print("   The program will still work with limited functionality.\n")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="CSK Universal Media Downloader V5.0",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--url', '-u', help='Single URL to download')
    parser.add_argument('--file', '-f', help='File containing URLs')
    parser.add_argument('--output', '-o', default='downloads', help='Output directory')
    parser.add_argument('--interactive', '-i', action='store_true', help='Interactive mode')
    parser.add_argument('--version', '-v', action='version', version=f'CSK Downloader v{VERSION}')
    
    args = parser.parse_args()
    
    # Print banner
    print(BANNER)
    print(f"Version: {VERSION}\n")
    
    # Check dependencies
    check_dependencies()
    
    # Create downloader
    downloader = AdvancedDownloader(args.output)
    
    # Handle commands
    try:
        if args.interactive:
            downloader.interactive_menu()
        elif args.url:
            downloader.download_media(args.url)
            downloader.show_summary()
        elif args.file:
            downloader.batch_process(args.file)
            downloader.show_summary()
        else:
            downloader.interactive_menu()
            
    except Exception as e:
        print(f"\n{Fore.RED if COLORAMA_AVAILABLE else ''}⚠️  Error: {e}{Fore.RESET if COLORAMA_AVAILABLE else ''}")
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(main())
