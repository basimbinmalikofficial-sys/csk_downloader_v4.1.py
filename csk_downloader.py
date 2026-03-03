#!/usr/bin/env python3
"""
CSK Universal Media Downloader – V4.2 Pinterest Optimized
Advanced Production-ready CLI tool with enhanced features
"""

import os
import re
import sys
import json
import time
import argparse
import logging
import urllib.parse
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from dataclasses import dataclass
from enum import Enum
import hashlib
import pickle
from urllib.parse import urlparse, parse_qs

import requests
import yt_dlp
from colorama import init, Fore, Back, Style, init
from bs4 import BeautifulSoup
from tqdm import tqdm
import validators
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Initialize colorama
init(autoreset=True)

# Constants
VERSION = "4.2.0"
BANNER = f"""
{Fore.GREEN}{Style.BRIGHT}
 ██████╗ ███████╗██╗  ██╗    {Fore.CYAN}██╗  ██╗██████╗ 
██╔════╝ ██╔════╝██║ ██╔╝    {Fore.CYAN}██║ ██╔╝██╔══██╗
██║      ███████╗█████╔╝     {Fore.CYAN}█████╔╝ ██████╔╝
██║      ╚════██║██╔═██╗     {Fore.CYAN}██╔═██╗ ██╔══██╗
╚██████╗ ███████║██║  ██╗    {Fore.CYAN}██║  ██╗██║  ██║
 ╚═════╝ ╚══════╝╚═╝  ╚═╝    {Fore.CYAN}╚═╝  ╚═╝╚═╝  ╚═╝
{Fore.YELLOW}
╔══════════════════════════════════════════════════════════╗
║     CSK UNIVERSAL MEDIA DOWNLOADER V4.2                 ║
║     Pinterest Optimized Edition | Advanced Build        ║
║     Enhanced Features: Resume, Cache, Proxy, API       ║
╚══════════════════════════════════════════════════════════╝
{Style.RESET_ALL}
"""

# Enums for better type safety
class MediaType(Enum):
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    GIF = "gif"
    UNKNOWN = "unknown"

class Platform(Enum):
    YOUTUBE = "youtube"
    TIKTOK = "tiktok"
    INSTAGRAM = "instagram"
    FACEBOOK = "facebook"
    TWITTER = "twitter"
    REDDIT = "reddit"
    PINTEREST = "pinterest"
    DIRECT = "direct"
    UNKNOWN = "unknown"

@dataclass
class DownloadResult:
    """Data class for download results"""
    success: bool
    filepath: Optional[str]
    media_type: MediaType
    platform: Platform
    size: int
    duration: float
    error: Optional[str] = None
    url: Optional[str] = None

class ConfigManager:
    """Manage configuration and settings"""
    
    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
        self.config_file = config_dir / "settings.json"
        self.cache_file = config_dir / "cache.pkl"
        self.load_config()
        
    def load_config(self):
        """Load configuration from file"""
        default_config = {
            'max_retries': 3,
            'timeout': 30,
            'concurrent_downloads': 3,
            'chunk_size': 8192,
            'enable_cache': True,
            'cache_expiry': 86400,  # 24 hours
            'proxy': None,
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'quality': 'best',
            'extract_metadata': True,
            'download_thumbnails': False,
            'rename_pattern': '{platform}_{date}_{title}',
            'folder_structure': '{media_type}/{platform}/{date}',
        }
        
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    user_config = json.load(f)
                    self.config = {**default_config, **user_config}
            except:
                self.config = default_config
        else:
            self.config = default_config
            self.save_config()
            
    def save_config(self):
        """Save configuration to file"""
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=4)
            
    def get(self, key: str, default=None):
        """Get configuration value"""
        return self.config.get(key, default)
        
    def set(self, key: str, value):
        """Set configuration value"""
        self.config[key] = value
        self.save_config()

class CacheManager:
    """Manage caching for downloaded content"""
    
    def __init__(self, cache_file: Path, expiry: int = 86400):
        self.cache_file = cache_file
        self.expiry = expiry
        self.cache = self.load_cache()
        
    def load_cache(self) -> Dict:
        """Load cache from file"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'rb') as f:
                    cache = pickle.load(f)
                    # Clean expired entries
                    current_time = time.time()
                    cache = {k: v for k, v in cache.items() 
                            if current_time - v['timestamp'] < self.expiry}
                    return cache
            except:
                return {}
        return {}
        
    def save_cache(self):
        """Save cache to file"""
        with open(self.cache_file, 'wb') as f:
            pickle.dump(self.cache, f)
            
    def get(self, key: str) -> Optional[Dict]:
        """Get cached item"""
        if key in self.cache:
            item = self.cache[key]
            if time.time() - item['timestamp'] < self.expiry:
                return item['data']
            else:
                del self.cache[key]
        return None
        
    def set(self, key: str, data: Any):
        """Set cached item"""
        self.cache[key] = {
            'timestamp': time.time(),
            'data': data
        }
        self.save_cache()

class DownloadTracker:
    """Track download progress and statistics"""
    
    def __init__(self):
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'retries': 0,
            'total_size': 0,
            'start_time': time.time()
        }
        self.failed_urls: List[Tuple[str, str]] = []
        self.successful_urls: List[str] = []
        
    def update(self, result: DownloadResult):
        """Update statistics with download result"""
        self.stats['total'] += 1
        
        if result.success:
            self.stats['success'] += 1
            self.stats['total_size'] += result.size
            self.successful_urls.append(result.url)
        else:
            self.stats['failed'] += 1
            self.failed_urls.append((result.url, result.error))
            
    def get_summary(self) -> Dict:
        """Get download summary"""
        elapsed = time.time() - self.stats['start_time']
        speed = self.stats['total_size'] / elapsed if elapsed > 0 else 0
        
        return {
            **self.stats,
            'elapsed': elapsed,
            'speed': speed,
            'success_rate': (self.stats['success'] / self.stats['total'] * 100 
                           if self.stats['total'] > 0 else 0)
        }

class AdvancedDownloader:
    """Enhanced downloader with advanced features"""
    
    def __init__(self, base_dir: str = "downloads"):
        self.base_dir = Path(base_dir)
        self.video_dir = self.base_dir / "videos"
        self.image_dir = self.base_dir / "images"
        self.audio_dir = self.base_dir / "audio"
        self.temp_dir = self.base_dir / "temp"
        self.log_dir = Path("logs")
        self.config_dir = Path("config")
        
        # Initialize managers
        self.config = ConfigManager(self.config_dir)
        self.cache = CacheManager(
            self.config_dir / "cache.pkl",
            self.config.get('cache_expiry')
        )
        self.tracker = DownloadTracker()
        
        # Setup directories and logging
        self.setup_directories()
        self.setup_logging()
        
        # Initialize session with retry strategy
        self.session = self.create_session()
        
        # Platform patterns
        self.patterns = self.load_patterns()
        
    def create_session(self) -> requests.Session:
        """Create requests session with retry strategy"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=self.config.get('max_retries'),
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,
            pool_maxsize=20
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set headers
        session.headers.update({
            'User-Agent': self.config.get('user_agent'),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Set proxy if configured
        proxy = self.config.get('proxy')
        if proxy:
            session.proxies = {'http': proxy, 'https': proxy}
            
        return session
        
    def setup_directories(self):
        """Create necessary directory structure"""
        for directory in [self.video_dir, self.image_dir, self.audio_dir, 
                         self.temp_dir, self.log_dir, self.config_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            
    def setup_logging(self):
        """Configure enhanced logging"""
        log_file = self.log_dir / f"downloader_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # File handler with detailed format
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        
        # Console handler with simple format
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        
        # Setup logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
    def load_patterns(self) -> Dict:
        """Load platform patterns"""
        return {
            Platform.YOUTUBE: [
                r'(?:https?:\/\/)?(?:www\.)?(?:youtube\.com|youtu\.be)\/',
                r'(?:https?:\/\/)?(?:www\.)?youtube\.com\/shorts\/'
            ],
            Platform.TIKTOK: [
                r'(?:https?:\/\/)?(?:www\.)?tiktok\.com\/',
                r'(?:https?:\/\/)?(?:www\.)?tiktok\.com\/@[\w.-]+\/video\/\d+'
            ],
            Platform.INSTAGRAM: [
                r'(?:https?:\/\/)?(?:www\.)?instagram\.com\/p\/',
                r'(?:https?:\/\/)?(?:www\.)?instagram\.com\/reel\/',
                r'(?:https?:\/\/)?(?:www\.)?instagram\.com\/tv\/'
            ],
            Platform.FACEBOOK: [
                r'(?:https?:\/\/)?(?:www\.)?facebook\.com\/.*\/videos\/',
                r'(?:https?:\/\/)?(?:www\.)?fb\.watch\/'
            ],
            Platform.TWITTER: [
                r'(?:https?:\/\/)?(?:www\.)?(?:twitter\.com|x\.com)\/.*\/status\/\d+'
            ],
            Platform.REDDIT: [
                r'(?:https?:\/\/)?(?:www\.)?reddit\.com\/r\/[\w]+\/comments\/',
                r'(?:https?:\/\/)?(?:www\.)?reddit\.com\/user\/[\w]+\/comments\/',
                r'(?:https?:\/\/)?(?:www\.)?redd\.it\/'
            ],
            Platform.PINTEREST: [
                r'(?:https?:\/\/)?(?:[a-z]+\.)?pinterest\.(?:com|ca|co\.uk|de|fr|es|it|jp|nl|ph|pt|se)\/pin\/',
                r'(?:https?:\/\/)?(?:[a-z]+\.)?pinterest\.(?:com|ca|co\.uk|de|fr|es|it|jp|nl|ph|pt|se)\/[\w-]+\/pin\/'
            ],
            Platform.DIRECT: [
                r'https?:\/\/.+\.(?:jpg|jpeg|png|gif|webp|bmp|mp4|webm|mkv|avi|mov|wmv|flv|3gp|m4a|mp3|wav|aac)(?:\?.*)?$'
            ]
        }
        
    def validate_url(self, url: str) -> Tuple[bool, str]:
        """Enhanced URL validation"""
        if not url or not isinstance(url, str):
            return False, "Empty or invalid URL type"
            
        url = url.strip()
        
        # Check URL format
        if not validators.url(url):
            return False, "Invalid URL format"
            
        # Check URL length
        if len(url) > 2048:
            return False, "URL too long"
            
        # Check for allowed schemes
        if not url.startswith(('http://', 'https://')):
            return False, "Only HTTP/HTTPS URLs are supported"
            
        return True, url
        
    def detect_platform(self, url: str) -> Platform:
        """Enhanced platform detection"""
        url_lower = url.lower()
        
        for platform, patterns in self.patterns.items():
            for pattern in patterns:
                if re.search(pattern, url_lower):
                    return platform
                    
        return Platform.UNKNOWN
        
    def detect_media_type(self, url: str) -> MediaType:
        """Enhanced media type detection"""
        platform = self.detect_platform(url)
        
        # Check direct URLs first
        if platform == Platform.DIRECT:
            ext = os.path.splitext(urlparse(url).path)[1].lower()
            if ext in ['.mp4', '.webm', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.3gp']:
                return MediaType.VIDEO
            elif ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
                return MediaType.IMAGE
            elif ext in ['.mp3', '.wav', '.aac', '.m4a']:
                return MediaType.AUDIO
                
        # Check cache first
        cache_key = f"media_type_{hashlib.md5(url.encode()).hexdigest()}"
        cached = self.cache.get(cache_key)
        if cached:
            return MediaType(cached)
            
        # Detect for each platform
        try:
            if platform == Platform.PINTEREST:
                media_type = self.detect_pinterest_media_type(url)
            elif platform == Platform.INSTAGRAM:
                media_type = self.detect_instagram_media_type(url)
            elif platform == Platform.TWITTER:
                media_type = self.detect_twitter_media_type(url)
            else:
                # For other platforms, use yt-dlp
                media_type = self.detect_with_ytdlp(url)
                
            # Cache the result
            self.cache.set(cache_key, media_type.value)
            return media_type
            
        except Exception as e:
            self.logger.warning(f"Media type detection failed: {e}")
            return MediaType.UNKNOWN
            
    def detect_with_ytdlp(self, url: str) -> MediaType:
        """Detect media type using yt-dlp"""
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': True,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                
                if info.get('is_live'):
                    return MediaType.VIDEO
                    
                if info.get('entries'):
                    # Playlist
                    return MediaType.VIDEO
                    
                # Check formats
                formats = info.get('formats', [])
                if any(f.get('vcodec') != 'none' for f in formats):
                    return MediaType.VIDEO
                elif any(f.get('acodec') != 'none' for f in formats):
                    return MediaType.AUDIO
                    
                return MediaType.UNKNOWN
                
        except:
            return MediaType.UNKNOWN
            
    def detect_pinterest_media_type(self, url: str) -> MediaType:
        """Advanced Pinterest media type detection"""
        try:
            response = self.session.get(url, timeout=self.config.get('timeout'))
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Check multiple video indicators
                video_indicators = [
                    soup.find('meta', {'property': 'og:video'}),
                    soup.find('meta', {'name': 'twitter:player'}),
                    soup.find('video'),
                    soup.find('div', {'data-test-id': 'video-skeleton'}),
                    soup.find('script', {'type': 'application/ld+json'})
                ]
                
                for indicator in video_indicators:
                    if indicator:
                        if indicator.name == 'script' and indicator.string:
                            try:
                                data = json.loads(indicator.string)
                                if '@type' in data and data['@type'] == 'VideoObject':
                                    return MediaType.VIDEO
                            except:
                                pass
                        else:
                            return MediaType.VIDEO
                            
                # Check JSON data
                scripts = soup.find_all('script', {'id': '__PWS_INITIAL_PROPS__'})
                for script in scripts:
                    if script.string:
                        try:
                            data = json.loads(script.string)
                            if 'video' in str(data).lower():
                                return MediaType.VIDEO
                        except:
                            pass
                            
            return MediaType.IMAGE
            
        except Exception as e:
            self.logger.error(f"Pinterest detection error: {e}")
            return MediaType.IMAGE
            
    def detect_instagram_media_type(self, url: str) -> MediaType:
        """Detect Instagram media type (post, reel, story, etc.)"""
        if '/reel/' in url or '/tv/' in url:
            return MediaType.VIDEO
        return MediaType.IMAGE
        
    def detect_twitter_media_type(self, url: str) -> MediaType:
        """Detect Twitter/X media type"""
        if '/video/' in url or '/video/' in url:
            return MediaType.VIDEO
        return MediaType.IMAGE
        
    def download_media(self, url: str) -> DownloadResult:
        """Main download method with enhanced features"""
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
                duration=0,
                error=url_or_error,
                url=url
            )
            
        url = url_or_error
        platform = self.detect_platform(url)
        media_type = self.detect_media_type(url)
        
        print(f"\n{Fore.CYAN}╔══ Processing URL ═══╗{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}Platform: {platform.value}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}Type: {media_type.value}{Style.RESET_ALL}")
        
        # Download based on type
        try:
            if platform == Platform.PINTEREST and media_type == MediaType.IMAGE:
                result = self.download_pinterest_image(url)
            elif media_type == MediaType.IMAGE:
                result = self.download_image(url)
            elif media_type == MediaType.VIDEO:
                result = self.download_video(url)
            elif media_type == MediaType.AUDIO:
                result = self.download_audio(url)
            else:
                # Try generic download
                result = self.generic_download(url)
                
        except Exception as e:
            result = DownloadResult(
                success=False,
                filepath=None,
                media_type=media_type,
                platform=platform,
                size=0,
                duration=time.time() - start_time,
                error=str(e),
                url=url
            )
            
        # Update tracker
        self.tracker.update(result)
        
        # Show result
        if result.success:
            print(f"{Fore.GREEN}✓ Success: {result.filepath}{Style.RESET_ALL}")
            print(f"{Fore.CYAN}Size: {self.format_size(result.size)}{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}✗ Failed: {result.error}{Style.RESET_ALL}")
            
        return result
        
    def download_pinterest_image(self, url: str) -> DownloadResult:
        """Enhanced Pinterest image download with multiple extraction methods"""
        start_time = time.time()
        
        try:
            # Try cache first
            cache_key = f"pinterest_img_{hashlib.md5(url.encode()).hexdigest()}"
            cached = self.cache.get(cache_key)
            if cached and Path(cached).exists():
                size = Path(cached).stat().st_size
                return DownloadResult(
                    success=True,
                    filepath=cached,
                    media_type=MediaType.IMAGE,
                    platform=Platform.PINTEREST,
                    size=size,
                    duration=time.time() - start_time,
                    url=url
                )
                
            response = self.session.get(url, timeout=self.config.get('timeout'))
            
            if response.status_code != 200:
                return DownloadResult(
                    success=False,
                    filepath=None,
                    media_type=MediaType.IMAGE,
                    platform=Platform.PINTEREST,
                    size=0,
                    duration=time.time() - start_time,
                    error=f"HTTP {response.status_code}",
                    url=url
                )
                
            soup = BeautifulSoup(response.text, 'html.parser')
            image_url = None
            
            # Method 1: OpenGraph image
            og_image = soup.find('meta', {'property': 'og:image'})
            if og_image and og_image.get('content'):
                image_url = og_image['content']
                
            # Method 2: Pinterest specific meta
            if not image_url:
                pin_image = soup.find('meta', {'name': 'og:image'})
                if pin_image and pin_image.get('content'):
                    image_url = pin_image['content']
                    
            # Method 3: JSON-LD data
            if not image_url:
                scripts = soup.find_all('script', {'type': 'application/ld+json'})
                for script in scripts:
                    if script.string:
                        try:
                            data = json.loads(script.string)
                            if isinstance(data, dict):
                                if data.get('@type') == 'ImageObject':
                                    image_url = data.get('contentUrl') or data.get('url')
                                    break
                        except:
                            pass
                            
            # Method 4: High-res image from JSON data
            if not image_url:
                scripts = soup.find_all('script', {'id': '__PWS_INITIAL_PROPS__'})
                for script in scripts:
                    if script.string:
                        try:
                            data = json.loads(script.string)
                            # Navigate to find high-res image
                            if 'initialState' in data:
                                pins = data['initialState'].get('pins', {})
                                for pin_id, pin_data in pins.items():
                                    if 'images' in pin_data:
                                        images = pin_data['images']
                                        if 'orig' in images:
                                            image_url = images['orig'].get('url')
                                        elif '736x' in images:
                                            image_url = images['736x'].get('url')
                                        break
                        except:
                            pass
                            
            # Method 5: Regular img tags
            if not image_url:
                img_tags = soup.find_all('img')
                for img in img_tags:
                    src = img.get('src', '')
                    if 'originals' in src or '736x' in src or '1200x' in src:
                        image_url = src
                        # Try to get high-res version
                        if '236x' in src:
                            image_url = src.replace('236x', '736x')
                        break
                        
            if not image_url:
                return DownloadResult(
                    success=False,
                    filepath=None,
                    media_type=MediaType.IMAGE,
                    platform=Platform.PINTEREST,
                    size=0,
                    duration=time.time() - start_time,
                    error="No image URL found",
                    url=url
                )
                
            # Download image with progress
            filename = self.generate_filename(url, Platform.PINTEREST, MediaType.IMAGE)
            filepath = self.image_dir / filename
            
            success, size = self.download_file(image_url, filepath)
            
            if success:
                # Cache the result
                self.cache.set(cache_key, str(filepath))
                
                return DownloadResult(
                    success=True,
                    filepath=str(filepath),
                    media_type=MediaType.IMAGE,
                    platform=Platform.PINTEREST,
                    size=size,
                    duration=time.time() - start_time,
                    url=url
                )
            else:
                return DownloadResult(
                    success=False,
                    filepath=None,
                    media_type=MediaType.IMAGE,
                    platform=Platform.PINTEREST,
                    size=0,
                    duration=time.time() - start_time,
                    error="Download failed",
                    url=url
                )
                
        except Exception as e:
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.IMAGE,
                platform=Platform.PINTEREST,
                size=0,
                duration=time.time() - start_time,
                error=str(e),
                url=url
            )
            
    def download_video(self, url: str) -> DownloadResult:
        """Enhanced video download with yt-dlp"""
        start_time = time.time()
        
        try:
            # Configure yt-dlp options
            ydl_opts = {
                'outtmpl': str(self.video_dir / '%(title)s_%(id)s.%(ext)s'),
                'format': self.config.get('quality'),
                'quiet': True,
                'no_warnings': True,
                'ignoreerrors': True,
                'no_color': True,
                'geo_bypass': True,
                'socket_timeout': self.config.get('timeout'),
                'retries': self.config.get('max_retries'),
                'fragment_retries': self.config.get('max_retries'),
                'continuedl': True,
                'buffersize': 1024,
                'noprogress': True,  # We'll use our own progress
            }
            
            # Add proxy if configured
            if self.config.get('proxy'):
                ydl_opts['proxy'] = self.config.get('proxy')
                
            # Extract metadata if configured
            if self.config.get('extract_metadata'):
                ydl_opts['writethumbnail'] = self.config.get('download_thumbnails')
                ydl_opts['writeinfojson'] = True
                
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Extract info first
                info = ydl.extract_info(url, download=False)
                
                if info is None:
                    return DownloadResult(
                        success=False,
                        filepath=None,
                        media_type=MediaType.VIDEO,
                        platform=self.detect_platform(url),
                        size=0,
                        duration=time.time() - start_time,
                        error="Failed to extract video info",
                        url=url
                    )
                    
                # Get estimated size
                filesize = info.get('filesize') or info.get('filesize_approx', 0)
                
                # Download with progress
                ydl.params['progress_hooks'] = [self.ytdlp_progress_hook]
                ydl.download([url])
                
                # Get downloaded file
                filename = ydl.prepare_filename(info)
                
                if Path(filename).exists():
                    size = Path(filename).stat().st_size
                    return DownloadResult(
                        success=True,
                        filepath=filename,
                        media_type=MediaType.VIDEO,
                        platform=self.detect_platform(url),
                        size=size,
                        duration=time.time() - start_time,
                        url=url
                    )
                else:
                    return DownloadResult(
                        success=False,
                        filepath=None,
                        media_type=MediaType.VIDEO,
                        platform=self.detect_platform(url),
                        size=0,
                        duration=time.time() - start_time,
                        error="File not found after download",
                        url=url
                    )
                    
        except Exception as e:
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.VIDEO,
                platform=self.detect_platform(url),
                size=0,
                duration=time.time() - start_time,
                error=str(e),
                url=url
            )
            
    def ytdlp_progress_hook(self, d):
        """Progress hook for yt-dlp"""
        if d['status'] == 'downloading':
            if 'total_bytes' in d:
                percentage = d['downloaded_bytes'] / d['total_bytes'] * 100
                print(f"\r{Fore.CYAN}Downloading: {percentage:.1f}%{Style.RESET_ALL}", end='')
            elif 'total_bytes_estimate' in d:
                percentage = d['downloaded_bytes'] / d['total_bytes_estimate'] * 100
                print(f"\r{Fore.CYAN}Downloading: {percentage:.1f}% (est){Style.RESET_ALL}", end='')
        elif d['status'] == 'finished':
            print(f"\r{Fore.GREEN}Download complete, now processing...{Style.RESET_ALL}")
            
    def download_image(self, url: str) -> DownloadResult:
        """Enhanced image download with resume support"""
        start_time = time.time()
        
        try:
            filename = self.generate_filename(url, Platform.DIRECT, MediaType.IMAGE)
            filepath = self.image_dir / filename
            
            # Check if partially downloaded
            resume_header = {}
            if filepath.exists():
                resume_size = filepath.stat().st_size
                resume_header = {'Range': f'bytes={resume_size}-'}
                
            response = self.session.get(
                url, 
                headers=resume_header,
                timeout=self.config.get('timeout'),
                stream=True
            )
            
            if response.status_code not in [200, 206]:
                return DownloadResult(
                    success=False,
                    filepath=None,
                    media_type=MediaType.IMAGE,
                    platform=Platform.DIRECT,
                    size=0,
                    duration=time.time() - start_time,
                    error=f"HTTP {response.status_code}",
                    url=url
                )
                
            # Get total size
            total_size = int(response.headers.get('content-length', 0))
            if resume_header and response.status_code == 206:
                total_size += resume_size
                
            # Download with progress
            mode = 'ab' if resume_header else 'wb'
            downloaded = resume_size if resume_header else 0
            
            with open(filepath, mode) as f:
                with tqdm(
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    desc=filename[:30],
                    initial=downloaded,
                    colour='green'
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=self.config.get('chunk_size')):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
                            
            return DownloadResult(
                success=True,
                filepath=str(filepath),
                media_type=MediaType.IMAGE,
                platform=Platform.DIRECT,
                size=total_size,
                duration=time.time() - start_time,
                url=url
            )
            
        except Exception as e:
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.IMAGE,
                platform=Platform.DIRECT,
                size=0,
                duration=time.time() - start_time,
                error=str(e),
                url=url
            )
            
    def download_audio(self, url: str) -> DownloadResult:
        """Download audio from supported platforms"""
        start_time = time.time()
        
        try:
            ydl_opts = {
                'outtmpl': str(self.audio_dir / '%(title)s_%(id)s.%(ext)s'),
                'format': 'bestaudio/best',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
                'quiet': True,
                'no_warnings': True,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                filename = ydl.prepare_filename(info).replace('.webm', '.mp3').replace('.m4a', '.mp3')
                
                if Path(filename).exists():
                    size = Path(filename).stat().st_size
                    return DownloadResult(
                        success=True,
                        filepath=filename,
                        media_type=MediaType.AUDIO,
                        platform=self.detect_platform(url),
                        size=size,
                        duration=time.time() - start_time,
                        url=url
                    )
                    
        except Exception as e:
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.AUDIO,
                platform=self.detect_platform(url),
                size=0,
                duration=time.time() - start_time,
                error=str(e),
                url=url
            )
            
    def generic_download(self, url: str) -> DownloadResult:
        """Generic download for unknown types"""
        start_time = time.time()
        
        try:
            response = self.session.head(url, timeout=self.config.get('timeout'))
            content_type = response.headers.get('content-type', '').lower()
            
            if 'video' in content_type:
                return self.download_video(url)
            elif 'image' in content_type:
                return self.download_image(url)
            elif 'audio' in content_type:
                return self.download_audio(url)
            else:
                return DownloadResult(
                    success=False,
                    filepath=None,
                    media_type=MediaType.UNKNOWN,
                    platform=self.detect_platform(url),
                    size=0,
                    duration=time.time() - start_time,
                    error="Unknown media type",
                    url=url
                )
                
        except Exception as e:
            return DownloadResult(
                success=False,
                filepath=None,
                media_type=MediaType.UNKNOWN,
                platform=self.detect_platform(url),
                size=0,
                duration=time.time() - start_time,
                error=str(e),
                url=url
            )
            
    def download_file(self, url: str, filepath: Path) -> Tuple[bool, int]:
        """Download file with progress"""
        try:
            response = self.session.get(url, stream=True, timeout=self.config.get('timeout'))
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(filepath, 'wb') as f:
                with tqdm(
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    desc=filepath.name[:30],
                    colour='green'
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=self.config.get('chunk_size')):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
                            
            return True, total_size
            
        except Exception as e:
            self.logger.error(f"File download failed: {e}")
            return False, 0
            
    def generate_filename(self, url: str, platform: Platform, media_type: MediaType) -> str:
        """Generate filename based on pattern"""
        try:
            parsed = urlparse(url)
            path = parsed.path
            query = parse_qs(parsed.query)
            
            # Get extension
            ext = os.path.splitext(path)[1]
            if not ext:
                if media_type == MediaType.IMAGE:
                    ext = '.jpg'
                elif media_type == MediaType.VIDEO:
                    ext = '.mp4'
                elif media_type == MediaType.AUDIO:
                    ext = '.mp3'
                else:
                    ext = '.bin'
                    
            # Generate base name
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            
            # Apply pattern
            pattern = self.config.get('rename_pattern')
            name = pattern.format(
                platform=platform.value,
                date=timestamp,
                title=url_hash,
                id=url_hash
            )
            
            # Sanitize
            name = re.sub(r'[<>:"/\\|?*]', '_', name)
            return f"{name}{ext}"
            
        except:
            # Fallback
            return f"{platform.value}_{int(time.time())}.{media_type.value}"
            
    def format_size(self, size: int) -> str:
        """Format file size"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"
        
    def batch_process(self, file_path: str):
        """Enhanced batch processing"""
        try:
            with open(file_path, 'r') as f:
                urls = [line.strip() for line in f 
                       if line.strip() and not line.startswith(('#', '//'))]
                       
            print(f"{Fore.GREEN}╔════════════════════════════════════╗{Style.RESET_ALL}")
            print(f"{Fore.GREEN}║ Batch Processing: {len(urls)} URLs      ║{Style.RESET_ALL}")
            print(f"{Fore.GREEN}╚════════════════════════════════════╝{Style.RESET_ALL}")
            
            # Filter valid URLs
            valid_urls = []
            invalid_urls = []
            
            for url in urls:
                is_valid, url_or_error = self.validate_url(url)
                if is_valid:
                    valid_urls.append(url_or_error)
                else:
                    invalid_urls.append((url, url_or_error))
                    
            if invalid_urls:
                print(f"\n{Fore.YELLOW}Invalid URLs skipped:{Style.RESET_ALL}")
                for url, error in invalid_urls[:5]:
                    print(f"{Fore.RED}  ✗ {url[:50]}... - {error}{Style.RESET_ALL}")
                    
            # Process with thread pool
            max_workers = self.config.get('concurrent_downloads')
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(self.download_media, url): url 
                          for url in valid_urls}
                          
                for future in tqdm(as_completed(futures), total=len(futures),
                                  desc="Processing", unit="url", colour="cyan"):
                    try:
                        future.result(timeout=300)  # 5 minute timeout per URL
                    except TimeoutError:
                        self.logger.error(f"Timeout processing URL")
                    except Exception as e:
                        self.logger.error(f"Error in batch processing: {e}")
                        
        except FileNotFoundError:
            print(f"{Fore.RED}[!] File not found: {file_path}{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}[!] Batch processing error: {str(e)}{Style.RESET_ALL}")
            
    def show_advanced_summary(self):
        """Display enhanced download summary"""
        summary = self.tracker.get_summary()
        
        print(f"\n{Fore.CYAN}╔════════════════════════════════════╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║     DOWNLOAD COMPLETE              ║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╠════════════════════════════════════╣{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL} Total URLs: {summary['total']:<16}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL} Successful: {Fore.GREEN}{summary['success']:<16}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL} Failed: {Fore.RED}{summary['failed']:<19}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL} Success Rate: {summary['success_rate']:.1f}%{' ':<11}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL} Total Size: {self.format_size(summary['total_size']):<13}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL} Time: {summary['elapsed']:.1f}s{' ':<18}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL} Speed: {self.format_size(summary['speed'])}/s{' ':<13}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚════════════════════════════════════╝{Style.RESET_ALL}")
        
        if self.tracker.failed_urls:
            print(f"\n{Fore.YELLOW}Failed URLs:{Style.RESET_ALL}")
            for url, error in self.tracker.failed_urls:
                print(f"{Fore.RED}  ✗ {url[:60]}... - {error}{Style.RESET_ALL}")
                
    def interactive_menu(self):
        """Interactive menu for advanced options"""
        while True:
            print(f"\n{Fore.CYAN}╔════════════════════════════════════╗{Style.RESET_ALL}")
            print(f"{Fore.CYAN}║     INTERACTIVE MENU               ║{Style.RESET_ALL}")
            print(f"{Fore.CYAN}╠════════════════════════════════════╣{Style.RESET_ALL}")
            print(f"{Fore.CYAN}║{Style.RESET_ALL} 1. Download Single URL         {Fore.CYAN}║{Style.RESET_ALL}")
            print(f"{Fore.CYAN}║{Style.RESET_ALL} 2. Download Multiple URLs      {Fore.CYAN}║{Style.RESET_ALL}")
            print(f"{Fore.CYAN}║{Style.RESET_ALL} 3. Batch from File             {Fore.CYAN}║{Style.RESET_ALL}")
            print(f"{Fore.CYAN}║{Style.RESET_ALL} 4. Show Statistics             {Fore.CYAN}║{Style.RESET_ALL}")
            print(f"{Fore.CYAN}║{Style.RESET_ALL} 5. Configure Settings          {Fore.CYAN}║{Style.RESET_ALL}")
            print(f"{Fore.CYAN}║{Style.RESET_ALL} 6. Clear Cache                 {Fore.CYAN}║{Style.RESET_ALL}")
            print(f"{Fore.CYAN}║{Style.RESET_ALL} 7. Exit                        {Fore.CYAN}║{Style.RESET_ALL}")
            print(f"{Fore.CYAN}╚════════════════════════════════════╝{Style.RESET_ALL}")
            
            choice = input(f"{Fore.GREEN}Enter choice (1-7): {Style.RESET_ALL}").strip()
            
            if choice == '1':
                url = input(f"{Fore.GREEN}Enter URL: {Style.RESET_ALL}").strip()
                if url:
                    self.download_media(url)
                    
            elif choice == '2':
                print(f"{Fore.YELLOW}Enter URLs (one per line, empty line to finish):{Style.RESET_ALL}")
                urls = []
                while True:
                    url = input(f"{Fore.GREEN}URL: {Style.RESET_ALL}").strip()
                    if not url:
                        break
                    urls.append(url)
                    
                if urls:
                    for url in urls:
                        self.download_media(url)
                        
            elif choice == '3':
                file_path = input(f"{Fore.GREEN}Enter file path: {Style.RESET_ALL}").strip()
                if file_path:
                    self.batch_process(file_path)
                    
            elif choice == '4':
                self.show_advanced_summary()
                
            elif choice == '5':
                self.configure_settings()
                
            elif choice == '6':
                self.cache.cache = {}
                self.cache.save_cache()
                print(f"{Fore.GREEN}Cache cleared!{Style.RESET_ALL}")
                
            elif choice == '7':
                print(f"{Fore.YELLOW}Exiting...{Style.RESET_ALL}")
                break
                
    def configure_settings(self):
        """Interactive configuration"""
        print(f"\n{Fore.CYAN}Current Settings:{Style.RESET_ALL}")
        for key, value in self.config.config.items():
            print(f"{Fore.YELLOW}{key}: {Fore.WHITE}{value}{Style.RESET_ALL}")
            
        print(f"\n{Fore.GREEN}Enter new values (leave empty to keep current):{Style.RESET_ALL}")
        
        new_value = input(f"Max retries [{self.config.get('max_retries')}]: ").strip()
        if new_value:
            self.config.set('max_retries', int(new_value))
            
        new_value = input(f"Timeout [{self.config.get('timeout')}]: ").strip()
        if new_value:
            self.config.set('timeout', int(new_value))
            
        new_value = input(f"Concurrent downloads [{self.config.get('concurrent_downloads')}]: ").strip()
        if new_value:
            self.config.set('concurrent_downloads', int(new_value))
            
        new_value = input(f"Quality [{self.config.get('quality')}]: ").strip()
        if new_value:
            self.config.set('quality', new_value)
            
        new_value = input(f"Proxy (http://proxy:port or empty): ").strip()
        if new_value:
            self.config.set('proxy', new_value)
            
        print(f"{Fore.GREEN}Settings updated!{Style.RESET_ALL}")

def main():
    """Enhanced main entry point"""
    parser = argparse.ArgumentParser(
        description="CSK Universal Media Downloader V4.2 - Advanced Edition",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --url "https://pinterest.com/pin/12345"
  %(prog)s --file links.txt --workers 5
  %(prog)s --interactive
  %(prog)s --config show
        """
    )
    
    parser.add_argument('--url', '-u', help='Single URL to download')
    parser.add_argument('--file', '-f', help='File containing URLs')
    parser.add_argument('--output', '-o', default='downloads', help='Output directory')
    parser.add_argument('--workers', '-w', type=int, help='Number of concurrent downloads')
    parser.add_argument('--quality', '-q', choices=['best', 'worst', 'audio'], 
                       help='Download quality')
    parser.add_argument('--proxy', '-p', help='Proxy server (http://proxy:port)')
    parser.add_argument('--interactive', '-i', action='store_true', 
                       help='Interactive mode')
    parser.add_argument('--config', choices=['show', 'reset'], 
                       help='Configuration options')
    parser.add_argument('--clear-cache', action='store_true', 
                       help='Clear download cache')
    parser.add_argument('--version', '-v', action='version', 
                       version=f'CSK Downloader v{VERSION}')
    
    args = parser.parse_args()
    
    # Display banner
    print(BANNER)
    print(f"{Fore.YELLOW}{'SECURITY NOTICE':^60}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}{'This tool downloads publicly available content only.':^60}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}{'Users must respect platform policies.':^60}{Style.RESET_ALL}\n")
    
    # Initialize downloader
    downloader = AdvancedDownloader(args.output)
    
    # Handle configuration commands
    if args.config:
        if args.config == 'show':
            print(f"{Fore.CYAN}Current Configuration:{Style.RESET_ALL}")
            for key, value in downloader.config.config.items():
                print(f"{Fore.YELLOW}{key}: {Fore.WHITE}{value}{Style.RESET_ALL}")
            return
        elif args.config == 'reset':
            downloader.config.config = {}
            downloader.config.load_config()
            print(f"{Fore.GREEN}Configuration reset to defaults{Style.RESET_ALL}")
            return
            
    # Handle cache clear
    if args.clear_cache:
        downloader.cache.cache = {}
        downloader.cache.save_cache()
        print(f"{Fore.GREEN}Cache cleared{Style.RESET_ALL}")
        return
        
    # Apply command line overrides
    if args.workers:
        downloader.config.set('concurrent_downloads', args.workers)
    if args.quality:
        downloader.config.set('quality', args.quality)
    if args.proxy:
        downloader.config.set('proxy', args.proxy)
        
    try:
        if args.interactive:
            # Interactive mode
            downloader.interactive_menu()
            
        elif args.url:
            # Single URL mode
            downloader.download_media(args.url)
            downloader.show_advanced_summary()
            
        elif args.file:
            # Batch mode
            downloader.batch_process(args.file)
            downloader.show_advanced_summary()
            
        else:
            # Default interactive mode
            downloader.interactive_menu()
            
    except KeyboardInterrupt:
        print(f"\n\n{Fore.YELLOW}[!] Download interrupted by user{Style.RESET_ALL}")
        downloader.show_advanced_summary()
        sys.exit(0)
        
    except Exception as e:
        print(f"{Fore.RED}[!] Fatal error: {str(e)}{Style.RESET_ALL}")
        downloader.logger.critical(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()