#!/usr/bin/env python3
"""
Comprehensive Scraper Orchestrator - Enhanced Version
Supports custom timeouts, better error handling, and flexible configuration
Fixed for WSL environment with improved record counting and error handling
"""

import json
import logging
import subprocess
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
import os
import sys
import time
import psutil
import signal
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from typing import List, Dict, Optional, Tuple

class ComprehensiveScraperOrchestrator:
    def __init__(self, base_directory: str = None):
        self.results = []
        # Base directory where all scraper folders are located
        if base_directory:
            self.base_directory = Path(base_directory)
        else:
            # Fixed path for WSL environment
            self.base_directory = Path("/home/karan/projects/reg-intel/Reg Intel/Scripts")
        
        self.setup_logging()
        self.active_processes = []
        
        # Default timeouts for different types of scrapers
        self.default_timeouts = {
            'standard': 300,      # 5 minutes - most scrapers
            'heavy': 600,         # 10 minutes - JavaScript/PDF heavy
            'complex': 480,       # 8 minutes - complex scrapers
            'quick': 180          # 3 minutes - simple scrapers
        }
        
    def setup_logging(self):
        """Setup logging to both file and console"""
        log_dir = self.base_directory / "logs"
        log_dir.mkdir(exist_ok=True)
        
        log_filename = f"scraper_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_path = log_dir / log_filename
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Log startup info
        self.logger.info("="*80)
        self.logger.info("COMPREHENSIVE SCRAPER ORCHESTRATOR - WSL ENHANCED VERSION")
        self.logger.info(f"Base Directory: {self.base_directory}")
        self.logger.info(f"Log File: {log_path}")
        self.logger.info(f"Running on: {os.name} ({'WSL' if 'microsoft' in os.uname().release.lower() else 'Native Linux'})")
        self.logger.info("="*80)
    
    def cleanup_chrome_processes(self):
        """Kill any hanging Chrome processes"""
        try:
            chrome_processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                        # Check if it's a Chrome process started by our scripts
                        cmdline = proc.info.get('cmdline', [])
                        if any(keyword in ' '.join(cmdline).lower() for keyword in 
                               ['headless', 'disable-dev-shm-usage', 'no-sandbox']):
                            chrome_processes.append(proc)
                except (psutil.NoSuchProcess, psutil.AccessDenied, TypeError):
                    continue
            
            if chrome_processes:
                self.logger.info(f"🧹 Cleaning up {len(chrome_processes)} Chrome processes...")
                for proc in chrome_processes:
                    try:
                        proc.terminate()
                        proc.wait(timeout=5)  # Wait up to 5 seconds for graceful shutdown
                    except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                        try:
                            proc.kill()  # Force kill if terminate doesn't work
                        except psutil.NoSuchProcess:
                            pass
                
                time.sleep(2)  # Give time for cleanup
                self.logger.info("✅ Chrome cleanup completed")
            else:
                self.logger.debug("No Chrome processes found to clean up")
                
        except Exception as e:
            self.logger.warning(f"⚠️ Error during Chrome cleanup: {e}")
    
    def count_json_records(self, filepath: Path) -> int:
        """Count records in JSON file - enhanced to handle various structures"""
        try:
            if filepath and filepath.exists():
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # Handle different JSON structures
                    if isinstance(data, list):
                        return len(data)
                    elif isinstance(data, dict):
                        # Check for common patterns where records are nested
                        # Look for keys that might contain the actual data
                        for key in ['data', 'records', 'items', 'results', 'articles', 'news', 'releases', 'entries']:
                            if key in data and isinstance(data[key], list):
                                return len(data[key])
                        # If no common key found, count the dict keys
                        return len(data)
                    else:
                        return 1
            return 0
        except Exception as e:
            self.logger.warning(f"Could not count records in {filepath}: {e}")
            return 0
    
    def find_python_file(self, folder_path: Path) -> Optional[Path]:
        """Find the Python scraper file in the given folder"""
        if not folder_path.exists() or not folder_path.is_dir():
            return None
            
        python_files = list(folder_path.glob("*.py"))
        
        if not python_files:
            return None
        elif len(python_files) == 1:
            return python_files[0]
        else:
            # If multiple .py files, try to find the main scraper
            # Look for files that don't contain common utility names
            excluded_keywords = [
                'test', 'util', 'helper', 'config', '__', 'orchestrator', 
                'wrapper', 'setup', 'install', 'requirements'
            ]
            main_files = [f for f in python_files if not any(
                keyword in f.stem.lower() for keyword in excluded_keywords
            )]
            return main_files[0] if main_files else python_files[0]
    
    def find_json_file(self, folder_path: Path, after_timestamp: float = None) -> Optional[Path]:
        """Find the JSON output file in the data subfolder or main folder"""
        # First check data subfolder
        data_folder = folder_path / "data"
        if data_folder.exists():
            json_files = list(data_folder.glob("*.json"))
            if json_files:
                # If we have a timestamp, filter files modified after it
                if after_timestamp:
                    json_files = [f for f in json_files if f.stat().st_mtime > after_timestamp]
                if json_files:
                    # Return the most recently modified JSON file
                    return max(json_files, key=lambda f: f.stat().st_mtime)
        
        # Fallback: look for JSON files in the main folder
        json_files = list(folder_path.glob("*.json"))
        if json_files:
            if after_timestamp:
                json_files = [f for f in json_files if f.stat().st_mtime > after_timestamp]
            if json_files:
                return max(json_files, key=lambda f: f.stat().st_mtime)
            
        return None
    
    def validate_scraper_config(self, config: Tuple) -> bool:
        """Validate scraper configuration"""
        if not isinstance(config, (tuple, list)) or len(config) < 2:
            return False
        
        # Check if folder exists
        regulator_folder = config[1]
        folder_path = self.base_directory / regulator_folder
        
        return folder_path.exists() and folder_path.is_dir()
    
    def parse_scraper_config(self, config: Tuple) -> Dict:
        """Parse scraper configuration into standardized format"""
        parsed = {
            'display_name': '',
            'folder': '',
            'scripts': None,
            'timeout': self.default_timeouts['standard'],
            'category': 'standard'
        }
        
        if len(config) >= 2:
            parsed['display_name'] = config[0]
            parsed['folder'] = config[1]
        
        if len(config) >= 3 and config[2] is not None:
            parsed['scripts'] = config[2] if isinstance(config[2], list) else [config[2]]
        
        if len(config) >= 4 and isinstance(config[3], int):
            parsed['timeout'] = config[3]
        
        if len(config) >= 5 and isinstance(config[4], str):
            parsed['category'] = config[4]
            # Override timeout based on category if not explicitly set
            if len(config) < 4:
                parsed['timeout'] = self.default_timeouts.get(config[4], self.default_timeouts['standard'])
        
        return parsed
    
    def run_scraper_with_timeout(self, regulator_name: str, folder_path: Path, 
                                specific_script: str = None, timeout: int = 300) -> Dict:
        """Run individual scraper with enforced timeout and resource monitoring"""
        folder = Path(folder_path)
        start_timestamp = time.time()
        
        # Find the Python script
        if specific_script:
            python_file = folder / specific_script
            if not python_file.exists():
                error_msg = f'Specified script {specific_script} not found'
                self.logger.error(f"❌ {regulator_name}: {error_msg}")
                return {
                    'regulator': regulator_name,
                    'status': 'script_not_found',
                    'error': error_msg,
                    'new_records': 0,
                    'execution_time': 0,
                    'timeout_used': timeout
                }
        else:
            python_file = self.find_python_file(folder)
            if not python_file:
                error_msg = 'No Python script found in folder'
                self.logger.error(f"❌ {regulator_name}: {error_msg}")
                return {
                    'regulator': regulator_name,
                    'status': 'no_script',
                    'error': error_msg,
                    'new_records': 0,
                    'execution_time': 0,
                    'timeout_used': timeout
                }
        
        # Find the JSON output file before execution
        json_file_before = self.find_json_file(folder)
        before_count = self.count_json_records(json_file_before) if json_file_before else 0
        
        self.logger.info(f"🔄 Starting {regulator_name} scraper...")
        self.logger.info(f"   📁 Folder: {folder.name}")
        self.logger.info(f"   🐍 Script: {python_file.name}")
        self.logger.info(f"   📄 JSON (before): {json_file_before.name if json_file_before else 'None found'}")
        self.logger.info(f"   📊 Records before: {before_count}")
        self.logger.info(f"   ⏱️ Timeout: {timeout}s ({timeout//60}m {timeout%60}s)")
        
        # Set environment variables to optimize execution for WSL
        env = os.environ.copy()
        env.update({
            'CHROME_HEADLESS': '1',
            'PYTHONUNBUFFERED': '1',  # Ensure real-time output
            'PYTHONIOENCODING': 'utf-8',  # Handle encoding issues
            'DISPLAY': ':0',  # WSL display setting (if needed)
        })
        
        process = None
        
        try:
            # Run the script in its own directory
            process = subprocess.Popen(
                [sys.executable, python_file.name], 
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True, 
                cwd=str(folder),
                env=env,
                # Use process group for better process management in WSL
                preexec_fn=os.setsid if os.name != 'nt' else None
            )
            
            self.active_processes.append(process)
            
            # Wait for completion with timeout
            try:
                stdout, stderr = process.communicate(timeout=timeout)
                execution_time = time.time() - start_timestamp
                
                # Remove from active processes
                if process in self.active_processes:
                    self.active_processes.remove(process)
                
                # Wait a moment for file system to catch up
                time.sleep(0.5)
                
                # Find JSON file after execution (may be newly created)
                json_file_after = self.find_json_file(folder, after_timestamp=start_timestamp)
                if not json_file_after:
                    json_file_after = self.find_json_file(folder)  # Try without timestamp filter
                
                # Count records after
                after_count = self.count_json_records(json_file_after) if json_file_after else 0
                new_records = max(0, after_count - before_count)
                
                # Log the stdout/stderr for debugging
                if stdout and self.logger.level <= logging.DEBUG:
                    self.logger.debug(f"STDOUT from {regulator_name}:\n{stdout[:500]}")
                if stderr and self.logger.level <= logging.DEBUG:
                    self.logger.debug(f"STDERR from {regulator_name}:\n{stderr[:500]}")
                
                if process.returncode == 0:
                    self.logger.info(f"✅ {regulator_name}: SUCCESS")
                    self.logger.info(f"   📊 Records: {before_count} → {after_count} (+{new_records})")
                    self.logger.info(f"   ⏱️ Time: {execution_time:.1f}s")
                    
                    return {
                        'regulator': regulator_name,
                        'status': 'success',
                        'new_records': new_records,
                        'total_records': after_count,
                        'before_count': before_count,
                        'after_count': after_count,
                        'script_file': python_file.name,
                        'json_file': json_file_after.name if json_file_after else 'Unknown',
                        'execution_time': execution_time,
                        'timeout_used': timeout
                    }
                else:
                    # Handle script errors
                    error_msg = stderr.strip() if stderr else stdout.strip()
                    if not error_msg:
                        error_msg = f"Script exited with code {process.returncode}"
                    
                    self.logger.error(f"❌ {regulator_name}: FAILED - {error_msg[:100]}...")
                    return {
                        'regulator': regulator_name,
                        'status': 'failed',
                        'error': error_msg[:500],  # Increased error message length
                        'new_records': new_records,  # Still count any records that were added
                        'before_count': before_count,
                        'after_count': after_count,
                        'script_file': python_file.name,
                        'json_file': json_file_after.name if json_file_after else 'Unknown',
                        'execution_time': execution_time,
                        'timeout_used': timeout
                    }
                    
            except subprocess.TimeoutExpired:
                execution_time = time.time() - start_timestamp
                error_msg = f"Script timeout after {timeout} seconds"
                self.logger.error(f"❌ {regulator_name}: TIMEOUT - {error_msg}")
                
                # Kill the process and all its children (WSL-compatible)
                try:
                    # In WSL/Linux, use process group
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                    time.sleep(2)
                    try:
                        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                    except:
                        pass
                except Exception as kill_error:
                    self.logger.warning(f"Failed to kill process {process.pid}: {kill_error}")
                
                if process in self.active_processes:
                    self.active_processes.remove(process)
                
                return {
                    'regulator': regulator_name,
                    'status': 'timeout',
                    'error': error_msg,
                    'new_records': 0,
                    'before_count': before_count,
                    'after_count': before_count,
                    'script_file': python_file.name,
                    'json_file': json_file_before.name if json_file_before else 'Unknown',
                    'execution_time': execution_time,
                    'timeout_used': timeout
                }
                
        except Exception as e:
            execution_time = time.time() - start_timestamp
            error_msg = f"Exception occurred: {str(e)}"
            self.logger.error(f"❌ {regulator_name}: EXCEPTION - {error_msg}")
            
            if process and process in self.active_processes:
                self.active_processes.remove(process)
            
            return {
                'regulator': regulator_name,
                'status': 'exception',
                'error': error_msg,
                'new_records': 0,
                'before_count': before_count,
                'after_count': before_count,
                'script_file': python_file.name if python_file else 'Unknown',
                'json_file': json_file_before.name if json_file_before else 'Unknown',
                'execution_time': execution_time,
                'timeout_used': timeout
            }
    
    def run_scraper(self, config: Dict) -> Dict:
        """Wrapper for running a single scraper with cleanup"""
        # Clean up any hanging Chrome processes before starting
        self.cleanup_chrome_processes()
        
        regulator_name = config['display_name']
        folder_path = self.base_directory / config['folder']
        timeout = config['timeout']
        
        if config['scripts']:
            # Run specific scripts
            results = []
            for script_name in config['scripts']:
                script_config = config.copy()
                script_config['display_name'] = f"{regulator_name} ({script_name})"
                result = self.run_scraper_with_timeout(
                    script_config['display_name'], 
                    folder_path, 
                    script_name, 
                    timeout
                )
                results.append(result)
                self.results.append(result)
                
                # Brief pause between scripts
                time.sleep(1)
            
            return results[-1] if results else None
        else:
            # Auto-find and run script
            result = self.run_scraper_with_timeout(regulator_name, folder_path, None, timeout)
            self.results.append(result)
            
            # Brief pause between scrapers to allow system recovery
            time.sleep(2)
            
            return result
    
    def run_parallel_scrapers(self, scraper_configs: List[Dict], max_workers: int = 3):
        """Run multiple scrapers in parallel with limited concurrency"""
        self.logger.info(f"🔄 Running {len(scraper_configs)} scrapers with {max_workers} workers...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all scraper tasks
            future_to_config = {}
            for config in scraper_configs:
                regulator_name = config['display_name']
                folder_path = self.base_directory / config['folder']
                timeout = config['timeout']
                
                if config['scripts']:
                    # For parallel execution, run first script only to avoid complexity
                    script_name = config['scripts'][0]
                    display_name = f"{regulator_name} ({script_name})"
                else:
                    script_name = None
                    display_name = regulator_name
                
                future = executor.submit(
                    self.run_scraper_with_timeout, 
                    display_name, 
                    folder_path, 
                    script_name,
                    timeout
                )
                future_to_config[future] = config
            
            # Collect results as they complete
            for future in as_completed(future_to_config):
                config = future_to_config[future]
                try:
                    # Use timeout + 30 seconds for future result
                    result = future.result(timeout=config['timeout'] + 30)
                    self.results.append(result)
                except Exception as e:
                    regulator_name = config['display_name']
                    self.logger.error(f"❌ {regulator_name}: Thread execution failed - {e}")
                    self.results.append({
                        'regulator': regulator_name,
                        'status': 'thread_failed',
                        'error': str(e),
                        'new_records': 0,
                        'execution_time': 0,
                        'timeout_used': config['timeout']
                    })
    
    def send_email_alert(self, subject: str, body: str) -> bool:
        """Send email notification with HTML formatting"""
        # Email configuration - UPDATE THESE VALUES
        email_config = {
            'smtp_server': "smtp.gmail.com",
            'smtp_port': 587,
            'sender_email': "karan.sharma124@gmail.com",  # UPDATE THIS
            'sender_password': "opmk kwvx moer mqda",   # UPDATE THIS (use App Password)
            'recipient_email': "karan.sharma124@gmail.com"  # UPDATE THIS
        }
        
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = email_config['sender_email']
            msg['To'] = email_config['recipient_email']
            
            # Create HTML version
            html_body = body.replace('\n', '<br>')
            html_body = f"""
            <html>
                <body>
                    <pre style='font-family: monospace; font-size: 12px; background-color: #f5f5f5; padding: 10px; border-radius: 5px;'>
                        {html_body}
                    </pre>
                </body>
            </html>
            """
            
            text_part = MIMEText(body, 'plain')
            html_part = MIMEText(html_body, 'html')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
                server.starttls()
                server.login(email_config['sender_email'], email_config['sender_password'])
                server.send_message(msg)
            
            self.logger.info("📧 Email alert sent successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"📧 Failed to send email: {e}")
            return False
    
    def generate_summary(self) -> Tuple[str, int, int]:
        """Generate comprehensive execution summary"""
        total_new_records = sum(r.get('new_records', 0) for r in self.results)
        failed_count = sum(1 for r in self.results if r['status'] not in ['success'])
        success_count = len(self.results) - failed_count
        total_execution_time = sum(r.get('execution_time', 0) for r in self.results)
        
        # Calculate statistics by status
        status_counts = {}
        for result in self.results:
            status = result['status']
            status_counts[status] = status_counts.get(status, 0) + 1
        
        # Calculate timeout statistics
        timeout_stats = {}
        for result in self.results:
            timeout = result.get('timeout_used', 'unknown')
            timeout_stats[timeout] = timeout_stats.get(timeout, 0) + 1
        
        summary = f"""
==================================================
COMPREHENSIVE REGULATORY NEWS SCRAPING REPORT
==================================================
Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Total Regulators: {len(self.results)}
Successful: {success_count}
Failed: {failed_count}
Total New Records: {total_new_records}
Total Execution Time: {total_execution_time:.1f}s ({total_execution_time/60:.1f} minutes)

STATUS BREAKDOWN:
=================================================="""
        
        for status, count in sorted(status_counts.items()):
            summary += f"\n{status.upper()}: {count}"
        
        summary += f"""

TIMEOUT USAGE:
=================================================="""
        
        # Fixed sorting to handle mixed types
        # Separate numeric and non-numeric timeouts
        numeric_timeouts = {k: v for k, v in timeout_stats.items() if isinstance(k, (int, float))}
        non_numeric_timeouts = {k: v for k, v in timeout_stats.items() if not isinstance(k, (int, float))}
        
        # Sort numeric timeouts
        for timeout, count in sorted(numeric_timeouts.items()):
            summary += f"\n{timeout}s: {count} scrapers"
        
        # Add non-numeric timeouts
        for timeout, count in sorted(non_numeric_timeouts.items()):
            if timeout != 'unknown':
                summary += f"\n{timeout}: {count} scrapers"
            else:
                summary += f"\nUnknown timeout: {count} scrapers"
        
        summary += f"""

DETAILED RESULTS:
=================================================="""
        
        # Sort results by status (success first, then by name)
        sorted_results = sorted(self.results, key=lambda x: (x['status'] != 'success', x['regulator']))
        
        for result in sorted_results:
            status_emoji = "✅" if result['status'] == 'success' else "❌"
            regulator_name = result['regulator'][:30].ljust(30)  # Limit name length
            exec_time = result.get('execution_time', 0)
            timeout_used = result.get('timeout_used', 0)
            
            if result['status'] == 'success':
                summary += f"\n{status_emoji} {regulator_name} | New: {result['new_records']:>4} | Total: {result.get('total_records', 0):>6} | Time: {exec_time:>5.1f}s/{timeout_used}s | {result.get('script_file', 'Unknown')}"
            else:
                error_msg = result.get('error', 'Unknown error')[:60]
                summary += f"\n{status_emoji} {regulator_name} | ERROR: {error_msg} | Time: {exec_time:>5.1f}s/{timeout_used}s"
                if result.get('script_file'):
                    summary += f"\n{'':>3} Script: {result.get('script_file', 'Unknown')}"
        
        summary += f"\n==================================================\n"
        
        return summary, total_new_records, failed_count
    
    def cleanup_on_exit(self):
        """Clean up resources on exit"""
        self.logger.info("🧹 Performing final cleanup...")
        
        # Kill any remaining active processes
        for process in self.active_processes[:]:  # Create a copy to iterate over
            try:
                if process.poll() is None:  # Process is still running
                    self.logger.info(f"Terminating active process {process.pid}")
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=2)
            except Exception as e:
                self.logger.warning(f"Error terminating process: {e}")
            finally:
                if process in self.active_processes:
                    self.active_processes.remove(process)
        
        # Clean up Chrome processes
        self.cleanup_chrome_processes()
        
        self.logger.info("✅ Final cleanup completed")
    
    def get_default_regulators(self) -> List[Tuple]:
        """Get default regulator configurations"""
        return [
            # Australian Regulators
            ("AUSTRAC Media", "AUSTRAC", ["austrac_media_releases_scrape.py"], 300, "standard"),
            ("AUSTRAC News", "AUSTRAC", ["austrac_all_news_scrape.py"], 300, "standard"),
            ("ACCC", "ACCC", None, 300, "standard"),
            ("ASIC Media", "ASIC", ["asic_media_releases_scrape.py"], 480, "complex"),
            ("ASIC News", "ASIC", ["asic_news_scrape.py"], 480, "complex"),
            ("ASIC Consultations", "ASIC", ["asic_consultations_scrape.py"], 1500, "giant"),  # JavaScript + PDF
            ("APRA News", "APRA", ["apra_news_scrape.py"], 300, "standard"),
            ("APRA Info Papers", "APRA", ["apra_info_papers_scrape.py"], 900, "heavy"),
            ("APRA Statistical Publications", "APRA", ["apra_statistical_publications_scrape.py"], 900, "heavy"),
            ("APRA Letters", "APRA", ["apra_letters_scrape.py"], 300, "standard"),
            ("APRA Consultations", "APRA", ["apra_consultations_scrape.py"], 480, "heavy"),
            ("AFCA News", "AFCA", ["afca_latest_news_scrape.py"], 300, "standard"),
            ("AFCA Media Release", "AFCA", ["afca_media_releases.py"], 300, "standard"),
            ("RBA Media Releases", "RBA", ["rba_media_releases_scrape.py"], 300, "standard"),
            ("RBA Speeches", "RBA", ["rba_speeches_scrape.py"], 300, "standard"),
            ("RBA News", "RBA", ["rba_all_news_scrape.py"], 300, "standard"),
            ("ATO", "ATO", None, 300, "standard"),
            ("Finance", "FinanceGovAu", None, 300, "standard"),
            ("ACMA Media Releases", "ACMA", ["acma_all_media_releases_scrape.py"], 480, "heavy"),
            ("ACMA News", "ACMA", ["acma_all_news_scrape.py"], 480, "heavy"),
            ("OAIC", "OAIC", None, 300, "standard"),
            ("NTC", "NTC", None, 300, "standard"),
            ("ABS", "ABS", None, 300, "standard"),
            ("AER News", "AER", ["aer_news_scrape.py"], 300, "standard"),
            ("AER Resources", "AER", ["aer_resources_scrape.py"], 300, "standard"),
            ("AHPRA", "AHPRA", None, 300, "standard"),
            ("HealthAU", "HealthAU", None, 300, "standard"),
            ("TGA Alerts", "TGA", ["tga_alerts_scrape.py"], 300, "standard"),
            ("TGA Articles", "TGA", ["tga_articles_scrape.py"], 300, "standard"),
            ("TREASURYAU Consultations", "TREASURYAU", None, 300, "standard"),
            ("AEMO News", "AEMO", ["aemo_news_scrape.py"], 300, "standard"),
            ("AEMO Publications", "AEMO", ["aemo_publication_scrape.py"], 300, "standard"),
            ("AEMO Guides", "AEMO", ["aemo_guide_scrape.py"], 300, "standard"),
            ("AEMO Media Releases", "AEMO", ["aemo_media_releases_scrape.py"], 300, "standard"),
            ("NHVR", "NHVR", None, 300, "standard"),
            ("AMSA", "AMSA", None, 300, "standard"),
            ("TRANSPARENCY_AU", "TRANSPARENCY_AU", None, 480, "heavy"),
            ("AICIS Reg Notices", "AICIS", ["aicis_regNotices_scrape.py"], 300, "standard"),
            ("AICIS News", "AICIS", ["aicis_news_scrape.py"], 300, "standard"),
            ("NOPSEMA", "NOPSEMA", None, 300, "standard"),
            
            
            # New Zealand Regulators
           # ("RBNZ News and Events", "RBNZ", ["rbnz_news_and_events_scrape.py"], 480, "complex"),  
            ("MBIE", "MBIE", None, 300, "standard"),
            ("FMA Articles", "FMA", ["fma_articles_scrape.py"], 300, "standard"),
            ("FMA Media Releases", "FMA", ["fma_media_releases_scrape.py"], 300, "standard"),
            ("FMA Speeches", "FMA", ["fma_speeches_scrape.py"], 300, "standard"),
            ("FMA Guidance", "FMA", ["fma_guidance_scrape.py"], 300, "standard"),
            ("FMA Reports", "FMA", ["fma_reports_scrape.py"], 300, "standard"),
            ("FMA Opinions", "FMA", ["fma_opinions_scrape.py"], 300, "standard"),
            ("COMCOMNZ News", "COMCOMNZ", ["comcom_all_news_scrape.py"], 300, "standard"),
            ("TREASURYNZ News", "TREASURYNZ", ["treasuryNZ_news_scrape.py"], 300, "standard"),
            ("RBNZ News", "RBNZ", ["rbnz_news_scrape.py"], 300, "standard"),
            ("RBNZ Publications", "RBNZ", ["rbnz_publications_scrape.py"], 300, "standard"),
            ("RBNZ Consultations", "RBNZ", ["rbnz_consultations_scrape.py"], 480, "heavy"),
        ]
    
    def run_all_scrapers(self, use_parallel: bool = False, max_workers: int = 3, 
                        custom_regulators: List[Tuple] = None, 
                        filter_categories: List[str] = None,
                        dry_run: bool = False) -> int:
        """Main execution function with enhanced options"""
        start_time = datetime.now()
        summary = None
        total_new_records = 0
        failed_count = 0
        
        self.logger.info("🚀 STARTING COMPREHENSIVE REGULATORY NEWS SCRAPING JOB")
        self.logger.info(f"📁 Base directory: {self.base_directory}")
        self.logger.info(f"⚡ Parallel mode: {use_parallel}")
        self.logger.info(f"👥 Max workers: {max_workers}")
        self.logger.info(f"🧪 Dry run: {dry_run}")
        
        # Initial cleanup
        if not dry_run:
            self.cleanup_chrome_processes()
        
        try:
            # Use custom regulators or default ones
            regulator_configs = custom_regulators or self.get_default_regulators()
            
            # Parse and validate configurations
            parsed_configs = []
            skipped_configs = []
            
            for config in regulator_configs:
                parsed = self.parse_scraper_config(config)
                
                # Apply category filter if specified
                if filter_categories and parsed['category'] not in filter_categories:
                    continue
                
                if self.validate_scraper_config(config):
                    parsed_configs.append(parsed)
                else:
                    skipped_configs.append(parsed)
                    self.logger.warning(f"⚠️ Skipping {parsed['display_name']}: folder not found")
                    self.results.append({
                        'regulator': parsed['display_name'],
                        'status': 'folder_not_found',
                        'error': f'Folder not found: {self.base_directory / parsed["folder"]}',
                        'new_records': 0,
                        'execution_time': 0,
                        'timeout_used': parsed['timeout']
                    })
            
            if not parsed_configs:
                self.logger.error("❌ No valid scraper configurations found")
                return 1
            
            self.logger.info(f"📊 Found {len(parsed_configs)} valid scrapers, {len(skipped_configs)} skipped")
            
            # Log configuration summary
            category_counts = {}
            timeout_counts = {}
            for config in parsed_configs:
                category = config['category']
                timeout = config['timeout']
                category_counts[category] = category_counts.get(category, 0) + 1
                timeout_counts[timeout] = timeout_counts.get(timeout, 0) + 1
            
            self.logger.info(f"📋 Category breakdown: {dict(category_counts)}")
            self.logger.info(f"⏱️ Timeout breakdown: {dict(timeout_counts)}")
            
            if dry_run:
                self.logger.info("🧪 DRY RUN MODE - No scrapers will be executed")
                for config in parsed_configs:
                    self.logger.info(f"   Would run: {config['display_name']} (timeout: {config['timeout']}s)")
                return 0
            
            # Check if base directory exists
            if not self.base_directory.exists():
                self.logger.error(f"❌ Base directory does not exist: {self.base_directory}")
                return 1
            
            if use_parallel:
                # Run scrapers in parallel
                self.logger.info(f"🔄 Running scrapers in parallel with {max_workers} workers...")
                self.run_parallel_scrapers(parsed_configs, max_workers)
            else:
                # Run scrapers sequentially
                self.logger.info("🔄 Running scrapers sequentially...")
                for i, config in enumerate(parsed_configs, 1):
                    self.logger.info(f"🎯 Processing {i}/{len(parsed_configs)}: {config['display_name']}")
                    self.run_scraper(config)
            
            # Generate and log summary
            summary, total_new_records, failed_count = self.generate_summary()
            
            execution_time = datetime.now() - start_time
            summary += f"Total execution completed in {execution_time.total_seconds():.1f} seconds ({execution_time.total_seconds()/60:.1f} minutes)\n"
            
            print(summary)
            self.logger.info("📊 EXECUTION SUMMARY:")
            for line in summary.split('\n'):
                if line.strip():
                    self.logger.info(line)
            
            # Send email alert
            success_rate = ((len(self.results) - failed_count) / len(self.results) * 100) if self.results else 0
            
            if failed_count > len(self.results) // 2:  # More than 50% failed
                subject = f"🚨 REGULATORY SCRAPING CRITICAL - {failed_count}/{len(self.results)} failures, {total_new_records} new records"
            elif failed_count > 0:
                subject = f"⚠️ REGULATORY SCRAPING PARTIAL - {failed_count}/{len(self.results)} failures, {total_new_records} new records"
            else:
                subject = f"✅ REGULATORY SCRAPING SUCCESS - {total_new_records} new records, {success_rate:.1f}% success rate"
            
            email_sent = self.send_email_alert(subject, summary)
            
            self.logger.info(f"🏁 Job completed - {failed_count}/{len(self.results)} failures, {total_new_records} new records")
            self.logger.info(f"📈 Success rate: {success_rate:.1f}%")
            
            # Return exit code based on results
            if failed_count == len(self.results):  # All failed
                return 2
            elif failed_count > len(self.results) // 2:  # More than 50% failed
                return 1
            else:  # Success or minor failures
                return 0
            
        except KeyboardInterrupt:
            self.logger.info("🛑 Job interrupted by user")
            return 1
        except Exception as e:
            self.logger.error(f"💥 Job failed with exception: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            # Try to send error notification email
            try:
                if self.results:  # Only if we have some results
                    # Generate partial summary
                    partial_summary = f"""
SCRAPER ORCHESTRATOR ERROR REPORT
==================================================
Error occurred at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Error: {str(e)}

Partial Results Before Error:
==================================================
Total scrapers attempted: {len(self.results)}
"""
                    for result in self.results[-5:]:  # Last 5 results
                        partial_summary += f"\n{result['regulator']}: {result['status']}"
                    
                    self.send_email_alert(
                        "🚨 REGULATORY SCRAPING ERROR", 
                        partial_summary
                    )
            except Exception as email_error:
                self.logger.error(f"Failed to send error email: {email_error}")
            
            return 1
        finally:
            self.cleanup_on_exit()

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Comprehensive Scraper Orchestrator - WSL Enhanced',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Run all scrapers sequentially
  %(prog)s --parallel --max-workers 3        # Run in parallel with 3 workers
  %(prog)s --category heavy                  # Run only heavy scrapers
  %(prog)s --category standard quick         # Run standard and quick scrapers
  %(prog)s --dry-run                         # Test configuration without running
  %(prog)s --base-dir /path/to/scripts       # Use custom base directory
  %(prog)s --timeout-multiplier 2.0          # Double all timeouts
        """
    )
    
    # Execution mode options
    parser.add_argument('--parallel', action='store_true', 
                       help='Run scrapers in parallel (default: sequential)')
    parser.add_argument('--max-workers', type=int, default=3, 
                       help='Maximum number of parallel workers (default: 3)')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be executed without running scrapers')
    
    # Configuration options
    parser.add_argument('--base-dir', type=str, 
                       help='Base directory containing scraper folders')
    parser.add_argument('--category', action='append', 
                       choices=['standard', 'heavy', 'complex', 'quick'],
                       help='Run only scrapers in specified categories (can be repeated)')
    parser.add_argument('--timeout-multiplier', type=float, default=1.0,
                       help='Multiply all timeouts by this factor (default: 1.0)')
    
    # Logging and notification options
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging')
    parser.add_argument('--quiet', '-q', action='store_true', 
                       help='Suppress console output (log file only)')
    parser.add_argument('--no-email', action='store_true', 
                       help='Disable email notifications')
    
    # Regulator selection options
    parser.add_argument('--include', action='append', 
                       help='Include only specific regulators (can be repeated)')
    parser.add_argument('--exclude', action='append', 
                       help='Exclude specific regulators (can be repeated)')
    
    return parser.parse_args()

def filter_regulators_by_name(regulators: List[Tuple], include: List[str] = None, 
                             exclude: List[str] = None) -> List[Tuple]:
    """Filter regulators by name patterns"""
    if include:
        # Only include regulators matching any of the include patterns
        filtered = []
        for regulator in regulators:
            regulator_name = regulator[0].lower()
            if any(pattern.lower() in regulator_name for pattern in include):
                filtered.append(regulator)
        regulators = filtered
    
    if exclude:
        # Exclude regulators matching any of the exclude patterns
        filtered = []
        for regulator in regulators:
            regulator_name = regulator[0].lower()
            if not any(pattern.lower() in regulator_name for pattern in exclude):
                filtered.append(regulator)
        regulators = filtered
    
    return regulators

def apply_timeout_multiplier(regulators: List[Tuple], multiplier: float) -> List[Tuple]:
    """Apply timeout multiplier to all regulator configurations"""
    if multiplier == 1.0:
        return regulators
    
    modified = []
    for config in regulators:
        config = list(config)  # Convert to list for modification
        
        # Ensure we have at least 4 elements (add timeout if missing)
        while len(config) < 4:
            if len(config) == 3:
                config.append(300)  # Default timeout
            else:
                config.append(None)
        
        # Apply multiplier to timeout
        if isinstance(config[3], (int, float)):
            config[3] = int(config[3] * multiplier)
        
        modified.append(tuple(config))
    
    return modified

def main():
    """Main function with comprehensive argument handling"""
    args = parse_arguments()
    
    try:
        # Create orchestrator with custom base directory if specified
        orchestrator = ComprehensiveScraperOrchestrator(args.base_dir)
        
        # Adjust logging level
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        elif args.quiet:
            # Remove console handler, keep file handler only
            logger = logging.getLogger()
            for handler in logger.handlers[:]:
                if isinstance(handler, logging.StreamHandler) and handler.stream.name == '<stdout>':
                    logger.removeHandler(handler)
        
        # Get default regulators
        regulators = orchestrator.get_default_regulators()
        
        # Apply regulator name filtering
        if args.include or args.exclude:
            regulators = filter_regulators_by_name(regulators, args.include, args.exclude)
            orchestrator.logger.info(f"🎯 Filtered to {len(regulators)} regulators based on include/exclude patterns")
        
        # Apply timeout multiplier
        if args.timeout_multiplier != 1.0:
            regulators = apply_timeout_multiplier(regulators, args.timeout_multiplier)
            orchestrator.logger.info(f"⏱️ Applied timeout multiplier: {args.timeout_multiplier}x")
        
        # Disable email if requested
        if args.no_email:
            orchestrator.send_email_alert = lambda subject, body: True  # Mock function
            orchestrator.logger.info("📧 Email notifications disabled")
        
        # Run the orchestrator
        exit_code = orchestrator.run_all_scrapers(
            use_parallel=args.parallel,
            max_workers=args.max_workers,
            custom_regulators=regulators,
            filter_categories=args.category,
            dry_run=args.dry_run
        )
        
        # Log final exit information
        exit_messages = {
            0: "✅ SUCCESS - All scrapers completed successfully or with minor issues",
            1: "⚠️ PARTIAL SUCCESS - Some scrapers failed but majority succeeded", 
            2: "❌ CRITICAL FAILURE - All or most scrapers failed"
        }
        
        orchestrator.logger.info(f"🏁 Final exit code: {exit_code}")
        orchestrator.logger.info(exit_messages.get(exit_code, f"Unknown exit code: {exit_code}"))
        
        return exit_code
        
    except KeyboardInterrupt:
        print("\n🛑 Script interrupted by user")
        return 1
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)