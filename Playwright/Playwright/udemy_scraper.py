# -*- coding: utf-8 -*-
"""
Udemy Scraper v40 - THE PERFECT CODE

CÁC TÍNH NĂNG HOÀN THIỆN:
1. LOGIC GIÁ CHUẨN (v35): Max/Min Strategy + Regex Cleaning + HTML Fallback.
2. LOGIC DỪNG CHUẨN (v24): Dừng khi 2 trang liên tiếp < 16 link.
3. LOGIC TEST CHUẨN (v39): Test từ trang bất kỳ, chỉ chạy 2 trang.
4. LOGIC LOGIN CHUẨN (v32): Cookie Injection từ profile login thủ công.
5. MAIN LOGIC LINH HOẠT: Chạy theo Category lẻ hoặc Group, cho cả Dashboard và Tracker.
"""

import os
import sys
import io
import time
import json
import random
import argparse
import hashlib
import shutil
import datetime
import gc
import re
from typing import List, Dict, Optional, Tuple
import requests as standard_requests 

# --- CÀI ĐẶT ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "log") 
MASTER_PROFILE_DIR = os.path.join(SCRIPT_DIR, "udemy_profile") 

os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

import pandas as pd
from bs4 import BeautifulSoup

# --- MODULES ---
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except ImportError:
    print("LỖI: Chưa cài 'playwright'.")
    sys.exit(1)

try:
    from curl_cffi import requests
    Session = requests.Session
except ImportError:
    print("LỖI: Chưa cài 'curl_cffi'.")
    sys.exit(1)

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# =========================
# SETTINGS
# =========================
PAGELOAD_TIMEOUT = 240  
SCROLL_STEPS = 15      
PAGES_PER_BATCH = 40   
MAX_CONSECUTIVE_LOW_DATA = 2 
ELEMENT_TIMEOUT = 10000 

IMPERSONATE_PROFILES = ["chrome120", "chrome110", "chrome107"]

# =========================
# HELPER FUNCTIONS
# =========================

def get_proxies():
    api_url = os.getenv("PROXY_API_URL")
    if not api_url: return None
    try:
        with standard_requests.get(api_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10) as r: pass
        time.sleep(1)
    except: pass
    try:
        resp = requests.get(api_url, timeout=10, impersonate="chrome120")
        data = resp.json()
        proxy_str = data.get("proxyhttp") or data.get("proxy")
        if not proxy_str: return None
        parts = proxy_str.split(":")
        if len(parts) == 4: auth = f"{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
        elif len(parts) == 2: auth = f"{parts[0]}:{parts[1]}"
        else: return None
        return {"http": f"http://{auth}", "https": f"http://{auth}"}
    except: return None

def _jitter(a=0.5, b=1.5):
    time.sleep(random.uniform(a, b))

def _take_screenshot_playwright(page, job_name):
    try:
        if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"_DEBUG_{job_name}_{timestamp}.png"
        filepath = os.path.join(LOG_DIR, filename)
        page.screenshot(path=filepath, full_page=True)
        print(f"[debug] 📸 Screenshot saved: {filepath}")
    except: pass

# =========================
# AUTHENTICATION BRIDGE
# =========================

def get_auth_cookies_from_profile() -> Dict:
    if not os.path.exists(MASTER_PROFILE_DIR):
        print("[auth] ⚠️ Không tìm thấy profile gốc. Chạy chế độ Guest.")
        return {}

    temp_auth_path = os.path.join(SCRIPT_DIR, "udemy_profile_auth_temp")
    if os.path.exists(temp_auth_path): shutil.rmtree(temp_auth_path, ignore_errors=True)
    try:
        shutil.copytree(MASTER_PROFILE_DIR, temp_auth_path, dirs_exist_ok=True)
        lock = os.path.join(temp_auth_path, "SingletonLock")
        if os.path.exists(lock): os.remove(lock)
    except Exception as e:
        print(f"[auth] ❌ Lỗi copy profile: {e}")
        return {}

    cookies_dict = {}
    playwright = None
    context = None
    
    try:
        print("[auth] 🔐 Đang trích xuất Cookie...")
        playwright = sync_playwright().start()
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=temp_auth_path,
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        cookies = context.cookies("https://www.udemy.com")
        for c in cookies:
            cookies_dict[c['name']] = c['value']
        
        print(f"[auth] ✅ Đã lấy được {len(cookies_dict)} cookies.")
    except Exception as e:
        print(f"[auth] ⚠️ Lỗi lấy cookie: {e}")
    finally:
        try:
            if context: context.close()
            if playwright: playwright.stop()
            if os.path.exists(temp_auth_path): shutil.rmtree(temp_auth_path, ignore_errors=True)
        except: pass
        
    return cookies_dict

# =========================
# GIAI ĐOẠN 1: PLAYWRIGHT
# =========================

def _human_scroll_playwright(page):
    for _ in range(SCROLL_STEPS):
        scroll_amount = random.randint(400, 800)
        page.evaluate(f"window.scrollBy(0, {scroll_amount});")
        time.sleep(random.uniform(0.4, 0.8)) 
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    _jitter()

def _extract_course_links_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = set()
    
    start_node = None
    candidates = soup.find_all(['h2', 'h1', 'div', 'span'])
    for tag in candidates:
        text = tag.get_text(strip=True)
        if text.startswith("All ") and text.endswith(" courses"):
            start_node = tag
            break
            
    source_tags = []
    if start_node:
        source_tags = start_node.find_all_next('a', href=True)
    else:
        source_tags = soup.select('a[href*="/course/"]')

    for a in source_tags:
        href = a.get("href") or ""
        if "/course/" in href:
            if href.startswith("/"): href = "https://www.udemy.com" + href
            href = href.split("?")[0].rstrip("/") + "/"
            if href.startswith("https://www.udemy.com/course/"):
                links.add(href)
                
    return sorted(list(links))

def get_course_urls_per_page_playwright(listing_url: str, headless: bool = False) -> List[str]:
    url_hash = hashlib.md5(listing_url.encode()).hexdigest()[:8]
    profile_path = os.path.join(SCRIPT_DIR, f"udemy_profile_pw_{url_hash}")
    
    if os.path.exists(profile_path):
        lock_file = os.path.join(profile_path, "SingletonLock")
        if os.path.exists(lock_file):
            try: os.remove(lock_file)
            except: pass
    else:
        os.makedirs(profile_path)
    
    playwright_instance = None
    context = None
    final_links = []
    
    try:
        playwright_instance = sync_playwright().start()
        context = playwright_instance.chromium.launch_persistent_context(
            user_data_dir=profile_path,
            headless=headless,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage", "--window-size=1920,1080"],
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        page = context.new_page()
        page.set_default_timeout(PAGELOAD_TIMEOUT * 1000)
        
        for attempt in range(1, 3):
            try:
                print(f"[driver-pw] 🌍 Mở trang (Lần {attempt}): {listing_url}")
                if attempt == 1: page.goto(listing_url, wait_until="domcontentloaded")
                else: page.reload(wait_until="domcontentloaded")
                
                print("[wait] ⏳ Chờ thẻ khóa học...")
                try:
                    page.wait_for_selector('h3 a[href*="/course/"]', state="attached", timeout=ELEMENT_TIMEOUT)
                except: pass

                _jitter()
                _human_scroll_playwright(page)
                try: page.wait_for_load_state("networkidle", timeout=ELEMENT_TIMEOUT)
                except: pass
                
                html = page.content()
                links = _extract_course_links_from_html(html)
                
                final_links = links
                if len(links) >= 5:
                    print(f"[category] ✅ Tìm thấy {len(links)} khóa học.")
                    break
                else:
                    print(f"[category] ⚠️ Tìm thấy {len(links)} link.")
                    if attempt == 2: _take_screenshot_playwright(page, "EMPTY_FINAL")
            except Exception as e:
                print(f"[driver-pw] ❌ Lỗi: {e}")
                time.sleep(3)
    except Exception as e:
        print(f"[FATAL ERROR] ☠️ Playwright Crash: {e}")
    finally:
        try:
            if context: context.close()
            if playwright_instance: playwright_instance.stop()
            if os.path.exists(profile_path): shutil.rmtree(profile_path, ignore_errors=True)
        except: pass
        
    return final_links

# =========================
# GIAI ĐOẠN 2: PARSING (V35/V38 - ULTIMATE)
# =========================

def _clean_price_str(price_str: str) -> Optional[float]:
    if not price_str: return None
    try:
        txt = price_str.replace("Free", "0").replace("₫", "").replace("$", "").strip()
        if "," in txt: txt = txt.replace(",", "") 
        match = re.search(r"(\d+(\.\d+)?)", txt)
        if match: return float(match.group(1))
        return None
    except: return None

def _extract_price_data(soup: BeautifulSoup, data_json: Dict) -> Tuple[Optional[float], Optional[float]]:
    found_prices = []

    # 1. Meta Tag
    try:
        meta_price = soup.find("meta", {"property": "udemy_com:price"})
        if meta_price:
            p = _clean_price_str(meta_price.get("content", ""))
            if p is not None: found_prices.append(p)
    except: pass

    # 2. JSON-LD
    try:
        script_tag = soup.find("script", {"type": "application/ld+json"})
        if script_tag:
            json_ld = json.loads(script_tag.string)
            if isinstance(json_ld, dict): json_ld = [json_ld]
            for item in json_ld:
                if item.get("@type") in ["Course", "Product"]:
                    offers = item.get("offers", [])
                    if isinstance(offers, dict): offers = [offers]
                    for offer in offers:
                        p = float(offer.get("price", 0))
                        if p > 0: found_prices.append(p)
    except: pass

    # 3. Data Module
    try:
        course_data = data_json.get('serverSideProps', {}).get('course', {}) or \
                      data_json.get('componentProps', {}).get('course', {}) or \
                      data_json.get('portal_data', {}).get('course', {})
        
        if 'price_text_data' in course_data:
            ptd = course_data['price_text_data']
            if 'amount' in ptd: found_prices.append(float(ptd['amount']))
            if 'list_price' in ptd and 'amount' in ptd['list_price']: 
                found_prices.append(float(ptd['list_price']['amount']))
        
        if 'discount' in course_data:
            d = course_data['discount']
            if 'list_price' in d and 'amount' in d['list_price']:
                found_prices.append(float(d['list_price']['amount']))
        
        if 'base_price' in course_data:
            bp = course_data['base_price']
            if 'amount' in bp: found_prices.append(float(bp['amount']))
    except: pass

    # 4. HTML Text Fallback
    try:
        price_divs = soup.select('div[data-purpose="course-price-text"] span')
        for div in price_divs:
            text = div.get_text(strip=True)
            if any(c.isdigit() for c in text):
                p = _clean_price_str(text)
                if p is not None and p > 0: found_prices.append(p)
    except: pass

    valid_prices = sorted(list(set([p for p in found_prices if p is not None])))
    
    if not valid_prices: return None, None
    
    if len(valid_prices) == 1:
        return valid_prices[0], valid_prices[0]
    else:
        return max(valid_prices), min(valid_prices)

def parse_course_details(html_content: str) -> Optional[Dict]:
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        body_tag = soup.find('body')
        if not (body_tag and 'data-module-args' in body_tag.attrs): return None
        data = json.loads(body_tag['data-module-args'])
        
        course_data = data.get('serverSideProps', {}).get('course', {}) or \
                      data.get('componentProps', {}).get('course', {}) or \
                      data.get('portal_data', {}).get('course', {})
        if not course_data: return None

        reviews_data = data.get('serverSideProps', {}).get('reviewsRatings', {}) or \
                       data.get('componentProps', {}).get('reviews', {})

        course_id = data.get('course_id') or course_data.get('id')
        title = data.get('title') or course_data.get('title')
        
        instructors_list = []
        if 'instructors' in course_data and 'instructors_info' in course_data['instructors']:
            instructors_list = course_data['instructors']['instructors_info']
        elif 'visible_instructors' in course_data:
            instructors_list = course_data['visible_instructors']
        elif 'instructors' in course_data and isinstance(course_data['instructors'], list):
            instructors_list = course_data['instructors']

        all_instructors = []
        for instructor in instructors_list or []:
            all_instructors.append({
                'instructor_id': instructor.get('id'),
                'name': instructor.get('display_name') or instructor.get('title'),
                'job_title': instructor.get('job_title'),
                'num_students': instructor.get('total_num_students') or instructor.get('num_students'),
                'avg_rating_score': instructor.get('avg_rating_recent') or instructor.get('rating'),
                'num_of_courses': instructor.get('total_num_taught_courses') or instructor.get('num_published_courses'),
                'total_num_reviews': instructor.get('total_num_reviews') or instructor.get('num_reviews')
            })

        original_price, discount_price = _extract_price_data(soup, data)
        
        rating_dist = {}
        try:
            raw_dist = reviews_data.get('ratingDistribution', [])
            rating_dist = json.dumps(raw_dist) 
        except: pass

        return {
            'course_data': {
                'course_id': course_id, 
                'title': title,
                'headline': course_data.get('headline'),
                'language': course_data.get('localeSimpleEnglishTitle'),
                'level': course_data.get('instructionalLevel') or course_data.get('instructional_level_simple'),
                'course_duration_seconds': course_data.get('contentLengthVideo') or course_data.get('content_length_video'),
                'publishes_date': course_data.get('publishedDate') or course_data.get('published_time'),
                'lasted_updated_date': course_data.get('lastUpdateDate') or course_data.get('last_update_date'),
                'original_price': original_price,
                'discount_price': discount_price,
                'num_students': course_data.get('numStudents') or course_data.get('num_students'),
                'num_reviews': course_data.get('numReviews') or course_data.get('num_reviews'),
                'avg_rating_score': course_data.get('rating'),
                'rating_distribution': rating_dist
            },
            'instructors': all_instructors
        }
    except: return None

def parse_course_price_only(html_content: str) -> Optional[Dict]:
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        body_tag = soup.find('body')
        if not body_tag or 'data-module-args' not in body_tag.attrs: return None
        data = json.loads(body_tag['data-module-args'])
        
        course_id = data.get('course_id')
        title = data.get('title')
        original_price, discount_price = _extract_price_data(soup, data)

        return {
            'course_id': course_id, 
            'title': title, 
            'original_price': original_price, 
            'discount_price': discount_price
        }
    except: return None

def run_course_parsing_loop(course_urls: List[str], category_name: str, parser_func, cookies: Dict = None) -> List[Dict]:
    results = []
    impersonate_id = random.choice(IMPERSONATE_PROFILES)
    session = Session(impersonate=impersonate_id) 

    for i, url in enumerate(course_urls, 1):
        success = False
        for attempt in range(3):
            try:
                r = session.get(url, timeout=20)
                parsed = parser_func(r.text)
                if parsed:
                    if 'course_data' in parsed:
                        parsed['course_data']["_url"] = url
                        parsed['course_data']["_category"] = category_name
                        parsed['course_data']["_scraped_datetime"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                    else:
                        parsed["_url"] = url
                        parsed["_category"] = category_name
                        parsed["_scraped_datetime"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                    results.append(parsed)
                success = True
                time.sleep(random.uniform(0.1, 0.3))
                break
            except:
                try: session.close()
                except: pass
                session = Session(impersonate=impersonate_id)
        if not success: print(f"  [SKIP] {url}")
    try: session.close()
    except: pass
    return results

def save_batch_to_azure(batch_data: List[Dict], job_type: str, group_name: str, start_p: int, end_p: int, conn_str: str, container: str, test_mode: bool):
    if not batch_data: return
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    mode = "TEST" if test_mode else "PROD"
    page_range = f"p{start_p}-{end_p}"
    prefix_name = f"{mode}_{job_type}_{group_name}_{page_range}_{timestamp}"
    
    print(f"\n[azure] ☁️ Đang upload: {prefix_name} ({len(batch_data)} khóa)...")
    try:
        courses_list = []
        instructors_list = []
        for row in batch_data:
            if 'course_data' in row:
                c_data = row['course_data']
                c_id = c_data.get('course_id')
                if not c_id: continue
                courses_list.append(c_data)
                insts = row.get('instructors', [])
                for inst in insts:
                    inst['course_id'] = c_id
                    instructors_list.append(inst)
            else:
                courses_list.append(row)

        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service.get_container_client(container)

        if courses_list:
            df = pd.json_normalize(courses_list)
            for col in df.columns:
                if df[col].apply(type).isin([dict, list]).any():
                    df[col] = df[col].apply(json.dumps)
            blob_name = f"{job_type}/{prefix_name}_courses.parquet"
            buf = io.BytesIO()
            df.to_parquet(buf, compression='gzip', index=False)
            container_client.upload_blob(name=blob_name, data=buf.getvalue(), overwrite=True)
            print(f"[azure] ✅ Saved Courses: {blob_name}")

        if instructors_list:
            df = pd.DataFrame(instructors_list)
            blob_name = f"{job_type}/{prefix_name}_instructors.parquet"
            buf = io.BytesIO()
            df.to_parquet(buf, compression='gzip', index=False)
            container_client.upload_blob(name=blob_name, data=buf.getvalue(), overwrite=True)
            print(f"[azure] ✅ Saved Instructors: {blob_name}")
    except Exception as e:
        print(f"[azure] ❌ Upload Failed: {e}")

CATEGORIES_FULL = {
    "Web Development": "https://www.udemy.com/courses/development/web-development/",
    "Data Science": "https://www.udemy.com/courses/development/data-science/",
    "Mobile Development": "https://www.udemy.com/courses/development/mobile-apps/",
    "Programming Languages": "https://www.udemy.com/courses/development/programming-languages/",
    "Game Development": "https://www.udemy.com/courses/development/game-development/",
    "Database Design & Development": "https://www.udemy.com/courses/development/databases/",
    "Software Testing": "https://www.udemy.com/courses/development/software-testing/",
    "Software Engineering": "https://www.udemy.com/courses/development/software-engineering/",
    "Software Development Tools": "https://www.udemy.com/courses/development/development-tools/",
    "No-Code Development": "https://www.udemy.com/courses/development/no-code-development/"
}

CATEGORY_GROUPS = {
    "group1": ["Web Development", "Software Engineering"],
    "group2": ["Programming Languages", "Database Design & Development", "Software Testing", "No-Code Development"],
    "group3": ["Data Science", "Mobile Development", "Game Development", "Software Development Tools"], 
}

def run_job_with_page_batching(job_type: str, group_name: str, categories: Dict, headless: bool, azure_conn: str, azure_cont: str, test_mode: bool, start_page_override: int = 1):
    print(f"--- BẮT ĐẦU JOB: {job_type} (Group: {group_name} | Start: {start_page_override}) ---")
    masked_key = azure_conn[:15] + "..." + azure_conn[-10:] if azure_conn else "NONE"
    print(f"[debug] Connection: {masked_key} | Container: {azure_cont}")
    
    auth_cookies = get_auth_cookies_from_profile()
    if auth_cookies: print(f"[system] ✅ Auth Cookies Loaded ({len(auth_cookies)} items).")
    else: print(f"[system] ⚠️ Running as GUEST (No Cookies).")

    parser_func = parse_course_details if job_type == 'dashboard' else parse_course_price_only
    max_total_pages = 9999
    
    for cat_name, url in categories.items():
        print(f"\n>>> CATEGORY: {cat_name}")
        current_page = start_page_override
        low_data_streak = 0
        while current_page <= max_total_pages:
            start_p = current_page
            end_p = current_page + PAGES_PER_BATCH
            
            if test_mode and start_p > start_page_override + 2: 
                print("🛑 TEST MODE: Dừng sau 2 trang.")
                break

            print(f"\n[phase-1] 🕸️ Gom link trang {start_p} -> {end_p - 1}...")
            batch_urls = []
            actual_last_page = start_p
            for p in range(start_p, end_p):
                actual_last_page = p
                if test_mode and p >= start_page_override + 2: 
                    print(f"🛑 TEST MODE: Đã đủ 2 trang.")
                    break
                links = get_course_urls_per_page_playwright(f"{url}?p={p}", headless)
                if len(links) < 16:
                    low_data_streak += 1
                    print(f"⚠️ Trang {p} thiếu dữ liệu: {len(links)} link. (Streak: {low_data_streak}/{MAX_CONSECUTIVE_LOW_DATA})")
                    if links:
                        new_links = [l for l in links if l not in batch_urls]
                        batch_urls.extend(new_links)
                    if low_data_streak >= MAX_CONSECUTIVE_LOW_DATA:
                        print(f"🛑 Dừng Category '{cat_name}'.")
                        current_page = 999999 
                        break
                else:
                    low_data_streak = 0
                    new_links = [l for l in links if l not in batch_urls]
                    batch_urls.extend(new_links)
                    print(f"-> Page {p}: +{len(new_links)} links")
            
            unique_batch = sorted(list(set(batch_urls)))
            if len(unique_batch) > 0:
                print(f"\n[phase-2] 🚀 Parse & Save {len(unique_batch)} khóa học...")
                full_results = []
                chunk_size = 50
                for i in range(0, len(unique_batch), chunk_size):
                    chunk = unique_batch[i : i + chunk_size]
                    res = run_course_parsing_loop(chunk, cat_name, parser_func, cookies=auth_cookies)
                    full_results.extend(res)
                    print(f"  -> Progress: {min(i+chunk_size, len(unique_batch))}/{len(unique_batch)}")
                save_batch_to_azure(full_results, job_type, group_name, start_p, actual_last_page, azure_conn, azure_cont, test_mode)
                del batch_urls, unique_batch, full_results
                gc.collect()
                print("[system] 🧹 RAM Cleaned.")
            if current_page != 999999: current_page = end_p

    print("\n--- [HOÀN TẤT JOB] ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True, choices=['dashboard', 'tracker'])
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--group", choices=["group1", "group2", "group3"])
    parser.add_argument("--category", type=str, help="Chạy riêng lẻ 1 category cụ thể")
    parser.add_argument("--start-page", type=int, default=1)
    args = parser.parse_args()
    
    load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
    raw_conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    AZURE_CONN = raw_conn.strip().replace('"', '').replace("'", "").replace("\r", "")
    AZURE_CONT = "udemy-it"
    IS_HEADLESS = os.environ.get("IS_HEADLESS", "false").lower() == "true"
    
    cats_to_run = {}
    group_name_run = "custom"
    
    if args.category:
        if args.category in CATEGORIES_FULL:
            cats_to_run = {args.category: CATEGORIES_FULL[args.category]}
            group_name_run = f"cat_{args.category.replace(' ', '_')}"
        else:
            print(f"❌ Error: Category '{args.category}' not found!")
            sys.exit(1)
    elif args.job == 'dashboard':
        if args.group:
            group_name_run = args.group
            group_cats = CATEGORY_GROUPS.get(args.group, [])
            cats_to_run = {k:v for k,v in CATEGORIES_FULL.items() if k in group_cats}
        else:
            group_name_run = "all"
            cats_to_run = CATEGORIES_FULL
    else:
        # [FLEXIBLE TRACKER LOGIC]
        # Bây giờ Tracker cũng có thể chạy theo Group hoặc Category
        if args.group:
             group_name_run = f"tracker_{args.group}"
             group_cats = CATEGORY_GROUPS.get(args.group, [])
             cats_to_run = {k:v for k,v in CATEGORIES_FULL.items() if k in group_cats}
        else:
             # Mặc định cũ (nếu không có tham số gì)
             group_name_run = "tracker_nocode"
             cats_to_run = {"No-Code Development": CATEGORIES_FULL["No-Code Development"]}
        
    run_job_with_page_batching(args.job, group_name_run, cats_to_run, IS_HEADLESS, AZURE_CONN, AZURE_CONT, args.test, args.start_page)