# -*- coding: utf-8 -*-
"""
LOADER SCRIPT: Azure Blob -> PostgreSQL Data Warehouse
Tính năng:
1. Incremental Load: Chỉ nạp file mới chưa từng nạp.
2. Auto Schema: Tự động tạo bảng và schema 'raw' nếu chưa có.
3. Robust: Tự động xử lý tên cột, kiểu dữ liệu để tránh lỗi SQL.
"""

import os
import io
import json
import pandas as pd
from sqlalchemy import create_engine, text
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# --- CẤU HÌNH ---
load_dotenv()

# 1. Azure Config
AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
# Nếu chuỗi kết nối bị lỗi dính ngoặc kép, làm sạch nó
if AZURE_CONN_STR:
    AZURE_CONN_STR = AZURE_CONN_STR.strip().replace('"', '').replace("'", "")

CONTAINER = "udemy-it"

# 2. Postgres DW Config (Kết nối từ trong Docker Network)
# Host là 'postgres_dw' (tên service trong docker-compose)
# Port là '5432' (cổng nội bộ container)
DB_USER = "user_dw"
DB_PASS = "password_dw"
DB_HOST = "postgres_dw"
DB_PORT = "5432"
DB_NAME = "udemy_dw"

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_db_engine():
    return create_engine(DB_URL)

def init_infrastructure(engine):
    """Khởi tạo Schema và Bảng Log nếu chưa có"""
    # SỬA LỖI: Dùng engine.begin() để tự động commit
    with engine.begin() as conn:
        # Tạo Schema raw
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        
        # Tạo bảng ghi chép lịch sử nạp (Sổ Nam Tào)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.loaded_files_log (
                filename TEXT PRIMARY KEY,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_count INT,
                status TEXT
            );
        """))
    print("[setup] ✅ Đã kiểm tra hạ tầng DB (Schema + Log Table).")

def get_loaded_files(engine):
    """Lấy danh sách file đã nạp thành công"""
    try:
        df = pd.read_sql("SELECT filename FROM raw.loaded_files_log WHERE status = 'SUCCESS'", engine)
        return set(df['filename'].tolist())
    except Exception:
        return set()

def log_file_status(engine, filename, status, row_count=0):
    """Ghi lại trạng thái nạp file"""
    # SỬA LỖI: Dùng engine.begin() để tự động commit
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO raw.loaded_files_log (filename, status, row_count, loaded_at)
                VALUES (:fn, :st, :rc, NOW())
                ON CONFLICT (filename) DO UPDATE 
                SET status = :st, row_count = :rc, loaded_at = NOW();
            """),
            {"fn": filename, "st": status, "rc": row_count}
        )

def determine_target_table(filename):
    """Quy tắc ánh xạ tên file -> tên bảng"""
    if "tracker" in filename and "courses" in filename:
        return "price_tracker"
    elif "dashboard" in filename and "instructors" in filename:
        return "instructors"
    elif "dashboard" in filename and "courses" in filename:
        return "courses"
    return None

def clean_dataframe(df):
    """Làm sạch dữ liệu trước khi nạp"""
    # 1. Chuẩn hóa tên cột: chữ thường, bỏ khoảng trắng
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
    
    # 2. Xử lý cột JSON (rating_distribution) nếu có
    # Pandas đọc parquet có thể tự convert thành object/dict, cần ép về string để lưu vào Text DB
    if 'rating_distribution' in df.columns:
        df['rating_distribution'] = df['rating_distribution'].astype(str)
        
    return df

def main():
    print(f"--- BẮT ĐẦU JOB LOAD DỮ LIỆU ---")
    
    # 1. Kết nối
    try:
        blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
        container_client = blob_service.get_container_client(CONTAINER)
        engine = get_db_engine()
        init_infrastructure(engine)
    except Exception as e:
        print(f"❌ Lỗi kết nối ban đầu: {e}")
        return

    # 2. Lấy danh sách file
    try:
        all_blobs = list(container_client.list_blobs())
        parquet_files = [b for b in all_blobs if b.name.endswith(".parquet")]
    except Exception as e:
        print(f"❌ Lỗi liệt kê file trên Azure: {e}")
        return

    # 3. Lọc file mới
    loaded_files = get_loaded_files(engine)
    new_files = [b for b in parquet_files if b.name not in loaded_files]

    print(f"📦 Tổng file trên Azure: {len(parquet_files)}")
    print(f"📚 File đã nạp trước đó: {len(loaded_files)}")
    
    if not new_files:
        print("✅ Hệ thống đã cập nhật (Up-to-date). Không có file mới.")
        return

    print(f"🚀 Tìm thấy {len(new_files)} file MỚI. Bắt đầu nạp...")

    # 4. Vòng lặp nạp
    for blob in new_files:
        table_name = determine_target_table(blob.name)
        if not table_name:
            print(f"⚠️ [Skip] File không đúng định dạng: {blob.name}")
            continue

        print(f"⬇️ [Processing] {blob.name} -> raw.{table_name}")
        
        try:
            # Tải file về RAM
            blob_client = container_client.get_blob_client(blob)
            stream = blob_client.download_blob()
            file_content = stream.readall()
            
            # Đọc Parquet
            df = pd.read_parquet(io.BytesIO(file_content))
            
            # Làm sạch
            df = clean_dataframe(df)
            
            # Nạp vào DB
            df.to_sql(
                name=table_name,
                con=engine,
                schema="raw",
                if_exists="append", # Nối đuôi, không xóa dữ liệu cũ
                index=False,
                method='multi',     # Tăng tốc độ insert
                chunksize=1000      # Gửi mỗi lần 1000 dòng
            )
            
            # Ghi log thành công
            log_file_status(engine, blob.name, 'SUCCESS', len(df))
            print(f"   ✅ Thành công: Đã thêm {len(df)} dòng.")
            
        except Exception as e:
            print(f"   ❌ Lỗi xử lý file: {e}")
            log_file_status(engine, blob.name, 'FAILED', 0)

    print("\n🏁 HOÀN TẤT QUÁ TRÌNH NẠP.")

if __name__ == "__main__":
    main()