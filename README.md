# 🚀 Hệ thống Data Pipeline & Giải pháp Theo dõi Giá Khóa học Udemy

[![DUE](https://img.shields.io/badge/University-DUE%20Danang-orange?style=for-the-badge)](https://due.udn.vn/)
[![Internship](https://img.shields.io/badge/Intern-XinKgroup-green?style=for-the-badge)](https://www.facebook.com/xinkgroup)

## 📝 Giới thiệu dự án
Dự án **Capstone 2** được thực hiện bởi nhóm sinh viên ngành **Khoa học dữ liệu và Phân tích kinh doanh**, Trường Đại học Kinh tế – Đại học Đà Nẵng. Chúng tôi xây dựng một hệ thống dữ liệu khép kín (**End-to-End**) nhằm tự động hóa việc thu thập, xử lý và theo dõi biến động giá các khóa học IT trên nền tảng Udemy.

* **Mục tiêu:** Xây dựng pipeline tự động vận hành theo lịch, tổ chức kho dữ liệu logic và cung cấp công cụ hỗ trợ người dùng "săn" khóa học giá tốt.
* **Đơn vị thực tập:** Công ty phần mềm **XinKgroup**.
* **Giảng viên hướng dẫn:** ThS. Trần Văn Lộc .

---

## 🏗 Kiến trúc hệ thống (System Architecture)
Hệ thống áp dụng mô hình **ELT (Extract – Load – Transform)** kết hợp với kiến trúc **Medallion** (Bronze - Silver - Gold) để quản lý vòng đời dữ liệu. Toàn bộ hạ tầng được đóng gói bằng **Docker** và kết nối an toàn qua **Tailscale VPN**.

| Lớp thành phần | Công nghệ sử dụng | Chức năng chính |
| :--- | :--- | :--- |
| **Extraction** | Python, Playwright, curl_cffi | Thu thập dữ liệu web động và giả lập trình duyệt. |
| **Data Lake** | Azure Blob Storage | Lưu trữ dữ liệu thô dưới định dạng Parquet tối ưu.|
| **Warehouse** | PostgreSQL | Tổ chức dữ liệu theo mô hình Snowflake/Star Schema .|
| **Transform** | dbt (Data Build Tool) | Làm sạch và biến đổi dữ liệu theo tư duy "Data as code".|
| **Orchestration**| Apache Airflow | Điều phối quy trình dưới dạng đồ thị có hướng (DAG) .|
| **Analytics** | Power BI | Xây dựng Dashboard phân tích thị trường tổng quan.|
| **Web App** | Streamlit / FastAPI | Công cụ Price Tracker theo dõi và cảnh báo giá. |

---

## ⚡ Các tính năng nổi bật

### 1. Data Pipeline Tự động hóa
* Quy trình được điều phối bởi **Apache Airflow**, tự động chạy vào 9h sáng hàng ngày.
* Cơ chế **Incremental Load** (nạp tăng dần) giúp tối ưu hiệu suất xử lý dữ liệu mới từ Azure Blob vào PostgreSQL.

### 2. Dashboard Phân tích (Power BI)
* **Market Overview:** Trực quan hóa KPI về doanh thu, lượt ghi danh, phân bổ cấp độ và thời lượng khóa học.
* **Best Courses:** Bộ lọc thông minh hỗ trợ xếp hạng khóa học tiềm năng dựa trên rating và hiệu suất.

### 3. Công cụ Price Tracker (Web App)
* **Theo dõi lịch sử giá:** Hiển thị biểu đồ biến động giá (Time-series) để nhận diện chu kỳ khuyến mãi.
* **Cảnh báo Email:** Tự động gửi thông báo khi khóa học đang theo dõi giảm sâu hoặc chạm mức giá thấp nhất lịch sử.

---

## 👨‍💻 Thành viên thực hiện
* **Đặng Duy Cường** - Sinh viên Khoa học dữ liệu, DUE.
* **Phan Trường Huy** - Sinh viên Khoa học dữ liệu, DUE.
  Cám ơn Huy đã đồng hành cùng tôi 
---
