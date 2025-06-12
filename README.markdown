# Hướng dẫn cài đặt Apache Spark 3.4.4 trên WSL (Ubuntu 22.04 LTS)

Hướng dẫn này cung cấp các bước chi tiết để cài đặt và thiết lập môi trường Apache Spark phiên bản 3.4.4 trên Windows Subsystem for Linux (WSL) với Ubuntu 22.04 LTS, sử dụng OpenJDK 11 và Python 3.10+.

## Yêu cầu
- Hệ điều hành: Windows 10/11 với WSL đã kích hoạt.
- Ubuntu 22.04 LTS được cài đặt trên WSL.
- Kết nối internet để tải các gói phần mềm.

## Các bước cài đặt

### 1. Cài đặt WSL (nếu chưa có)
1. Mở Command Prompt với quyền Administrator và chạy lệnh:
   ```
   wsl --install
   ```
2. Sau khi cài đặt, mở terminal Ubuntu 22.04 và cập nhật hệ thống:
   ```
   sudo apt update && sudo apt upgrade -y
   ```
3. Cài đặt các gói cần thiết:
   ```
   sudo apt install python3 python3-pip -y
   sudo apt install openjdk-11-jdk -y
   ```

### 2. Tải và giải nén Apache Spark
1. Tải Apache Spark 3.4.4 (đã biên dịch với Hadoop 3) từ trang chính thức:
   ```
   wget https://archive.apache.org/dist/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz
   ```
2. Giải nén tệp:
   ```
   tar -xvzf spark-3.4.4-bin-hadoop3.tgz
   ```
3. Đổi tên thư mục để dễ quản lý:
   ```
   mv spark-3.4.4-bin-hadoop3 sparks
   ```

### 3. Cấu hình biến môi trường
1. Mở tệp cấu hình shell:
   ```
   nano ~/.bashrc
   ```
2. Thêm các dòng sau vào cuối tệp:
   ```
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
   export SPARK_HOME=/mnt/c/Users/Admin/sparks
   export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
   export PATH=$PATH:/mnt/c/Users/Admin/.local/bin
   ```
3. Lưu và thoát (Ctrl+O, Enter, Ctrl+X).
4. Tạo môi trường ảo Python:
   ```
   sudo apt install python3-venv python3-pip
   python3 -m venv ~/pyspark_env
   source ~/pyspark_env/bin/activate
   ```
5. Tải lại tệp cấu hình:
   ```
   source ~/.bashrc
   ```

### 4. Cài đặt PySpark và thư viện hỗ trợ
1. Cài đặt PySpark tương ứng với Spark 3.4.4:
   ```
   pip3 install pyspark==3.4.4
   ```
2. Cài đặt thư viện `cloudpickle` để hỗ trợ serialization:
   ```
   pip3 install cloudpickle==1.6.0
   ```

## Kiểm tra cài đặt
- Chạy lệnh sau để kiểm tra Spark:
  ```
  spark-shell
  ```
- Nếu không có lỗi, Spark đã được cài đặt thành công.

## Lưu ý
- Đảm bảo đường dẫn `SPARK_HOME` và `JAVA_HOME` khớp với cấu trúc thư mục trên hệ thống của bạn.
- Môi trường ảo (`pyspark_env`) giúp tránh xung đột gói Python.