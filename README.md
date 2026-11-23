# SocketProgramming

Skeleton của project, đồ án chat và truyền tệp qua TCP/UDP trong mạng LAN.

| Thư mục | Nội dung dự kiến |
| --- | --- |
| `backend/` | UDP discovery, kết nối TCP, giao thức, chat, truyền tệp và lưu dữ liệu |
| `frontend/` | Cửa sổ và thành phần PySide6 |
| `downloads/` | Thư mục nhận tệp |


| file |  |
|---|---|
| `lan_service.py` | nối giao diện. Khởi động mạng và cung cấp các hàm `connect()`, `send_message()`, `send_file()` và `leave()`. |
| `app_config.py` | chọn card mạng IPv4, tính địa chỉ broadcast và đặt các giá trị mặc định  TCP `5000` UDP `9999`. |
| `udp_discovery.py` | Tìm thiết bị cùng LAN Phát thông báo hiện diện mỗi 3 giây và loại thiết bị khỏi danh sách hoạt động sau 10 giây không nhận được thông báo |
| `tcp_sessions.py` | Mở, nhận và đóng các kết nối TCP.  người dùng bấm Ngắt kết nối, nó đóng phiên chat và chờ người dùng kết nối lại |
| `wire_protocol.py` | Quy định cách hai máy đóng gói và đọc dữ liệu |
| `chat_messages.py` | Gửi, nhận và cập nhật trạng thái tin nhắn. Máy nhận lưu tin vào SQLite rồi gửi `chat_ack` để xác nhận. |
| `file_transfers.py` | Truyền tệp theo khối trên kết nối TCP riêng, cập nhật tiến độ, kiểm tra SHA-256 và dọn tệp tải dở khi gặp lỗi. |
| `sqlite_store.py` | Lưu hồ sơ, lịch sử chat và lượt truyền tệp. Dùng mốc đã đọc của từng cuộc trò chuyện để tính số tin chưa đọc. |
| `app_errors.py` | Định nghĩa `LanError`, chứa mã lỗi như `peer_offline` để giao diện hiển thị thông báo phù hợp. |
| `__init__.py` | Đánh dấu `backend` là một gói Python để các phần khác có thể import. |

