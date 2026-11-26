# LANLink Desktop

Đồ án lập trình socket cho phép các máy trong cùng mạng LAN tìm thấy nhau qua UDP, nhắn tin và chuyển tệp qua TCP.

## Cài đặt và chạy trên Windows

Dùng Windows và CPython 3.12 bản 64 bit. Cần Internet để tải thư viện trong lần cài đặt đầu tiên. [Tải Python cho Windows](https://www.python.org/downloads/windows/)

Nếu nhận mã nguồn dưới dạng ZIP, hãy giải nén vào thư mục có quyền ghi. Mở thư mục dự án chứa `main.py` và `requirements.txt`, rồi mở PowerShell ngay tại đó. 

~~~powershell
py -3.12 --version
py -3.12 -m venv .venv
& '.\.venv\Scripts\python.exe' -m pip install -r requirements.txt
~~~

Nếu lệnh `py -3.12 --version` không chạy được, thử `python --version`. Nếu kết quả là Python 3.12, dùng `python -m venv .venv` để tạo môi trường ảo.

Mở ứng dụng. Những lần sau chỉ cần chạy lại lệnh này từ thư mục dự án. Có thể gọi trực tiếp Python trong `.venv` mà không cần kích hoạt môi trường ảo.

~~~powershell
& '.\.venv\Scripts\python.exe' '.\main.py'
~~~

Ở lần chạy đầu tiên, nhập tên hiển thị rồi nhấn **Bắt đầu**. Có thể giữ các giá trị mặc định là **TCP 5000**, **UDP 9999** và thư mục nhận tệp `downloads/`.

## Demo nhắn tin và chuyển tệp trên một máy

Cách này phù hợp để chấm hoặc trình bày khi chỉ có một máy. Sau khi cài đặt, mở hai cửa sổ PowerShell tại cùng thư mục dự án và chạy một lệnh trong mỗi cửa sổ. Hai phiên dùng các cổng, thư mục dữ liệu và thư mục nhận tệp riêng.

**PowerShell 1**

~~~powershell
& '.\.venv\Scripts\python.exe' '.\main.py' --name (Nhập tên) --interface 127.0.0.1 --tcp-port 5001 --udp-port 9991 --discovery-target 127.0.0.1:9992 --data-dir '.\.lanlink\demo-an' --downloads '.\downloads\an'
~~~

**PowerShell 2**

~~~powershell
& '.\.venv\Scripts\python.exe' '.\main.py' --name (Nhập tên) --interface 127.0.0.1 --tcp-port 5002 --udp-port 9992 --discovery-target 127.0.0.1:9991 --data-dir '.\.lanlink\demo-binh' --downloads '.\downloads\binh'
~~~

1. Trong cửa sổ **An**, chờ **Binh** xuất hiện ở danh sách **Thiết bị trong LAN**, rồi chọn **Binh**.
2. Nhập một tin nhắn và nhấn Enter. Kiểm tra tin xuất hiện ở cửa sổ **Binh** và phía **An** hiển thị **Đã xác nhận**.
3. Ở phía **An**, nhấn Ctrl+O và chọn một tệp nhỏ. Ở phía **Binh**, chọn cuộc trò chuyện với **An**, mở **Chi tiết** và kiểm tra lượt truyền hiển thị **Hoàn tất**. Nhấn **Mở tệp đã nhận** hoặc tìm tệp trong `downloads\binh`.

## Chạy trên hai máy trong cùng mạng LAN

1. Cài và mở ứng dụng trên cả hai máy bằng lệnh chạy thông thường ở trên. Đặt tên khác nhau để dễ nhận diện.
2. Kết nối cả hai máy vào cùng mạng nội bộ có IPv4. Giữ cổng UDP khám phá giống nhau trên hai máy, mặc định là **9999**. Mỗi máy có thể giữ cổng TCP mặc định **5000**.
3. Nếu Windows Firewall hỏi quyền truy cập mạng, cho phép Python trên **mạng riêng tư (Private)** mà hai máy đang dùng. Khi tên máy kia xuất hiện trong **Thiết bị trong LAN**, thực hiện lại các bước gửi tin và tệp ở phần demo.

## Xử lý lỗi thường gặp

| Hiện tượng | Cách kiểm tra |
| --- | --- |
| PowerShell báo không tìm thấy `main.py` hoặc `.venv\Scripts\python.exe` | Kiểm tra đang đứng trong thư mục chứa `main.py`. Nếu thiếu `.venv`, làm lại bước cài đặt. |
| `ModuleNotFoundError: No module named 'PySide6'` | Chạy lại `& '.\.venv\Scripts\python.exe' -m pip install -r requirements.txt`, rồi dùng đúng lệnh mở ứng dụng ở trên. |
| Ứng dụng báo cổng đang được sử dụng | Đóng phiên LANLink khác đang dùng cổng đó, hoặc chọn cổng còn trống. Với demo một máy, nếu đổi cổng UDP của một phiên thì sửa cả `--discovery-target` của phiên còn lại. |
| Hai máy không thấy nhau | Kiểm tra cùng mạng IPv4, cùng cổng UDP và quyền qua tường lửa. Nếu máy có nhiều card mạng, chọn đúng địa chỉ IPv4 trong **Cài đặt** rồi khởi động lại ứng dụng. Một số mạng khách chặn kết nối giữa các máy, khi đó có thể dùng demo một máy. |

## Kiểm thử giao thức

Từ thư mục dự án, sau khi đã cài thư viện, chạy bộ kiểm thử giao thức đi kèm mã nguồn.

~~~powershell
& '.\.venv\Scripts\python.exe' -m unittest discover -s tests -p test_wire_protocol.py -v
~~~
