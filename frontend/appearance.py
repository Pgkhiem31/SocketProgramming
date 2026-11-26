from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QFont, QIcon, QPainter, QPixmap
from PySide6.QtWidgets import QApplication, QLabel

THEMES = Path(__file__).with_name("styles")
AVATARS = {"teal": "#16796d", "blue": "#315eaa", "amber": "#946222", "rose": "#a44867"}
PRESENCE = {"online": "Trực tuyến", "away": "Vắng mặt", "offline": "Ngoại tuyến"}
CONNECTION = {
    "connecting": "Đang kết nối", "connected": "Đã kết nối TCP",
    "disconnected": "Chưa kết nối TCP", "paused": "Đã ngắt kết nối", "error": "Kết nối thất bại",
}
TRANSFER = {
    "waiting": "Đang chuẩn bị", "transferring": "Đang truyền", "verifying": "Đang xác minh",
    "completed": "Hoàn tất", "failed": "Thất bại",
}
ERRORS = {
    "profile_required": "Hãy đặt tên thiết bị trước khi bật mạng.",
    "interface_unavailable": "Không tìm thấy card IPv4 đã chọn. Kiểm tra kết nối mạng hoặc chọn card khác.",
    "network_unavailable": "Kết nối mạng chưa sẵn sàng.",
    "network_failed": "Không thể khởi động các socket. Xem nhật ký chạy để biết lỗi cụ thể.",
    "address_in_use": "Cổng đang được sử dụng hoặc bị chặn. Chọn cổng khác trong Cài đặt.",
    "discovery_failed": "Không gửi được UDP broadcast. Kiểm tra card mạng và tường lửa.",
    "peer_offline": "Thiết bị không còn trực tuyến. Hãy thử lại khi thiết bị xuất hiện.",
    "connection_failed": "Không kết nối được thiết bị. Kiểm tra kết nối mạng hoặc tường lửa.",
    "chat_paused": "Bấm Kết nối trước khi tiếp tục trò chuyện.",
    "message_empty": "Hãy nhập nội dung tin nhắn.",
    "message_invalid": "Tin nhắn chứa ký tự không thể mã hóa.",
    "message_limit": "Tin nhắn vượt giới hạn 16 KiB.",
    "message_busy": "Tin nhắn này đang được xử lý.",
    "message_conflict": "Mã tin nhắn đã được dùng cho nội dung khác.",
    "ack_timeout": "Thiết bị bên kia chưa xác nhận tin nhắn.",
    "file_missing": "Không tìm thấy tệp đã chọn.",
    "file_unreadable": "Không đọc được tệp. Kiểm tra tệp và quyền truy cập.",
    "file_limit": "Tệp vượt giới hạn dung lượng của LANLink.",
    "invalid_filename": "Tên tệp không hợp lệ.",
    "integrity": "Kiểm tra kích thước hoặc SHA-256 của tệp thất bại.",
    "storage": "Không lưu được tệp. Kiểm tra thư mục và dung lượng đĩa.",
    "interrupted": "Lượt truyền hoặc tác vụ bị gián đoạn.",
    "busy": "Ứng dụng đang xử lý quá nhiều tác vụ. Hãy thử lại sau.",
    "unexpected": "Có lỗi khi xử lý thao tác. Xem nhật ký chạy để biết chi tiết.",
}


def theme(name: str) -> None:
    app = QApplication.instance()
    # Áp dụng QSS cho toàn bộ ứng dụng Qt khi người dùng đổi chủ đề.
    stylesheet = THEMES / ("light.qss" if name == "light" else "dark.qss")
    app.setStyleSheet(stylesheet.read_text(encoding="utf-8"))


def label(text: str = "", style: str = "", wrap: bool = False) -> QLabel:
    widget = QLabel(text)
    # Hiển thị nội dung dưới dạng văn bản thuần, kể cả khi có ký tự giống thẻ HTML.
    widget.setTextFormat(Qt.TextFormat.PlainText)
    widget.setWordWrap(wrap)
    if style:
        widget.setObjectName(style)
    return widget


def avatar(name: str, color: str = "teal", size: int = 32, presence: str = "") -> QPixmap:
    pixmap = QPixmap(size, size)
    pixmap.fill(Qt.GlobalColor.transparent)
    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing)
    painter.setPen(Qt.PenStyle.NoPen)
    painter.setBrush(QColor(AVATARS.get(color, AVATARS["teal"])))
    painter.drawEllipse(0, 0, size, size)
    painter.setPen(QColor("#FFFFFF"))
    font = QFont("Segoe UI")
    font.setBold(True)
    font.setPixelSize(max(10, size // 3))
    painter.setFont(font)
    # Dùng chữ đầu của tối đa hai từ làm ảnh đại diện.
    initials = "".join(word[0] for word in name.split()[:2]).upper() or "L"
    painter.drawText(pixmap.rect(), Qt.AlignmentFlag.AlignCenter, initials)
    # Vẽ thêm chấm màu ở góc để biểu thị trạng thái hiện diện.
    if presence:
        painter.setPen(QColor("#101828"))
        painter.setBrush(QColor({"online": "#4ade80", "away": "#fbbf24"}.get(presence, "#94a3b8")))
        painter.drawEllipse(size - 10, size - 10, 9, 9)
    painter.end()
    return pixmap


def icon(name: str, color: str, presence: str) -> QIcon:
    return QIcon(avatar(name, color, 32, presence))


def error_text(code: str | None) -> str:
    return ERRORS.get(code or "", ERRORS["unexpected"])


def size_text(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    # Đổi đơn vị theo bội số 1024, cùng quy ước KiB và MiB của phần truyền file.
    for unit in ("KiB", "MiB", "GiB"):
        size /= 1024
        if size < 1024 or unit == "GiB":
            return f"{size:.1f} {unit}"
    raise AssertionError("Size unit is missing")

