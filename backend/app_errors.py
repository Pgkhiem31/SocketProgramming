"""Các loại lỗi nghiệp vụ của LAN Chat.

Mỗi loại lỗi tự định nghĩa mã và thông báo mặc định. Module gọi lỗi chỉ cần
raise đúng class, không phải tự tạo mã bằng chuỗi.
"""
from __future__ import annotations


class LanError(Exception):
    """Lớp cha của lỗi ứng dụng; mã lỗi do lớp con định nghĩa."""

    code = "lan_error"
    default_message = "Đã xảy ra lỗi trong LAN Chat"

    def __init__(self, message: str | None = None):
        self.message = message or self.default_message
        super().__init__(self.message)


class InvalidMessageError(LanError):
    code = "message_invalid"
    default_message = "Nội dung tin nhắn không hợp lệ"


class EmptyMessageError(InvalidMessageError):
    code = "message_empty"
    default_message = "Tin nhắn không được để trống"


class MessageTooLongError(InvalidMessageError):
    code = "message_limit"
    default_message = "Tin nhắn vượt quá giới hạn độ dài"


class MessageEncodingError(InvalidMessageError):
    code = "message_encoding"
    default_message = "Tin nhắn không thể mã hóa thành UTF-8"


class PeerOfflineError(LanError):
    code = "peer_offline"
    default_message = "Peer đang ngoại tuyến hoặc chưa kết nối"


class ServiceNotStartedError(LanError):
    code = "service_not_started"
    default_message = "Hãy khởi động service trước"


class ServiceClosedError(LanError):
    code = "service_closed"
    default_message = "Service đã đóng; hãy tạo service mới"


class ListenerStartError(LanError):
    code = "listener_start_failed"
    default_message = "Không thể mở TCP listener; hãy kiểm tra IP và cổng"


class InvalidNameError(LanError):
    code = "invalid_name"
    default_message = "Tên người dùng không hợp lệ"


class SelfConnectionError(LanError):
    code = "self_connection"
    default_message = "Không thể kết nối tới chính mình"


class InvalidPeerError(LanError):
    code = "invalid_peer"
    default_message = "Thông tin peer không hợp lệ"


class ConnectionFailedError(LanError):
    code = "connection_failed"
    default_message = "Không thể kết nối tới peer"


class StorageError(LanError):
    code = "storage_error"
    default_message = "Không thể đọc hoặc ghi cơ sở dữ liệu"


class WorkerStartError(LanError):
    code = "worker_error"
    default_message = "Không khởi động được luồng gửi tin"


class AckTimeoutError(LanError):
    code = "ack_timeout"
    default_message = "Không nhận được xác nhận từ peer"


class InternalError(LanError):
    code = "internal_error"
    default_message = "Đã xảy ra lỗi nội bộ"


# Chuyển các mã tầng giao thức thành loại lỗi ứng dụng tương ứng.
_MESSAGE_ERRORS: dict[str, type[InvalidMessageError]] = {
    "message_empty": EmptyMessageError,
    "message_limit": MessageTooLongError,
    "message_invalid": MessageEncodingError,
}


def message_error_from_protocol(code: str) -> InvalidMessageError:
    """Đổi lỗi kiểm tra nội dung từ wire_protocol thành lỗi có kiểu cụ thể."""
    error_type = _MESSAGE_ERRORS.get(code, InvalidMessageError)
    return error_type()
