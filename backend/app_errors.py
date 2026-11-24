"""Mã lỗi chung để backend và giao diện trao đổi thống nhất."""
from __future__ import annotations


class LanError(Exception):
    # code dùng cho xử lý lỗi, message dùng để hiển thị.
    def __init__(self, code: str, message: str | None = None):
        self.code = code
        self.message = message if message is not None else code
        super().__init__(self.message)

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message}
