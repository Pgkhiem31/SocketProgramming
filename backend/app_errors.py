class LanError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        # Giữ mã lỗi riêng để giao diện tra thông báo tiếng Việt.
        self.code = code
