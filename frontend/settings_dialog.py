from pathlib import Path

from PySide6.QtWidgets import (
    QCheckBox, QComboBox, QDialog, QDialogButtonBox, QFileDialog, QFormLayout, QHBoxLayout,
    QLineEdit, QMessageBox, QPushButton, QSpinBox, QTabWidget, QVBoxLayout, QWidget,
)

from .appearance import AVATARS, label


class SettingsDialog(QDialog):
    def __init__(self, profile, network, parameters, parent=None, *, first_run=False):
        super().__init__(parent)
        self.setWindowTitle("Thiết lập LANLink" if first_run else "Cài đặt LANLink")
        self.setMinimumWidth(485)
        self.result_values = None

        outer = QVBoxLayout(self)
        outer.setContentsMargins(22, 20, 22, 20)
        outer.setSpacing(14)
        outer.addWidget(label("Thiết lập LANLink" if first_run else "Cài đặt", "heading"))
        tabs = QTabWidget()
        outer.addWidget(tabs)

        def form_page(title):
            page = QWidget()
            form = QFormLayout(page)
            form.setContentsMargins(16, 16, 16, 16)
            form.setSpacing(12)
            tabs.addTab(page, title)
            return form

        profile_form = form_page("Hồ sơ")
        self.name = QLineEdit(profile.get("name", ""))
        self.name.setMaxLength(40)
        self.name.setPlaceholderText("Tên hiển thị trên mạng")
        self.name.setAccessibleName("Tên hiển thị")
        self.color = QComboBox()
        for shade, title in zip(AVATARS, ("Xanh ngọc", "Xanh lam", "Hổ phách", "Hồng")):
            self.color.addItem(title, shade)
        self.color.setCurrentIndex(max(0, self.color.findData(profile.get("avatar", "teal"))))
        self.status = QComboBox()
        self.status.addItem("Trực tuyến", "online")
        self.status.addItem("Vắng mặt", "away")
        self.status.setCurrentIndex(max(0, self.status.findData(profile.get("status", "online"))))
        profile_form.addRow("Tên hiển thị", self.name)
        profile_form.addRow("Màu ảnh đại diện", self.color)
        profile_form.addRow("Trạng thái", self.status)

        network_form = form_page("Mạng")
        self.interface = QComboBox()
        self.interface.addItem("Tự động chọn IPv4", "")
        for card in network["interfaces"]:
            self.interface.addItem(f'{card["name"]} · {card["ip"]}', card["ip"])
        ip = profile.get("interface", "")
        # Giữ lựa chọn đã lưu ngay cả khi card mạng tạm thời không có trong danh sách.
        if ip and self.interface.findData(ip) < 0:
            self.interface.addItem(ip, ip)
        self.interface.setCurrentIndex(max(0, self.interface.findData(ip)))
        self.tcp = QSpinBox()
        self.udp = QSpinBox()
        for spin, key in ((self.tcp, "tcp_port"), (self.udp, "udp_port")):
            spin.setRange(1, 65535)
            spin.setValue(parameters[key])
        self.broadcast = QLineEdit(parameters.get("broadcast", ""))
        self.broadcast.setPlaceholderText("Tự động theo card mạng")
        network_form.addRow("Card mạng", self.interface)
        network_form.addRow("Cổng TCP", self.tcp)
        network_form.addRow("Cổng UDP", self.udp)
        network_form.addRow("Đích broadcast", self.broadcast)
        network_form.addRow("IPv4 đang dùng", label(network.get("ip") or "Chưa kết nối", "muted"))
        network_form.addRow("Broadcast hiện tại", label(network.get("broadcast") or "—", "muted"))
        network_form.addRow("Socket", label("Đang lắng nghe" if network["running"] else "Chưa hoạt động", "muted"))
        if not first_run:
            network_form.addRow(label("Thay đổi cổng và card mạng có hiệu lực ở lần khởi động sau.", "muted", True))

        appearance = form_page("Giao diện")
        self.theme = QComboBox()
        self.theme.addItem("Tối", "dark")
        self.theme.addItem("Sáng", "light")
        self.theme.setCurrentIndex(max(0, self.theme.findData(profile.get("theme", "dark"))))
        self.notifications = QCheckBox("Thông báo khi thiết bị vào, rời mạng hoặc đổi trạng thái")
        self.notifications.setChecked(profile.get("notifications", True))
        appearance.addRow("Chủ đề", self.theme)
        appearance.addRow(self.notifications)

        files = form_page("Tệp")
        self.downloads = QLineEdit(parameters["downloads"])
        self.downloads.setAccessibleName("Thư mục tải xuống")
        browse = QPushButton("Chọn…")
        browse.clicked.connect(self.choose_folder)
        download_row = QHBoxLayout()
        download_row.addWidget(self.downloads)
        download_row.addWidget(browse)
        self.limit = QSpinBox()
        self.limit.setRange(1, 4096)
        # Hiển thị giới hạn theo MiB; lúc tạo dịch vụ, chương trình mới đổi sang byte.
        self.limit.setSuffix(" MiB")
        self.limit.setValue(parameters["file_limit_mib"])
        files.addRow("Thư mục nhận", download_row)
        files.addRow("Giới hạn mỗi tệp", self.limit)
        files.addRow(label("Tệp nhận tự động được lưu sau khi xác minh. Thay đổi thư mục và giới hạn có hiệu lực ở lần khởi động sau.", "muted", True))

        about = form_page("Giới thiệu")
        about.addRow(label("LANLink Desktop 1.0", "section"))
        about.addRow(label("Connect locally. Communicate instantly.", "muted"))
        about.addRow(label("Đồ án lập trình socket. UDP broadcast tìm thiết bị. TCP gửi tin và truyền tệp trực tiếp.", "muted", True))

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel)
        buttons.button(QDialogButtonBox.StandardButton.Save).setText("Bắt đầu" if first_run else "Lưu thay đổi")
        buttons.button(QDialogButtonBox.StandardButton.Save).setProperty("primary", True)
        buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("Hủy")
        buttons.accepted.connect(self.validate_and_accept)
        buttons.rejected.connect(self.reject)
        outer.addWidget(buttons)

    def choose_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Chọn thư mục lưu tệp", self.downloads.text())
        if folder:
            self.downloads.setText(folder)

    def validate_and_accept(self):
        from ipaddress import IPv4Address

        name = self.name.text().strip()
        if not name or not name.isprintable():
            QMessageBox.warning(self, "LANLink", "Nhập tên hiển thị hợp lệ.")
            return
        broadcast = self.broadcast.text().strip()
        if broadcast:
            try:
                IPv4Address(broadcast)
            except ValueError:
                QMessageBox.warning(self, "LANLink", "Nhập địa chỉ IPv4 broadcast hợp lệ.")
                return
        if not self.downloads.text().strip():
            QMessageBox.warning(self, "LANLink", "Chọn thư mục lưu tệp.")
            return
        profile = {
            "name": name, "avatar": self.color.currentData(), "status": self.status.currentData(),
            "interface": self.interface.currentData(), "theme": self.theme.currentData(),
            "notifications": self.notifications.isChecked(),
        }
        parameters = {
            "tcp_port": self.tcp.value(), "udp_port": self.udp.value(), "broadcast": broadcast,
            "downloads": str(Path(self.downloads.text().strip()).expanduser().absolute()),
            "file_limit_mib": self.limit.value(),
        }
        # Trả hồ sơ và thông số khởi động để tầng gọi lưu chúng vào đúng nơi.
        self.result_values = (profile, parameters)
        self.accept()

