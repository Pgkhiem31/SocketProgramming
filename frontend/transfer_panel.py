from pathlib import Path

from PySide6.QtCore import Qt, QUrl, Signal
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import QFrame, QProgressBar, QPushButton, QScrollArea, QVBoxLayout, QWidget

from .appearance import TRANSFER, error_text, label, size_text
from backend.file_transfers import percent


class TransferCard(QFrame):
    open_requested = Signal(str)

    def __init__(self, transfer):
        super().__init__()
        self.setObjectName("transferCard")
        self.transfer_id = transfer["transfer_id"]
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 9, 10, 9)
        layout.setSpacing(5)
        name = label(transfer["filename"], "fileName", True)
        name.setMinimumWidth(1)
        layout.addWidget(name)
        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setTextVisible(True)
        layout.addWidget(self.progress)
        self.caption = label("", "muted", True)
        layout.addWidget(self.caption)
        self.open_button = QPushButton("Mở tệp đã nhận")
        self.open_button.clicked.connect(lambda: self.open_requested.emit(self.transfer_id))
        layout.addWidget(self.open_button)
        self.update_transfer(transfer)

    def update_transfer(self, transfer):
        # Dùng cùng quy tắc tính tiến độ của backend cho cả bản ghi cũ và sự kiện mới.
        count = percent(transfer["size"], transfer["transferred"], transfer["state"])
        self.progress.setValue(count)
        direction = "Gửi" if transfer["direction"] == "out" else "Nhận"
        description = f"{direction} · {TRANSFER[transfer['state']]}\n{size_text(transfer['transferred'])} / {size_text(transfer['size'])}"
        if transfer["error"]:
            description += f"\n{error_text(transfer['error'])}"
        self.caption.setText(description)
        self.open_button.setVisible(bool(transfer["saved_name"]))


class TransferPanel(QScrollArea):
    notice = Signal(str)

    def __init__(self, downloads: Path):
        super().__init__()
        self.downloads = downloads
        self.cards = {}
        self.records = {}
        self.selected = ""
        self.setWidgetResizable(True)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setFrameShape(QFrame.Shape.NoFrame)
        container = QWidget()
        container.setObjectName("detailsPanel")
        self.layout = QVBoxLayout(container)
        self.layout.setContentsMargins(16, 16, 16, 16)
        self.layout.setSpacing(10)
        self.layout.addWidget(label("CHI TIẾT", "eyebrow"))
        self.avatar = label()
        self.layout.addWidget(self.avatar)
        self.peer = label("Chưa chọn thiết bị", "section", True)
        self.diagnostics = label("", "muted", True)
        self.layout.addWidget(self.peer)
        self.layout.addWidget(self.diagnostics)
        self.connect_button = QPushButton("Kết nối")
        self.connect_button.hide()
        self.layout.addWidget(self.connect_button)
        self.layout.addWidget(label("Tệp và lượt truyền", "section"))
        self.empty = label("Chưa có lượt truyền tệp.", "muted", True)
        self.layout.addWidget(self.empty)
        self.layout.addStretch(1)
        self.setWidget(container)

    def select(self, peer_id):
        self.selected = peer_id
        self.update_visibility()

    def update_transfer(self, transfer):
        key = transfer["transfer_id"]
        self.records[key] = transfer
        # Giữ một thẻ cho mỗi lượt truyền, rồi cập nhật thẻ theo mã lượt.
        if key in self.cards:
            self.cards[key].update_transfer(transfer)
        else:
            card = TransferCard(transfer)
            card.open_requested.connect(self.open_file)
            self.cards[key] = card
            self.layout.insertWidget(7, card)
        self.update_visibility()

    def update_visibility(self):
        count = 0
        for key, card in self.cards.items():
            # Khi đã chọn thiết bị, chỉ hiện các lượt truyền liên quan đến thiết bị đó.
            shown = not self.selected or self.records[key]["peer_id"] == self.selected
            card.setVisible(shown)
            count += int(shown)
        self.empty.setVisible(count == 0)

    def open_file(self, transfer_id):
        name = self.records[transfer_id]["saved_name"]
        path = self.downloads / name
        # Chỉ mở file thường nằm trực tiếp trong thư mục tải xuống.
        if path.parent != self.downloads or path.is_symlink() or not path.is_file():
            self.notice.emit("Tệp đã nhận không còn trong thư mục tải xuống.")
            return
        if not QDesktopServices.openUrl(QUrl.fromLocalFile(str(path))):
            self.notice.emit("Không mở được tệp trên máy này.")

