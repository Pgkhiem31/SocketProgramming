from bisect import bisect_left
from datetime import datetime

from PySide6.QtCore import QSize, Qt, QTimer, Signal
from PySide6.QtWidgets import (
    QFrame, QHBoxLayout, QPushButton, QScrollArea, QSizePolicy, QTextEdit, QVBoxLayout, QWidget,
)

from .appearance import avatar, error_text, label


class Composer(QTextEdit):
    submitted = Signal()

    # Xử lý phím gửi ở ô soạn thảo trước khi chuyển các phím còn lại cho QTextEdit.
    def keyPressEvent(self, event):
        enter_keys = (Qt.Key.Key_Return, Qt.Key.Key_Enter)
        if event.key() in enter_keys and not event.modifiers() & Qt.KeyboardModifier.ShiftModifier:
            self.submitted.emit()
            return
        super().keyPressEvent(event)


class MessageBubble(QFrame):
    def __init__(self, message):
        super().__init__()
        self.setObjectName("sentBubble" if message["direction"] == "out" else "receivedBubble")
        self.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Maximum)
        content = QVBoxLayout(self)
        content.setContentsMargins(12, 8, 12, 7)
        content.setSpacing(4)
        self.body = label(message["text"], wrap=True)
        self.body.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.body.setMinimumWidth(1)
        self.meta = label("", "muted", True)
        content.addWidget(self.body)
        content.addWidget(self.meta)
        self.update_status(message)

    def update_status(self, message):
        time = datetime.fromisoformat(message["timestamp"]).astimezone().strftime("%H:%M")
        status = {"sending": "Đang gửi", "sent": "Đã xác nhận", "failed": "Gửi thất bại"}.get(message["state"])
        metadata = [time]
        if status:
            metadata.append(status)
        if message["error"]:
            metadata.append(error_text(message["error"]))
        self.meta.setText(" · ".join(metadata))
        body_width = max(self.body.fontMetrics().horizontalAdvance(line)
                         for line in self.body.text().split("\n"))
        meta_width = self.meta.fontMetrics().horizontalAdvance(self.meta.text())
        self.setFixedWidth(max(115, min(350, max(body_width, meta_width) + 28)))


class ChatPanel(QWidget):
    send_requested = Signal()
    attachment_requested = Signal()
    bottom_reached = Signal()
    details_requested = Signal()
    older_requested = Signal()

    def __init__(self):
        super().__init__()
        self.bubbles = {}
        self.loaded_ids = []
        self.scroll_to_bottom = False
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        header = QFrame()
        header.setObjectName("chatHeader")
        topline = QHBoxLayout(header)
        topline.setContentsMargins(22, 14, 20, 14)
        self.avatar = label()
        self.avatar.setFixedSize(QSize(40, 40))
        topline.addWidget(self.avatar)
        heading = QVBoxLayout()
        self.title = label("Tin nhắn", "section")
        self.subtitle = label("Chọn một thiết bị để bắt đầu.", "muted", True)
        heading.addWidget(self.title)
        heading.addWidget(self.subtitle)
        topline.addLayout(heading, 1)
        self.details_button = QPushButton("Chi tiết")
        self.details_button.setCheckable(True)
        self.details_button.setChecked(True)
        self.details_button.clicked.connect(self.details_requested)
        topline.addWidget(self.details_button)
        layout.addWidget(header)

        self.older = QPushButton("Tải tin nhắn cũ hơn")
        self.older.clicked.connect(self.older_requested)
        self.older.hide()
        layout.addWidget(self.older, 0, Qt.AlignmentFlag.AlignHCenter)
        self.scroll = QScrollArea()
        self.scroll.setObjectName("chatScroll")
        self.scroll.setWidgetResizable(True)
        self.scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.scroll.setFrameShape(QFrame.Shape.NoFrame)
        self.history = QWidget()
        self.history.setObjectName("history")
        self.messages = QVBoxLayout(self.history)
        self.messages.setContentsMargins(22, 18, 22, 18)
        self.messages.setSpacing(9)
        self.messages.addStretch(1)
        self.empty = label("Chọn một thiết bị để bắt đầu.", "muted", True)
        self.empty.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.messages.addWidget(self.empty, 0, Qt.AlignmentFlag.AlignCenter)
        self.messages.addStretch(1)
        self.scroll.setWidget(self.history)
        self.scroll.verticalScrollBar().rangeChanged.connect(self.follow_bottom)
        self.scroll.verticalScrollBar().valueChanged.connect(self.report_bottom)
        layout.addWidget(self.scroll, 1)

        footer = QFrame()
        footer.setObjectName("composerArea")
        compose = QVBoxLayout(footer)
        compose.setContentsMargins(18, 13, 18, 12)
        compose.setSpacing(8)
        row = QHBoxLayout()
        self.attach = QPushButton("Đính kèm")
        self.attach.setToolTip("Chọn tệp để gửi")
        self.attach.clicked.connect(self.attachment_requested)
        self.composer = Composer()
        self.composer.setPlaceholderText("Viết tin nhắn…")
        self.composer.setAccessibleName("Nội dung tin nhắn")
        self.composer.setAcceptRichText(False)
        self.composer.setFixedHeight(59)
        self.composer.submitted.connect(self.send_requested)
        self.send_button = QPushButton("Gửi")
        self.send_button.setProperty("primary", True)
        self.send_button.clicked.connect(self.send_requested)
        self.composer.textChanged.connect(self.update_send)
        row.addWidget(self.attach, 0, Qt.AlignmentFlag.AlignBottom)
        row.addWidget(self.composer, 1)
        row.addWidget(self.send_button, 0, Qt.AlignmentFlag.AlignBottom)
        compose.addLayout(row)
        compose.addWidget(label("Enter để gửi · Shift+Enter để xuống dòng", "muted"))
        layout.addWidget(footer)
        self.set_available(False)

    def set_peer(self, peer, subtitle):
        self.title.setText(peer["name"] if peer else "Tin nhắn")
        self.subtitle.setText(subtitle)
        self.avatar.setVisible(bool(peer))
        if peer:
            self.avatar.setPixmap(avatar(peer["name"], peer.get("avatar", "teal"), 40, peer["status"]))

    def set_available(self, available):
        self.composer.setEnabled(available)
        self.attach.setEnabled(available)
        self.update_send()

    def update_send(self):
        self.send_button.setEnabled(self.composer.isEnabled() and bool(self.composer.toPlainText().strip()))

    def clear_history(self, has_peer):
        for row, _ in self.bubbles.values():
            self.messages.removeWidget(row)
            row.deleteLater()
        self.bubbles.clear()
        self.loaded_ids.clear()
        self.empty.setText("Chưa có tin nhắn" if has_peer else "Chọn một thiết bị để bắt đầu.")
        self.empty.show()
        self.older.hide()
        self.messages.setStretch(self.messages.count() - 1, 1)
        self.scroll_to_bottom = True

    def near_bottom(self):
        bar = self.scroll.verticalScrollBar()
        return bar.maximum() - bar.value() <= 70

    def follow_bottom(self, minimum, maximum):
        if self.scroll_to_bottom:
            self.scroll.verticalScrollBar().setValue(maximum)
            self.scroll_to_bottom = False

    def report_bottom(self, value):
        if self.near_bottom():
            self.bottom_reached.emit()

    def show_older(self, messages):
        bar = self.scroll.verticalScrollBar()
        # Ghi vị trí cuộn trước khi chèn tin cũ, rồi bù phần chiều cao tăng thêm.
        before, position = bar.maximum(), bar.value()
        self.scroll_to_bottom = False
        for message in messages:
            self.show_message(message, follow=False)
        self.messages.activate()
        QTimer.singleShot(0, lambda: bar.setValue(position + max(0, bar.maximum() - before)))

    def show_message(self, message, *, follow=True):
        seq = message["seq"]
        # Nếu tin đã xuất hiện, chỉ cập nhật trạng thái trên bong bóng hiện có.
        if seq in self.bubbles:
            self.bubbles[seq][1].update_status(message)
            return
        # Chỉ tự cuộn xuống khi đang theo dõi cuối cuộc trò chuyện hoặc vừa gửi tin.
        if follow:
            self.scroll_to_bottom = self.scroll_to_bottom or self.near_bottom() or message["direction"] == "out"
        self.empty.hide()
        self.messages.setStretch(self.messages.count() - 1, 0)
        bubble = MessageBubble(message)
        row = QWidget()
        row.setObjectName("messageRow")
        layout = QHBoxLayout(row)
        layout.setContentsMargins(0, 0, 0, 0)
        if message["direction"] == "out":
            layout.addStretch(1)
        layout.addWidget(bubble)
        if message["direction"] == "in":
            layout.addStretch(1)
        # Chèn theo số thứ tự để tin tải từ lịch sử và tin mới nằm đúng vị trí.
        index = bisect_left(self.loaded_ids, seq)
        self.loaded_ids.insert(index, seq)
        self.messages.insertWidget(index + 2, row)
        self.bubbles[seq] = row, bubble

