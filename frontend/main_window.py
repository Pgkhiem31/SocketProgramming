"""The desktop window owns UI state. LANLink owns sockets and persistence."""

from pathlib import Path

from PySide6.QtCore import QSignalBlocker, QSize, Qt, QTimer, Slot
from PySide6.QtGui import QKeySequence, QShortcut
from PySide6.QtWidgets import (
    QComboBox, QDialog, QFileDialog, QFrame, QHBoxLayout, QLineEdit,
    QListWidget, QListWidgetItem, QMainWindow, QMessageBox, QPushButton,
    QSplitter, QVBoxLayout, QWidget,
)

from .appearance import CONNECTION, PRESENCE, avatar, error_text, icon, label, theme
from .chat_panel import ChatPanel
from .settings_dialog import SettingsDialog
from .signals import Actions
from .transfer_panel import TransferPanel

HISTORY_PAGE = 250


class MainWindow(QMainWindow):
    def __init__(self, service, signals, parameters, save_parameters):
        super().__init__()
        self.service = service
        self.actions = Actions(signals)
        self.parameters = parameters
        self.save_parameters = save_parameters
        self.profile = service.profile()
        self.network = {"interfaces": [], "running": False}
        self.peers = {}
        self.conversations = {}
        self.connections = {}
        self.drafts = {}
        self.read_through = {}
        self.selected = ""
        self.pending_send = set()
        self.ready_to_close = False

        self.setWindowTitle("LANLink Desktop")
        self.resize(1220, 760)
        self.setMinimumSize(900, 570)
        self.build()
        self.show_profile()
        self.update_lists()
        self.update_selected()

        self.summary_timer = QTimer(self, interval=50, singleShot=True)
        self.summary_timer.timeout.connect(self.refresh_conversations)
        signals.received.connect(self.on_event, Qt.ConnectionType.QueuedConnection)
        self.actions.run(service.start_network)
        self.refresh_conversations()
        self.actions.run(service.store.transfers, finished="transfers_loaded")

    def build(self):
        outer = QWidget()
        layout = QVBoxLayout(outer)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        header = QFrame()
        header.setObjectName("mainHeader")
        row = QHBoxLayout(header)
        row.setContentsMargins(22, 10, 22, 10)
        row.addWidget(label("LANLink", "wordmark"))
        row.addStretch(1)
        self.network_label = label("Đang khởi động…", "networkPill")
        row.addWidget(self.network_label)
        layout.addWidget(header)

        self.chat = ChatPanel()
        self.details = TransferPanel(self.service.config.downloads)
        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        for panel, width in ((self.build_sidebar(), 220), (self.chat, 415), (self.details, 210)):
            panel.setMinimumWidth(width)
            self.splitter.addWidget(panel)
        self.splitter.setChildrenCollapsible(False)
        self.splitter.setStretchFactor(1, 1)
        self.splitter.setSizes([250, 690, 280])
        layout.addWidget(self.splitter, 1)

        self.notice_label = label("", "notice", True)
        self.notice_label.setMinimumHeight(28)
        self.notice_label.hide()
        layout.addWidget(self.notice_label)
        self.notice_timer = QTimer(self, interval=5500, singleShot=True)
        self.notice_timer.timeout.connect(self.notice_label.hide)
        self.setCentralWidget(outer)

        self.chat.send_requested.connect(self.send_message)
        self.chat.attachment_requested.connect(self.send_file)
        self.chat.bottom_reached.connect(self.mark_read)
        self.chat.details_requested.connect(self.toggle_details)
        self.chat.older_requested.connect(self.load_older)
        self.details.connect_button.clicked.connect(self.toggle_connection)
        self.details.notice.connect(self.notice)
        QShortcut(QKeySequence("Ctrl+K"), self, activated=self.search.setFocus)
        QShortcut(QKeySequence("Ctrl+O"), self, activated=self.send_file)
        QShortcut(QKeySequence("F1"), self, activated=self.show_help)

    def build_sidebar(self):
        sidebar = QFrame()
        sidebar.setObjectName("sidebar")
        layout = QVBoxLayout(sidebar)
        layout.setContentsMargins(15, 16, 15, 13)
        layout.setSpacing(8)
        profile_row = QHBoxLayout()
        self.profile_avatar = label()
        profile_row.addWidget(self.profile_avatar)
        profile_column = QVBoxLayout()
        self.profile_name = label("", "section", True)
        profile_column.addWidget(self.profile_name)
        self.status = QComboBox()
        self.status.setAccessibleName("Trạng thái của tôi")
        for key in ("online", "away"):
            self.status.addItem(PRESENCE[key], key)
        self.status.currentIndexChanged.connect(self.change_status)
        profile_column.addWidget(self.status)
        profile_row.addLayout(profile_column, 1)
        layout.addLayout(profile_row)

        self.search = QLineEdit()
        self.search.setPlaceholderText("Tìm thiết bị…")
        self.search.setAccessibleName("Tìm thiết bị hoặc cuộc trò chuyện")
        self.search.textChanged.connect(self.update_lists)
        layout.addWidget(self.search)
        self.device_heading = label("THIẾT BỊ", "eyebrow")
        layout.addWidget(self.device_heading)
        self.devices = self.make_list("Thiết bị trong LAN")
        self.devices_empty = label("Chưa tìm thấy thiết bị trong LAN.", "muted", True)
        layout.addWidget(self.devices_empty)
        layout.addWidget(self.devices, 1)
        layout.addWidget(label("CUỘC TRÒ CHUYỆN", "eyebrow"))
        self.recent = self.make_list("Cuộc trò chuyện gần đây")
        self.recent_empty = label("Chưa có cuộc trò chuyện.", "muted", True)
        layout.addWidget(self.recent_empty)
        layout.addWidget(self.recent, 1)
        toolbar = QHBoxLayout()
        settings = QPushButton("Cài đặt")
        settings.clicked.connect(self.request_settings)
        help_button = QPushButton("Trợ giúp")
        help_button.clicked.connect(self.show_help)
        toolbar.addWidget(settings)
        toolbar.addWidget(help_button)
        layout.addLayout(toolbar)
        return sidebar

    def make_list(self, name):
        widget = QListWidget()
        widget.setAccessibleName(name)
        widget.setIconSize(QSize(33, 33))
        widget.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        widget.itemClicked.connect(self.choose_item)
        widget.itemActivated.connect(self.choose_item)
        return widget

    def choose_item(self, item):
        self.select(item.data(Qt.ItemDataRole.UserRole))

    def show_profile(self):
        name = self.profile["name"]
        self.profile_avatar.setPixmap(avatar(name, self.profile["avatar"], 38, self.profile["status"]))
        self.profile_name.setText(name)
        with QSignalBlocker(self.status):
            self.status.setCurrentIndex(self.status.findData(self.profile["status"]))
        theme(self.profile["theme"])

    def change_status(self):
        self.actions.run(self.service.update, {"status": self.status.currentData()})

    def add_peer_row(self, widget, peer, subtitle):
        peer_id = peer["peer_id"]
        unread = self.conversations.get(peer_id, {}).get("unread", 0)
        badge = f"  ·  {min(unread, 99)}{'+' if unread > 99 else ''}" if unread else ""
        item = QListWidgetItem(icon(peer["name"], peer["avatar"], peer["status"]),
                               f'{peer["name"]}{badge}\n{subtitle}')
        item.setSizeHint(QSize(0, 50))
        item.setToolTip(f'{peer["name"]} · {PRESENCE[peer["status"]]} · {unread} tin chưa đọc')
        item.setData(Qt.ItemDataRole.UserRole, peer_id)
        widget.addItem(item)
        if peer_id == self.selected:
            widget.setCurrentItem(item)

    def update_lists(self):
        query = self.search.text().strip().casefold()
        self.devices.clear()
        self.recent.clear()
        live = sorted(self.peers.values(), key=lambda peer: (peer["status"] != "online", peer["name"].casefold()))
        self.device_heading.setText(f"THIẾT BỊ TRONG LAN  ·  {len(live)}")
        for peer in live:
            if query in peer["name"].casefold() or query in peer["ip"]:
                self.add_peer_row(self.devices, peer, f'{PRESENCE[peer["status"]]} · {peer["ip"]}')
        for peer_id, conversation in self.conversations.items():
            peer = self.get_peer(peer_id)
            if query in peer["name"].casefold():
                preview = " ".join((conversation["last_text"] or "Chưa có tin nhắn").split())
                self.add_peer_row(self.recent, peer, preview if len(preview) <= 28 else f"{preview[:27]}…")
        for widget, empty, original in (
            (self.devices, self.devices_empty, "Chưa tìm thấy thiết bị trong LAN."),
            (self.recent, self.recent_empty, "Chưa có cuộc trò chuyện."),
        ):
            empty.setText("Không có kết quả." if query else original)
            empty.setVisible(widget.count() == 0)

    def get_peer(self, peer_id):
        if peer_id in self.peers:
            return self.peers[peer_id]
        saved = self.conversations.get(peer_id)
        return dict(saved, status="offline") if saved else None

    def select(self, peer_id):
        if peer_id == self.selected:
            return
        # Giữ bản nháp theo từng thiết bị khi chuyển cuộc trò chuyện.
        if self.selected:
            self.drafts[self.selected] = self.chat.composer.toPlainText()
        self.selected = peer_id
        self.chat.clear_history(bool(peer_id))
        self.chat.composer.setPlainText(self.drafts.get(peer_id, ""))
        self.details.select(peer_id)
        self.update_lists()
        self.update_selected()
        if peer_id:
            self.load_history(None)
        if peer_id in self.peers and self.connections.get(peer_id) not in ("paused", "connected", "connecting"):
            self.actions.run(self.service.connect, peer_id, peer_id=peer_id)

    def update_selected(self):
        peer = self.get_peer(self.selected)
        state = self.connections.get(self.selected, "disconnected")
        online = self.selected in self.peers
        text = f'{PRESENCE[peer["status"]]}  ·  {CONNECTION[state]}' if peer else "Chọn thiết bị để bắt đầu."
        header = f'{text}  ·  {peer["ip"]}' if peer else text
        self.chat.set_peer(peer, header)
        self.chat.set_available(online and state != "paused")
        if self.selected in self.pending_send:
            self.chat.send_button.setEnabled(False)
        if peer:
            self.details.avatar.setPixmap(avatar(peer["name"], peer["avatar"], 42, peer["status"]))
        self.details.avatar.setVisible(bool(peer))
        self.details.peer.setText(peer["name"] if peer else "Chưa chọn thiết bị")
        self.details.diagnostics.setText(
            f'{peer["ip"]}:{peer["tcp_port"]}\n{text}' if peer else "Địa chỉ và trạng thái TCP sẽ hiện tại đây.")
        self.details.connect_button.setVisible(online)
        self.details.connect_button.setEnabled(state != "connecting")
        self.details.connect_button.setText("Ngắt kết nối" if state == "connected" else "Kết nối")

    def refresh_conversations(self):
        self.actions.run(self.service.store.conversations, finished="conversations_loaded")

    def load_older(self):
        if self.chat.loaded_ids:
            self.chat.older.setEnabled(False)
            self.load_history(self.chat.loaded_ids[0])

    def load_history(self, before):
        self.actions.run(self.service.store.messages, self.selected, before, HISTORY_PAGE + 1,
                         finished="history_loaded", peer_id=self.selected, before=before)

    # Chỉ đánh dấu đã đọc khi cửa sổ đang hoạt động và người dùng ở gần cuối đoạn chat.
    def mark_read(self):
        peer_id = self.selected
        if not self.isActiveWindow() or not peer_id or not self.chat.near_bottom() or not self.chat.loaded_ids:
            return
        through = self.chat.loaded_ids[-1]
        if through <= self.read_through.get(peer_id, 0):
            return
        self.read_through[peer_id] = through
        self.actions.run(self.service.store.mark_read, peer_id, through, finished="read_saved")

    def toggle_details(self):
        self.details.setVisible(self.chat.details_button.isChecked())

    def toggle_connection(self):
        peer_id = self.selected
        if peer_id in self.peers:
            action = self.service.leave if self.connections.get(peer_id) == "connected" else self.service.connect
            self.actions.run(action, peer_id, peer_id=peer_id)

    def send_message(self):
        if not self.chat.send_button.isEnabled() or self.selected in self.pending_send:
            return
        peer_id = self.selected
        draft = self.chat.composer.toPlainText()
        command = draft.strip()
        if command.startswith("/"):
            if command == "/help":
                self.show_help()
            else:
                self.notice("Lệnh không hợp lệ. Dùng /help hoặc F1 để xem trợ giúp.")
            return
        self.pending_send.add(peer_id)
        self.chat.send_button.setEnabled(False)
        self.actions.run(self.service.send_message, peer_id, draft, finished="message_accepted", peer_id=peer_id, draft=draft)

    def send_file(self):
        if not self.chat.attach.isEnabled():
            return
        peer_id = self.selected
        # Hộp thoại chọn một file; backend sẽ khởi chạy lượt truyền trong luồng nền.
        path, _ = QFileDialog.getOpenFileName(self, "Chọn tệp để gửi")
        if path:
            self.actions.run(self.service.send_file, peer_id, Path(path), peer_id=peer_id)

    def notice(self, message):
        self.notice_label.setText(message)
        self.notice_label.show()
        self.notice_timer.start()

    def show_help(self):
        QMessageBox.information(self, "Trợ giúp LANLink",
            "Chọn thiết bị để kết nối TCP. Enter gửi tin, Shift+Enter xuống dòng.\n"
            "Ctrl+K tìm thiết bị, Ctrl+O đính kèm tệp, F1 hoặc /help mở trợ giúp.\n"
            "Mở Chi tiết để ngắt kết nối hoặc xem các lượt truyền tệp.")

    def request_settings(self):
        self.actions.run(self.service.network, finished="settings_network")

    def open_settings(self, network):
        dialog = SettingsDialog(self.profile, network, self.parameters, self)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        profile, parameters = dialog.result_values
        changed = parameters != self.parameters or profile["interface"] != self.profile["interface"]
        self.save_parameters(parameters)
        self.parameters = parameters
        self.actions.run(self.service.update, profile)
        if changed:
            self.notice("Đã lưu cấu hình. Khởi động lại LANLink để áp dụng thay đổi mạng và tệp.")

    @Slot(str, object)
    # Nhận sự kiện trên luồng giao diện rồi cập nhật widget tương ứng.
    def on_event(self, kind, data):
        if kind == "shutdown_complete":
            self.ready_to_close = True
            self.close()
            return
        if self.actions.closing:
            return
        if kind.startswith("peer_"):
            self.on_presence(kind, data["peer"])
        elif kind == "network_changed":
            self.network = data["network"]
            ip = self.network["ip"]
            self.network_label.setText(
                f'{ip}  ·  TCP {self.network["tcp_port"]}  /  UDP {self.network["udp_port"]}' if ip else "Mạng chưa sẵn sàng")
            if self.network["error"]:
                self.notice(error_text(self.network["error"]))
        elif kind == "connection_changed":
            self.on_connection(data)
        elif kind == "profile_updated":
            changed = data["profile"]["theme"] != self.profile["theme"]
            self.profile = data["profile"]
            self.show_profile()
            if changed:
                self.notice("Đã đổi chủ đề.")
        elif kind in ("message_received", "message_updated"):
            self.display_message(data["message"])
            if kind == "message_received" and self.profile["notifications"]:
                peer_id = data["message"]["peer_id"]
                peer = self.get_peer(peer_id)
                if peer and peer_id != self.selected:
                    self.notice(f'Tin nhắn mới từ {peer["name"]}.')
        # Bỏ qua lịch sử trả về muộn nếu người dùng đã chuyển sang cuộc trò chuyện khác.
        elif kind == "history_loaded" and data["peer_id"] == self.selected:
            messages = [item for item in data["result"][-HISTORY_PAGE:] if item["seq"] not in self.chat.bubbles]
            self.chat.older.setEnabled(True)
            self.chat.older.setVisible(len(data["result"]) > HISTORY_PAGE)
            if data["before"] is None:
                for message in messages:
                    self.chat.show_message(message)
                QTimer.singleShot(0, self.mark_read)
            elif data["before"] in self.chat.bubbles:
                self.chat.show_older(messages)
        elif kind == "message_accepted":
            self.on_message_accepted(data)
        elif kind == "conversations_loaded":
            self.conversations = {item["peer_id"]: item for item in data["result"]}
            self.update_lists()
            self.update_selected()
        elif kind == "read_saved":
            self.summary_timer.start()
        elif kind == "transfer_updated":
            self.details.update_transfer(data["transfer"])
            if data["transfer"]["state"] == "failed":
                self.notice(error_text(data["transfer"]["error"]))
            elif data["transfer"]["state"] == "completed" and data["transfer"]["direction"] == "in":
                self.notice(f'Đã nhận tệp {data["transfer"]["filename"]}.')
            self.summary_timer.start()
        # Giữ bản ghi đã được cập nhật trực tiếp nếu lịch sử tải về sau.
        elif kind == "transfers_loaded":
            for transfer in reversed(data["result"]):
                if transfer["transfer_id"] not in self.details.records:
                    self.details.update_transfer(transfer)
        elif kind == "settings_network":
            self.open_settings(data["result"])
        elif kind in ("action_error", "network_error"):
            if "before" in data and data["peer_id"] == self.selected:
                self.chat.older.setEnabled(True)
            if "draft" in data:
                self.pending_send.discard(data["peer_id"])
                self.update_selected()
            self.notice(error_text(data["error"]))

    def on_presence(self, kind, peer):
        peer_id = peer["peer_id"]
        if kind == "peer_offline":
            self.peers.pop(peer_id, None)
            if peer_id == self.selected and peer_id not in self.conversations:
                self.conversations[peer_id] = dict(peer, unread=0, last_text=None)
        else:
            self.peers[peer_id] = peer
        self.update_lists()
        self.update_selected()
        self.summary_timer.start()
        if self.profile["notifications"] and kind != "peer_updated":
            self.notice(f'{peer["name"]} · {PRESENCE[peer["status"]]}')

    def on_connection(self, data):
        peer_id, state = data["peer_id"], data["state"]
        previous = self.connections.get(peer_id)
        self.connections[peer_id] = state
        if peer_id == self.selected:
            self.update_selected()
            if state == "paused":
                self.chat.details_button.setChecked(True)
                self.details.show()
            elif state == "disconnected" and previous == "connected":
                self.notice("Thiết bị đã đóng phiên TCP. Bạn có thể chọn cuộc trò chuyện khác hoặc kết nối lại.")
            elif data["error"]:
                self.notice(error_text(data["error"]))

    def display_message(self, message):
        if message["peer_id"] == self.selected:
            self.chat.show_message(message)
            QTimer.singleShot(0, self.mark_read)
        self.summary_timer.start()

    def on_message_accepted(self, data):
        peer_id = data["peer_id"]
        self.pending_send.discard(peer_id)
        # Chỉ xóa đúng bản nháp đã gửi để giữ phần người dùng nhập thêm trong lúc chờ.
        if peer_id == self.selected and self.chat.composer.toPlainText() == data["draft"]:
            self.chat.composer.clear()
        elif peer_id != self.selected and self.drafts.get(peer_id) == data["draft"]:
            self.drafts.pop(peer_id, None)
        message = data["result"]
        if peer_id != self.selected or message["seq"] not in self.chat.bubbles:
            self.display_message(message)
        self.update_selected()

    def changeEvent(self, event):
        super().changeEvent(event)
        if hasattr(self, "chat") and self.isActiveWindow():
            QTimer.singleShot(0, self.mark_read)

    def closeEvent(self, event):
        if self.ready_to_close:
            event.accept()
            return
        # Giữ cửa sổ mở đến khi backend đóng kết nối và báo shutdown_complete.
        event.ignore()
        self.setEnabled(False)
        self.network_label.setText("Đang đóng các kết nối…")
        self.actions.stop(self.service.close)

