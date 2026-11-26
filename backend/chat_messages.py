"""Persist messages before emitting events or acknowledging receipt."""

from __future__ import annotations

import time
from threading import Event, Lock
from typing import TYPE_CHECKING
from uuid import UUID

from .app_errors import LanError
from .wire_protocol import ChatAck, ChatMessage, Header, ProtocolError, message_body
from .tcp_sessions import Session, SocketErrors
from .sqlite_store import Record

if TYPE_CHECKING:
    from .lan_service import LanLink
    from .tcp_sessions import Sessions

ACK_TIMEOUT = 5.0


class Chat:
    def __init__(self, service: LanLink):
        self.service = service
        self.lock = Lock()
        self.pending: dict[tuple[str, str], Event] = {}

    def receive(self, session: Session, header: Header, body: bytes) -> None:
        peer_id = session.peer["peer_id"]
        # Đánh thức đúng worker đang chờ ACK của máy và mã tin nhắn tương ứng.
        if isinstance(header, ChatAck):
            with self.lock:
                acknowledgement = self.pending.get((peer_id, str(header.message_id)))
                if acknowledgement:
                    acknowledgement.set()
            return
        if not isinstance(header, ChatMessage):
            raise ProtocolError("Unexpected frame in chat session")
        try:
            text = body.decode("utf-8")
        except UnicodeError as exc:
            raise ProtocolError("Invalid chat encoding") from exc
        message_body(text)
        # Lưu tin nhận vào SQLite trước khi cập nhật giao diện và gửi ACK.
        message = self.service.store.insert_message(session.peer, str(header.message_id), text, "in")
        self.service.events.emit("message_received", message=message)
        session.send(ChatAck(message_id=header.message_id))

    def send(self, peer_id: str, message_id: UUID, text: str) -> Record:
        try:
            body = message_body(text)
        except ProtocolError as exc:
            raise LanError(str(exc)) from exc
        sessions = self.service.require_sessions()
        if sessions.is_paused(peer_id):
            raise LanError("chat_paused")
        key = (peer_id, str(message_id))
        with self.lock:
            if key in self.pending:
                raise LanError("message_busy")
            ack = self.pending[key] = Event()
        # Nếu worker chưa nhận việc, hàm hiện tại phải tự dọn mục chờ ACK.
        queued = False
        try:
            live = self.service.registry.get(peer_id)
            active = sessions.get_active(peer_id)
            if not live and not active:
                raise LanError("peer_offline")
            peer = live.public() if live else active.peer
            saved = self.service.store.insert_message(peer, key[1], text, "out")
            if saved["state"] == "sent":
                return saved
            sending = self._state(key, "sending")
            try:
                sessions.workers.launch("chat-send", lambda: self._send(sessions, key, message_id, body, ack))
                queued = True
            except LanError as exc:
                return self._state(key, "failed", exc.code)
            return sending
        finally:
            # Khi đã lên lịch thành công, worker gửi chịu trách nhiệm dọn mục chờ ACK.
            if not queued:
                with self.lock:
                    self.pending.pop(key, None)

    def _state(self, key: tuple[str, str], state: str, error: str | None = None) -> Record:
        message = self.service.store.set_message_state(*key, state, error)
        self.service.events.emit("message_updated", message=message)
        return message

    def _send(self, sessions: Sessions, key: tuple[str, str], message_id: UUID, body: bytes, ack: Event) -> None:
        try:
            frame = ChatMessage(message_id=message_id, body_length=len(body))
            error = self._send_with_retry(sessions, key[0], frame, body, ack)
            with self.lock:
                if ack.is_set():
                    self._state(key, "sent")
                else:
                    self._state(key, "failed", error or "connection_failed")
        finally:
            with self.lock:
                self.pending.pop(key, None)

    def _send_with_retry(
        self, sessions: Sessions, peer_id: str, frame: ChatMessage, body: bytes, ack: Event,
    ) -> str | None:
        # Dùng lại mã tin nhắn khi thử lại để máy nhận có thể tránh lưu trùng.
        for _ in range(2):
            if ack.is_set():
                return None
            try:
                session = sessions.connect(peer_id)
            except LanError as exc:
                return exc.code
            try:
                session.send(frame, body)
            except SocketErrors:
                sessions.retire(session)
            else:
                error = self._wait_for_ack(ack, session.closed, sessions.workers.stop)
                if error != "connection_failed":
                    return error
            if sessions.workers.stop.wait(0.08):
                return "connection_failed"
        return "connection_failed"

    @staticmethod
    def _wait_for_ack(ack: Event, closed: Event, stopping: Event) -> str | None:
        # Chờ theo đồng hồ đơn điệu, đồng thời kiểm tra kết nối và tín hiệu dừng.
        deadline = time.monotonic() + ACK_TIMEOUT
        while not ack.wait(0.05):
            if closed.is_set() or stopping.is_set():
                return "connection_failed"
            if time.monotonic() >= deadline:
                return "ack_timeout"
        return None
