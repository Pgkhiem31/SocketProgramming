"""Gửi, nhận, lưu và xác nhận tin nhắn chat."""
from __future__ import annotations

import logging
import sqlite3
import time
from threading import Event, Lock, Thread, current_thread
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from .app_errors import (
    AckTimeoutError,
    ConnectionFailedError,
    InternalError,
    MessageEncodingError,
    PeerOfflineError,
    StorageError,
    WorkerStartError,
    message_error_from_protocol,
)
from .sqlite_store import MessageIdConflictError, Record
from .tcp_sessions import Session
from .wire_protocol import ChatAck, ChatMessage, Header, ProtocolError, message_body

if TYPE_CHECKING:
    from .lan_service import LanService

log = logging.getLogger(__name__)
ACK_TIMEOUT = 5.0


class ChatMessages:
    """Xử lý frame chat; lưu tin vào SQLite trước khi phát sự kiện/ACK."""

    def __init__(self, service: LanService):
        self.service = service
        self._lock = Lock()
        # Sự kiện chờ ACK được đánh chỉ mục theo peer và mã tin nhắn.
        self._pending: dict[tuple[str, str], Event] = {}
        self._workers: set[Thread] = set()

    def history(self, peer_id: str) -> list[Record]:
        """Trả về lịch sử chat đã lưu với peer."""
        return self.service.store.history(str(peer_id))

    def receive(self, session: Session, header: Header, body: bytes) -> None:
        """Callback TCP: xử lý ACK hoặc một tin nhắn vừa nhận."""
        peer_id = session.peer_id

        if isinstance(header, ChatAck):
            message_id = str(header.message_id)
            key = (peer_id, message_id)
            with self._lock:
                acknowledgement = self._pending.get(key)
                if acknowledgement:
                    acknowledgement.set()
            # ACK chỉ hợp lệ với tin gửi đi đã biết.
            try:
                saved = self.service.store.get_message(message_id)
                if saved and saved["peer_id"] == peer_id and saved["direction"] == "out":
                    updated = self.service.store.set_message_state(message_id, "sent")
                    if updated:
                        self.service._emit("message_updated", message=updated)
            except sqlite3.Error as exc:
                raise StorageError("Không cập nhật được trạng thái ACK") from exc
            return

        if not isinstance(header, ChatMessage):
            raise ProtocolError("Unexpected frame in chat session")
        try:
            text = body.decode("utf-8")
            message_body(text)  # Kiểm tra tin rỗng và giới hạn kích thước.
        except UnicodeError as exc:
            raise ProtocolError("Invalid chat encoding") from exc
        except ProtocolError as exc:
            raise ProtocolError("Invalid chat message") from exc

        # Ghi vào SQLite trước; nếu ghi lỗi thì không gửi ACK để bên kia thử lại.
        try:
            message, is_new = self.service.store.insert_message(
                message_id=str(header.message_id),
                peer_id=peer_id,
                peer_name=session.remote.name,
                text=text,
                direction="in",
                state="received",
                timestamp=time.time(),
            )
        except sqlite3.Error as exc:
            raise StorageError("Không lưu được tin nhắn nhận") from exc
        except MessageIdConflictError as exc:
            raise ProtocolError("Message ID conflicts with an existing message") from exc
        if is_new:
            self.service._emit("message_received", message=message)
        # Tin trùng do gửi lại vẫn được ACK, nhưng không lưu/phát lặp.
        session.send(ChatAck(message_id=header.message_id))

    def send(self, peer_id: str, text: str) -> Record:
        """Lưu và gửi tin; chờ ACK trên worker để không chặn giao diện."""
        peer_id = str(peer_id)
        try:
            body = message_body(text)
        except ProtocolError as exc:
            raise message_error_from_protocol(str(exc)) from exc
        except UnicodeError as exc:
            raise MessageEncodingError() from exc

        session = self.service.sessions.get(peer_id)
        if session is None:
            raise PeerOfflineError()

        message_id = uuid4()
        try:
            message, _ = self.service.store.insert_message(
                message_id=str(message_id),
                peer_id=peer_id,
                peer_name=session.remote.name,
                text=text,
                direction="out",
                state="sending",
                timestamp=time.time(),
            )
        except sqlite3.Error as exc:
            raise StorageError("Không lưu được tin nhắn gửi") from exc
        acknowledgement = Event()
        key = (peer_id, str(message_id))
        with self._lock:
            self._pending[key] = acknowledgement

        self.service._emit("message_updated", message=message)
        # Gửi và đợi xác nhận trong nền để UI luôn phản hồi.
        worker = Thread(
            target=self._send_worker,
            args=(session, key, body, acknowledgement),
            name="chat-send",
            daemon=True,
        )
        with self._lock:
            self._workers.add(worker)
        try:
            worker.start()
        except RuntimeError as exc:
            with self._lock:
                self._workers.discard(worker)
                self._pending.pop(key, None)
            try:
                failed = self.service.store.set_message_state(
                    str(message_id), "failed", WorkerStartError.code
                )
            except sqlite3.Error as storage_exc:
                raise StorageError("Không cập nhật được trạng thái tin nhắn") from storage_exc
            if failed:
                self.service._emit("message_updated", message=failed)
            raise WorkerStartError() from exc
        return message

    def _send_worker(self, session: Session, key: tuple[str, str],
                     body: bytes, acknowledgement: Event) -> None:
        """Gửi một frame và cập nhật trạng thái theo kết quả ACK."""
        peer_id, message_id = key
        try:
            frame = ChatMessage(message_id=UUID(message_id), body_length=len(body))
            session.send(frame, body)

            # Dừng chờ sớm nếu phiên bị đóng hoặc service đang thoát.
            deadline = time.monotonic() + ACK_TIMEOUT
            while not acknowledgement.wait(0.1):
                if session.closed.is_set() or self.service._stop.is_set():
                    break
                if time.monotonic() >= deadline:
                    break

            state = "sent" if acknowledgement.is_set() else "failed"
            error = None if state == "sent" else AckTimeoutError.code
            updated = self.service.store.set_message_state(message_id, state, error)
            if updated:
                self.service._emit("message_updated", message=updated)
        except (OSError, ConnectionError, ProtocolError) as exc:
            log.info("Không gửi được tin nhắn tới %s: %s", peer_id, exc)
            updated = self.service.store.set_message_state(
                message_id, "failed", ConnectionFailedError.code
            )
            if updated:
                self.service._emit("message_updated", message=updated)
        except Exception:
            # Lỗi database/callback nội bộ cũng được phản ánh thành trạng thái lỗi.
            log.exception("Lỗi khi xử lý tin gửi tới %s", peer_id)
            try:
                updated = self.service.store.set_message_state(
                    message_id, "failed", InternalError.code
                )
                if updated:
                    self.service._emit("message_updated", message=updated)
            except Exception:
                log.exception("Không cập nhật được trạng thái tin nhắn")
        finally:
            with self._lock:
                self._pending.pop(key, None)
                self._workers.discard(current_thread())

    def close(self) -> None:
        """Chờ các luồng gửi hoàn tất trước khi đóng SQLite."""
        with self._lock:
            workers = list(self._workers)
        for worker in workers:
            if worker is not current_thread():
                worker.join(timeout=ACK_TIMEOUT + 1)

