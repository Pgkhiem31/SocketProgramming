"""Điều phối UDP, TCP, SQLite và các dịch vụ chat/truyền tệp."""
from __future__ import annotations

import logging
import socket
from pathlib import Path
from queue import Empty, SimpleQueue
from threading import RLock
from typing import Callable, Protocol
from uuid import UUID

from .app_config import Config, choose_interface
from .app_errors import LanError
from .sqlite_store import Record, SQLiteStore
from .tcp_sessions import Session, TCPManager
from .udp_discovery import Discovery, Peer, Registry
from .wire_protocol import Header, Hello, Presence, ProtocolError

log = logging.getLogger(__name__)
EventSink = Callable[[str, Record], None]


# Hợp đồng tích hợp đề xuất; adapter của đồng đội phải tuân theo.
class ChatAdapter(Protocol):
    def send(self, session: Session, text: str) -> str: ...
    def handle(self, session: Session, frame: Header, body: bytes) -> None: ...
    def session_closed(self, session: Session) -> None: ...


class FileAdapter(Protocol):
    def send(self, manager: TCPManager, host: str, port: int,
             path: Path, peer_id: str) -> str: ...
    def receive(self, sock: socket.socket, hello: Hello) -> None: ...


class LanService:
    def __init__(self, config: Config, *,
                 chat_factory: Callable[[SQLiteStore, EventSink], ChatAdapter] | None = None,
                 file_factory: Callable[[Config, SQLiteStore, EventSink], FileAdapter] | None = None):
        self.config = config
        self.store = SQLiteStore(config.data_dir)
        self._profile_lock = RLock()
        self._profile = self.store.get_profile(config.name or "User")
        self._lifecycle = RLock()
        # Worker chỉ đưa event vào hàng đợi, không gọi trực tiếp widget Qt.
        self._events: SimpleQueue[Record] = SimpleQueue()
        self._tcp: TCPManager | None = None
        self._registry: Registry | None = None
        self._discovery: Discovery | None = None
        self._started = False
        self._chat = chat_factory(self.store, self._emit) if chat_factory else None
        self._file = file_factory(config, self.store, self._emit) if file_factory else None

    def _emit(self, event: str, data: Record) -> None:
        self._events.put({"event": event, "data": dict(data)})

    def drain_events(self, limit: int = 100) -> list[Record]:
        # GUI có thể gọi định kỳ bằng QTimer để lấy sự kiện trên main thread.
        if not 1 <= limit <= 1000:
            raise ValueError("invalid_limit")
        result = []
        for _ in range(limit):
            try:
                result.append(self._events.get_nowait())
            except Empty:
                break
        return result

    def get_profile(self) -> Record:
        with self._profile_lock:
            return dict(self._profile)

    def _presence(self) -> Presence:
        profile = self.get_profile()
        tcp = self._tcp
        port = tcp.port if tcp else self.config.tcp_port
        return Presence(peer_id=UUID(profile["peer_id"]), name=profile["name"],
                        avatar=profile["avatar"], status=profile["status"], tcp_port=port)

    @staticmethod
    def _peer_record(peer: Peer) -> Record:
        # Workaround: Peer.public()/to_dict() hiện chưa dùng được trên main.
        return {"peer_id": peer.peer_id, "name": peer.name, "avatar": peer.avatar,
                "ip": peer.ip, "tcp_port": int(peer.tcp_port),
                "status": peer.status, "last_seen": peer.last_seen}

    def _peer_changed(self, event: str, peer: Peer) -> None:
        self._emit(event, self._peer_record(peer))

    def start(self) -> int:
        with self._lifecycle:
            if self._started:
                return self._tcp.port
            try:
                interface = choose_interface(self.config.interface or "")
            except ValueError as exc:
                raise LanError("invalid_interface") from exc
            if interface is None:
                raise LanError("network_unavailable", "Không tìm thấy card mạng IPv4.")
            # TCP phải chạy trước để UDP quảng bá đúng port thực tế.
            self._tcp = TCPManager(self._presence, host=interface["ip"],
                                   port=self.config.tcp_port,
                                   on_chat=self._on_chat,
                                   on_file=self._on_file if self._file else None,
                                   on_close=self._on_close)
            self._registry = Registry(self.get_profile()["peer_id"], self.config.expiry,
                                      self._peer_changed)
            try:
                port = self._tcp.start()
                self._discovery = Discovery(self.config, interface["ip"], interface["broadcast"],
                                            self._registry, self._presence,
                                            lambda: self._emit("network_error", {"code": "udp_send_failed"}))
                self._discovery.start()
            except Exception as exc:
                self._shutdown()
                raise LanError("network_start_failed", str(exc)) from exc
            self._started = True
            self._emit("network_started", {"ip": interface["ip"], "tcp_port": port})
            return port

    def _shutdown(self) -> None:
        # Dừng UDP trước để không quảng bá port TCP đã đóng.
        for service in (self._discovery, self._tcp):
            if service is not None:
                try:
                    service.close()
                except Exception:
                    log.exception("Network cleanup failed")
        self._discovery = self._tcp = self._registry = None
        self._started = False

    def close(self) -> None:
        with self._lifecycle:
            if self._started:
                self._shutdown()
                self._emit("network_stopped", {})

    def _network(self) -> tuple[TCPManager, Registry]:
        with self._lifecycle:
            if not self._started or self._tcp is None or self._registry is None:
                raise LanError("network_not_started")
            return self._tcp, self._registry

    def get_peers(self) -> list[Record]:
        _, registry = self._network()
        # Dùng cùng lock với Registry để tránh đọc khi danh sách đang thay đổi.
        with registry.lock:
            peers = [self._peer_record(p) for p in registry.peers.values()]
        for peer in peers:
            peer["unread_count"] = self.store.unread_count(peer["peer_id"])
        return sorted(peers, key=lambda p: (p["name"].casefold(), p["peer_id"]))

    @staticmethod
    def _find_peer(registry: Registry, peer_id: str) -> Peer:
        peer = registry.get(str(peer_id))
        if peer is None:
            raise LanError("peer_offline", "Không tìm thấy thiết bị đang hoạt động.")
        return peer

    def connect(self, peer_id: str) -> Session:
        tcp, registry = self._network()
        peer_id = str(peer_id)
        if peer_id == self.get_profile()["peer_id"]:
            raise LanError("self_connection")
        existing = tcp.get(peer_id)
        if existing is not None:
            return existing
        peer = self._find_peer(registry, peer_id)
        try:
            session = tcp.connect(peer.ip, int(peer.tcp_port), peer_id)
        except (OSError, EOFError, ProtocolError) as exc:
            raise LanError("connection_failed", str(exc)) from exc
        self._emit("session_connected", {"peer_id": peer_id, "session_id": str(session.session_id)})
        return session

    def leave(self, peer_id: str) -> None:
        tcp, _ = self._network()
        tcp.leave(str(peer_id))

    def send_message(self, peer_id: str, text: str) -> str:
        tcp, _ = self._network()
        if self._chat is None:
            raise LanError("chat_unavailable")
        # Không tự reconnect sau khi người dùng đã bấm ngắt kết nối.
        session = tcp.get(str(peer_id))
        if session is None:
            raise LanError("not_connected")
        try:
            return self._chat.send(session, text)
        except LanError:
            raise
        except (OSError, EOFError, ProtocolError) as exc:
            raise LanError("message_failed", str(exc)) from exc

    def send_file(self, peer_id: str, path: str | Path) -> str:
        tcp, registry = self._network()
        if self._file is None:
            raise LanError("file_unavailable")
        peer = self._find_peer(registry, str(peer_id))
        try:
            # File dùng kết nối riêng, không yêu cầu phiên chat đang mở.
            return self._file.send(tcp, peer.ip, int(peer.tcp_port), Path(path), str(peer_id))
        except LanError:
            raise
        except Exception as exc:
            raise LanError("file_failed", str(exc)) from exc

    def _on_chat(self, session: Session, frame: Header, body: bytes) -> None:
        if self._chat is None:
            self._emit("network_error", {"code": "chat_unavailable", "peer_id": session.peer_id})
            raise ProtocolError("Chat handler unavailable")
        self._chat.handle(session, frame, body)

    def _on_file(self, sock: socket.socket, hello: Hello) -> None:
        if self._file is not None:
            self._file.receive(sock, hello)

    def _on_close(self, session: Session) -> None:
        try:
            if self._chat is not None:
                self._chat.session_closed(session)
        finally:
            # Kèm session ID để GUI không nhầm phiên cũ với phiên thay thế.
            self._emit("session_closed", {"peer_id": session.peer_id,
                                          "session_id": str(session.session_id)})

    def update_profile(self, name: str, avatar: str = "teal", status: str = "online") -> Record:
        with self._profile_lock:
            self._profile = self.store.update_profile(name, avatar, status)
            result = dict(self._profile)
        with self._lifecycle:
            if self._discovery is not None:
                self._discovery.refresh()
        self._emit("profile_updated", result)
        return result

    def get_history(self, peer_id: str, limit: int = 100, *, before_seq: int | None = None) -> list[Record]:
        return self.store.get_history(peer_id, limit, before_seq=before_seq)

    def mark_read(self, peer_id: str, through_seq: int | None = None) -> int:
        seq = self.store.mark_read(peer_id, through_seq)
        self._emit("unread_changed", {"peer_id": str(peer_id), "unread_count": self.store.unread_count(peer_id)})
        return seq

    def get_transfers(self, peer_id: str, limit: int = 100) -> list[Record]:
        return self.store.get_transfers(peer_id, limit)
