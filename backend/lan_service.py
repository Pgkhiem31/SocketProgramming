"""Compose discovery, chat sessions, transfers and persistence for one local peer."""

from __future__ import annotations

import errno
import logging
import os
from hashlib import sha256
from pathlib import Path
from threading import RLock
from typing import IO, Protocol
from uuid import UUID

from .chat_messages import Chat
from .app_config import Config, choose_interface, interfaces
from .udp_discovery import Discovery, Peer, Registry
from .app_errors import LanError
from .wire_protocol import Avatar, Identity, Presence, PresenceStatus
from .tcp_sessions import Sessions
from .sqlite_store import Record, Store
from .file_transfers import Transfers, cleanup

log = logging.getLogger(__name__)
BIND_ERRORS = {errno.EADDRINUSE, 10048, 10013}  # Winsock address-in-use and access-denied codes.


# Dịch vụ chỉ cần một nơi nhận sự kiện, nên không phụ thuộc trực tiếp vào Qt.
class EventSink(Protocol):
    def emit(self, kind: str, **payload: object) -> None: ...


def lock_directory(directory: Path) -> IO[bytes]:
    directory.mkdir(parents=True, exist_ok=True)
    # Giữ khóa trong suốt vòng đời dịch vụ để hai phiên không dùng chung thư mục dữ liệu.
    handle = (directory / "instance.lock").open("a+b")
    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        raise RuntimeError("This data directory is already in use by another LANLink instance.") from exc
    return handle


class LanLink:
    def __init__(self, config: Config, events: EventSink):
        self.config = config
        self.events = events
        self.lock = RLock()
        self.network_lock = RLock()
        self.sessions: Sessions | None = None
        self.discovery: Discovery | None = None
        self.ip: str | None = None
        self.broadcast: str | None = None
        self.network_error: str | None = None
        self.directory_lock = lock_directory(config.data_dir)
        try:
            self.store = Store(config.data_dir)
            self._initialize()
        except Exception:
            if hasattr(self, "store"):
                self.store.close()
            self.directory_lock.close()
            raise

    def _initialize(self) -> None:
        config = self.config
        saved = self.store.settings()
        self.peer_id = saved["peer_id"]
        self.name = config.name or saved.get("name", "")
        self.avatar: Avatar = saved.get("avatar", "teal")
        self.status: PresenceStatus = saved.get("status", "online")
        self.theme = saved.get("theme", "dark")
        self.notifications = saved.get("notifications", "yes") == "yes"
        self.interface = config.interface if config.interface is not None else saved.get("interface", "")
        self.active_interface = self.interface
        if config.name:
            self.store.save_settings(name=config.name)
        # Tách file tạm theo thư mục dữ liệu để các phiên dùng chung nơi tải xuống vẫn dọn riêng.
        namespace = sha256(str(config.data_dir.resolve()).encode()).hexdigest()[:16]
        self.partials = config.downloads / ".lanlink-parts" / namespace
        for path in self.partials.glob("*.part"):
            cleanup(path)
        self.registry = Registry(self.peer_id, config.expiry, self._peer_changed)
        self.chat = Chat(self)
        self.transfers = Transfers(self)

    def _peer_changed(self, kind: str, peer: Peer) -> None:
        record = peer.public()
        if kind != "peer_offline":
            self.store.update_known_peer(record)
        self.events.emit(kind, peer=record)

    def identity(self) -> Identity:
        with self.lock:
            return Identity(peer_id=UUID(self.peer_id), name=self.name,
                            tcp_port=self.sessions.port if self.sessions else self.config.tcp_port,
                            avatar=self.avatar, status=self.status)

    def presence(self) -> Presence:
        return Presence(**self.identity().model_dump())

    def profile(self) -> Record:
        with self.lock:
            return {
                "peer_id": self.peer_id, "name": self.name, "status": self.status, "avatar": self.avatar,
                "theme": self.theme, "notifications": self.notifications,
                "interface": self.interface,
            }

    def update(self, values: Record) -> Record:
        with self.network_lock:
            with self.lock:
                profile = {**self.profile(), **values}
                values_to_save = {key: str(value) for key, value in values.items() if key != "notifications"}
                if "notifications" in values:
                    values_to_save["notifications"] = "yes" if values["notifications"] else "no"
                self.store.save_settings(**values_to_save)
                for key, value in values.items():
                    setattr(self, key, value)
            if self.discovery:
                self.discovery.refresh()
            self.events.emit("profile_updated", profile=profile)
            return profile

    def network(self) -> Record:
        with self.network_lock:
            return {
                "running": self.sessions is not None, "ip": self.ip, "broadcast": self.broadcast,
                "directed_discovery": bool(self.config.targets),
                "tcp_port": self.sessions.port if self.sessions else self.config.tcp_port,
                "udp_port": self.config.udp_port, "interfaces": interfaces(), "error": self.network_error,
                "active_interface": self.active_interface, "restart_required": bool(
                    self.sessions and self.active_interface != self.interface),
                "file_limit": self.config.file_limit, "downloads": str(self.config.downloads),
                "protocol": 1, "version": "1.0.0",
            }

    def _announcement_failed(self) -> None:
        self.events.emit("network_error", error="discovery_failed")

    def start_network(self) -> Record:
        with self.network_lock:
            if self.discovery:
                self.discovery.refresh()
                return self.network()
            if not self.name:
                raise LanError("profile_required")
            adapter = choose_interface(self.interface)
            if not adapter:
                self.network_error = "interface_unavailable"
            else:
                self._start_sockets(adapter["ip"], self.config.broadcast or adapter["broadcast"])
            state = self.network()
            self.events.emit("network_changed", network=state)
            return state

    def _start_sockets(self, ip: str, broadcast: str) -> None:
        sessions = None
        discovery = None
        try:
            sessions = Sessions(
                ip, self.config.tcp_port, self.registry, self.identity,
                self.events.emit, self.chat.receive, self.transfers.receive,
            )
            discovery = Discovery(
                self.config, ip, broadcast, self.registry, self.presence, self._announcement_failed,
            )
            # Gán phiên TCP trước khi phát bản tin UDP để quảng bá đúng cổng đã mở.
            with self.lock:
                self.sessions = sessions
                self.discovery = discovery
            self.ip, self.broadcast = ip, broadcast
            self.active_interface = self.interface
            sessions.start()
            discovery.start()
        # Nếu khởi động dở dang, đóng cả những thành phần mạng đã tạo.
        except (OSError, LanError) as exc:
            if discovery:
                discovery.close()
            if sessions:
                sessions.close()
            with self.lock:
                self.sessions = self.discovery = None
            self.ip = self.broadcast = None
            log.warning("Could not start LAN sockets: %s", exc)
            self.network_error = "address_in_use" if getattr(exc, "errno", None) in BIND_ERRORS else "network_failed"
            return

        self.network_error = None
        log.info("%s · %s · UDP %s · TCP %s", self.name, ip, self.config.udp_port, sessions.port)

    def require_sessions(self) -> Sessions:
        with self.lock:
            if not self.sessions:
                raise LanError("network_unavailable")
            return self.sessions

    def connect(self, peer_id: str) -> None:
        self.require_sessions().start_chat(peer_id)

    def leave(self, peer_id: str) -> None:
        self.require_sessions().leave(peer_id)

    def send_message(self, peer_id: str, text: str) -> Record:
        from uuid import uuid4

        return self.chat.send(peer_id, uuid4(), text)

    def send_file(self, peer_id: str, path: Path) -> None:
        peer = self.registry.get(peer_id)
        if peer is None:
            raise LanError("peer_offline")
        if self.require_sessions().is_paused(peer_id):
            raise LanError("chat_paused")
        self.transfers.send(peer, path)

    # Dừng các luồng mạng trước khi đóng SQLite và trả khóa thư mục.
    def close(self) -> None:
        with self.network_lock:
            if self.discovery:
                self.discovery.close()
            if self.sessions:
                self.sessions.close()
            self.store.close()
            self.directory_lock.close()
