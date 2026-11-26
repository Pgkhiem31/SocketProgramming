#file này sau khi UPD tìm thấy trog cùng 1 mạng , giúp mở hoặc nhận kết nối cho chat gửi file ,,..

from __future__ import annotations
import logging
import socket
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Event, Lock, RLock, Thread
from typing import Literal
from uuid import UUID, uuid4
from .udp_discovery import Peer, Registry
from .app_errors import LanError
from .wire_protocol import Bye, Header, Hello, Identity, IO_TIMEOUT, ProtocolError, read_frame, send_frame
from .sqlite_store import Record

log = logging.getLogger(__name__)
SocketErrors = (OSError, EOFError, ProtocolError)

def close_socket(sock : socket.socket ) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR) #kết thúc từ 2 phía , vdu A dag chat với B mà B đang im lặng ko nhắn j , A tắt ứng dụng hàm close_socket được gọi để kết thúc chat và giải phóng socket
    except OSError:
        #nghĩa là khi A vừa mở ứng dụng và tắt ngay thì lúc đó đã mở socket lắng nghe để chờ kết nối đến 
        # shutdown(SHUT_RDWR) có thể báo OSError vì socket này chỉ chờ accept
        #vậy pass để xuống bước tiếp theo để dọn socket lắng nghe và giải phóng tài nguyên
        pass
    sock.close()

class Workers:
    def __init__(self , limit: int = 100):
        self.lock = RLock() #trong cùng 1 luồng đag gọi một hàm khác cũng cần khóa đó thì rlock cho phép lấy thêm khóa đó lần nữa
        self.stop = Event() #tạo cờ báo dừng
        self.limit = limit
        self.threads : set[Thread] = set() # theo dõi các worker(các luồng xử lí) nếu 1 luồng kthucs nó sẽ tự xóa khỏi set
        self.sockets: set[socket.socket] = set() #theo dõi các socket đã đăng ký gồm cả socket lắng nghe và socket kết nối
    def own(self , sock : socket.socket) -> None: #đky một socket đã đc tạo và để Wokers theo dõi và đóng khi cần
        with self.lock:
            if self.stop.is_set():
                close_socket(sock)
                raise LanError("interrupted")
            self.sockets.add(sock)
    def release(self , sock: socket.socket) -> None:
        #vdu A bắm ngắt chat vs B ứng dụng gọi frame "Bye" rồi gọi release để xóa B khỏi sockets
        close_socket(sock) # dừng gửi nhận rồi giải phóng socket
        with self.lock:
            self.sockets.discard(sock)
    def launch(self, name: str, job: Callable[[], None]) -> None:
        #tạo và chạy luồng nền để xử lí cviec, vdu A đag lắng nghe kết nối hoặc là chwof tnhan của B thì khi đó giao socket cho woker rồi quay lại hàm acpt để chờ các kết nối của người khác 
        def run() -> None:
            try:
                job()
            except Exception:
                log.exception("Unexpected failure in %s", name)
            finally:
                with self.lock:
                    self.threads.remove(thread)

        thread = Thread(name=name, target=run, daemon=True)
        with self.lock:
            if self.stop.is_set():
                raise LanError("interrupted")
            if len(self.threads) >= self.limit:
                raise LanError("busy")
            self.threads.add(thread)
            thread.start()
    def close(self) -> None:
        with self.lock: #giữ khóa
            self.stop.set() #bật cờ để launch từ chối khởi chạy các worker mới
            sockets = list(self.sockets) #sao chép các sockets đag đc theo dõi
            threads = list(self.threads) # ---- luồng đag đc theo dõi
        for sock in sockets:
            close_socket(sock)
        for thread in threads:
            thread.join() #Nếu worker còn chạy, luồng gọi join() sẽ chờ. Nếu worker đã kết thúc, join() trả về ngay. join() chỉ chờ, nên worker vẫn phải tự kết thúc công việc của nó
@dataclass
class Session: # quản lý từng người chat
    peer: Record #ttin máy bên kia
    sock : socket.socket #socket tcp dùng để gửi và nhận của chat
    rank : tuple[bool, int]
    sending: Lock = field(default_factory=Lock)
    closed: Event = field(default_factory=Event)

    def send(self, header: Header, body: bytes = b"") -> None:
        with self.sending:
            if self.closed.is_set() and not isinstance(header, Bye):
                raise ConnectionError("Chat session has closed")
            send_frame(self.sock, header, body)

class Sessions:
    def __init__(self, ip: str, port: int, registry: Registry, identity: Callable[[], Identity],notify: Callable[..., None], chat_frame: Callable[[Session, Header, bytes], None],receive_file: Callable[[socket.socket, Record], None]):
        self.registry = registry #nơi tra cứu các máy đã phát hiện trong mạng LAN
        self.ip = ip #IP của máy
        self.identity = identity #lấy ttin nhận dạng của máy
        self.notify = notify #tbao các sự kiện xảy ra , ví dụ A kết nối với B thành công gọi notify() là connect đại loại v
        self.chat_frame = chat_frame #xử lí framew nhận đc
        self.receive_file = receive_file # nhận file
        self.workers = Workers()
        self.lock = RLock()
        self.active: dict[str, Session] = {} #Tạo bảng rỗng lưu phiên chat đang hoạt động. Khóa là peer_id dạng chuỗi, giá trị là đối tượng Session chứa socket chat
        self.states: dict[str, str] = {} #Tạo bảng rỗng lưu trạng thái theo từng người
        self.dial_locks = tuple(Lock() for _ in range(100)) 
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)  #yêu cầu socket chiếm quyền sử dụng địa chỉ/port một cách độc quyền, ngăn socket khác bind trùng
            else:
                self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # cho phép chia sẻ hoặc tái sử dụng địa chỉ/port trong một số điều kiện
            self.listener.bind((ip, port))
            self.listener.listen(32) #32 là kích thước hàng chờ kết nối đến
            self.listener.settimeout(0.25) # đợi nghe đúng 0,25s nếu kco kết nối đến thì ktra cờ nếu chưa bật thì gọi connect và chờ tiếp
            self.workers.own(self.listener) # đăng ký socket lắng nghe
        except (OSError, LanError):
            self.listener.close()
            raise
        self.port: int = self.listener.getsockname()[1]

    def start(self) -> None:
        self.workers.launch("tcp-accept", self._accept)

    @staticmethod
    def _configure(sock: socket.socket) -> None:
        sock.settimeout(IO_TIMEOUT)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, "SIO_KEEPALIVE_VALS"):
            sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, 15000, 3000))
        elif hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 15)
            if hasattr(socket, "TCP_KEEPINTVL"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 3)
            if hasattr(socket, "TCP_KEEPCNT"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)

    def _accept(self) -> None:
        while not self.workers.stop.is_set():
            try:
                sock, address = self.listener.accept()
            except socket.timeout:
                continue
            except OSError as exc:
                if not self.workers.stop.is_set():
                    log.warning("TCP accept failed: %s", exc)
                    self.workers.stop.wait(0.2)
                continue
            try:
                self.workers.own(sock)
                self._configure(sock)
                self.workers.launch("tcp-incoming", lambda s=sock, ip=address[0]: self._incoming(s, ip))
            except (OSError, LanError) as exc:
                log.debug("TCP accept could not assign a worker: %s", exc)
                self.workers.release(sock)

    def _hello(self, mode: Literal["chat", "file"], session_id: UUID) -> Hello:
        return Hello(**self.identity().model_dump(), mode=mode, session_id=session_id)

    def _verify(self, hello: Header, ip: str, expected: Peer | None = None) -> tuple[Hello, Record]:
        if not isinstance(hello, Hello):
            raise ProtocolError("Expected identity handshake")
        peer_id = str(hello.peer_id)
        discovered = expected or self.registry.get(peer_id)
        if (peer_id == self.registry.own_id or
                (discovered and (discovered.peer_id, discovered.ip, discovered.tcp_port) !=
                 (peer_id, ip, hello.tcp_port))):
            raise ProtocolError("Conflicting identity or endpoint")
        return hello, {
            "peer_id": peer_id, "name": hello.name, "ip": ip, "tcp_port": hello.tcp_port,
            "avatar": hello.avatar, "status": hello.status,
        }

    def _incoming(self, sock: socket.socket, ip: str) -> None:
        try:
            hello, peer = self._verify(read_frame(sock)[0], ip)
            if hello.mode == "chat" and self.is_paused(peer["peer_id"]):
                return
            send_frame(sock, self._hello(hello.mode, hello.session_id))
            if hello.mode == "file":
                self.receive_file(sock, peer)
                return
            session = self._register(peer, sock, peer["peer_id"], hello.session_id)
            if session:
                self._read(session)
        except (OSError, EOFError, ProtocolError, LanError) as exc:
            log.debug("Incoming TCP connection ended: %s", exc)
        finally:
            self.workers.release(sock)

    def dial(self, peer: Peer, mode: Literal["chat", "file"]) -> tuple[socket.socket, Record, UUID]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.workers.own(sock)
        session_id = uuid4()
        try:
            self._configure(sock)
            sock.bind((self.ip, 0))
            sock.connect((peer.ip, peer.tcp_port))
            send_frame(sock, self._hello(mode, session_id))
            reply, remote = self._verify(read_frame(sock)[0], peer.ip, peer)
            if (reply.mode, reply.session_id) != (mode, session_id):
                raise ProtocolError("Handshake does not match this connection")
            return sock, remote, session_id
        except SocketErrors:
            self.workers.release(sock)
            raise

    def _set_state(self, peer_id: str, state: str, error: str | None = None) -> None:
        with self.lock:
            self.states[peer_id] = state
            self.notify("connection_changed", peer_id=peer_id, state=state, error=error)

    def get_active(self, peer_id: str) -> Session | None:
        with self.lock:
            return self.active.get(peer_id)

    def is_paused(self, peer_id: str) -> bool:
        with self.lock:
            return self.states.get(peer_id) == "paused"

    def start_chat(self, peer_id: str) -> None:
        def open_chat() -> None:
            try:
                self.connect(peer_id)
            except LanError as exc:
                if exc.code == "connection_failed":
                    return  # connect() has already published the dial failure.
                with self.lock:
                    if peer_id not in self.active and not self.is_paused(peer_id):
                        self._set_state(peer_id, "error", exc.code)

        with self.lock:
            self.workers.launch("tcp-connect", open_chat)
            if self.is_paused(peer_id):
                self._set_state(peer_id, "connecting")

    def _register(self, peer: Record, sock: socket.socket, initiator: str, session_id: UUID) -> Session | None:
        peer_id = peer["peer_id"]
        rank = (initiator == min(self.registry.own_id, peer_id), session_id.int)
        session = Session(peer, sock, rank)
        with self.lock:
            if self.is_paused(peer_id):
                return None
            previous = self.active.get(peer_id)
            if previous and previous.rank >= rank:
                return None
            self.active[peer_id] = session
            self._set_state(peer_id, "connected")
        if previous:
            self.retire(previous)
        return session

    def retire(self, session: Session) -> None:
        peer_id = session.peer["peer_id"]
        with self.lock:
            session.closed.set()
            if self.active.get(peer_id) is session:
                del self.active[peer_id]
                self._set_state(peer_id, "disconnected")
        self.workers.release(session.sock)

    def _read(self, session: Session) -> None:
        try:
            while not self.workers.stop.is_set() and not session.closed.is_set():
                header, body = read_frame(session.sock, idle=True)
                if session.closed.is_set() or isinstance(header, Bye):
                    break
                self.chat_frame(session, header, body)
        except (OSError, EOFError, ProtocolError, LanError) as exc:
            log.debug("Chat session with %s closed: %s", session.peer["peer_id"], exc)
        finally:
            self.retire(session)

    def connect(self, peer_id: str) -> Session:
        with self.dial_locks[hash(peer_id) % len(self.dial_locks)]:
            peer = self.registry.get(peer_id)
            with self.lock:
                if self.is_paused(peer_id):
                    raise LanError("chat_paused")
                existing = self.active.get(peer_id)
                if existing:
                    return existing
                if not peer:
                    raise LanError("peer_offline")
                self._set_state(peer_id, "connecting")
            try:
                sock, remote, session_id = self.dial(peer, "chat")
                session = self._register(remote, sock, self.registry.own_id, session_id)
                if session:
                    try:
                        self.workers.launch("tcp-chat", lambda: self._read(session))
                    except LanError:
                        self.retire(session)
                        raise
                    return session
                self.workers.release(sock)
            except SocketErrors as exc:
                log.info("Connection to %s failed: %s", peer_id, exc)
                with self.lock:
                    if self.is_paused(peer_id):
                        raise LanError("chat_paused") from exc
                    if peer_id not in self.active:
                        self._set_state(peer_id, "error", "connection_failed")
                        raise LanError("connection_failed") from exc
            with self.lock:
                if self.is_paused(peer_id):
                    raise LanError("chat_paused")
                existing = self.active.get(peer_id)
                if not existing:
                    raise LanError("connection_failed")
                return existing

    def leave(self, peer_id: str) -> None:
        with self.lock:
            session = self.active.pop(peer_id, None)
            if session:
                session.closed.set()
            self._set_state(peer_id, "paused")
        if session:
            try:
                session.send(Bye())
            except SocketErrors:
                log.debug("Chat already closed while sending BYE to %s", peer_id)
            finally:
                self.workers.release(session.sock)

    def snapshot(self) -> dict[str, str]:
        with self.lock:
            return self.states.copy()

    def close(self) -> None:
        self.workers.close()