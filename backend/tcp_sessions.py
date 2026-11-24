"""TCP transport: one listener, persistent chat sessions, separate file sockets."""
from __future__ import annotations

# Ghi log và sử dụng socket TCP của hệ điều hành.
import logging
import socket
from contextlib import contextmanager
from threading import BoundedSemaphore, Event, Lock, RLock, Thread, current_thread
from typing import Callable, Iterator
from uuid import UUID, uuid4

# Dùng lại giao thức đóng gói frame của nhóm.
from .wire_protocol import (
    Bye, ChatAck, ChatMessage, Header, Hello, Presence, ProtocolError,
    read_frame, send_frame,
)

log = logging.getLogger(__name__)


def close_socket(sock: socket.socket) -> None:
    """Interrupt blocked reads/writes before releasing the descriptor."""
    # Ngắt cả hai chiều để đánh thức luồng đang chờ gửi/nhận.
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    sock.close()  # Giải phóng tài nguyên socket.


# Một phiên TCP chat với một thiết bị khác.
class Session:
    def __init__(self, sock: socket.socket, remote: Hello):
        self.sock = sock
        self.remote = remote
        self.peer_id = str(remote.peer_id)
        self.session_id = remote.session_id
        self._write_lock = Lock()  # Tránh nhiều luồng gửi xen lẫn frame.
        self.closed = Event()  # Cờ báo phiên đã đóng.

    def send(self, frame: Header, body: bytes = b"") -> None:
        # Phiên chat chỉ chấp nhận tin nhắn, ACK và Bye.
        if not isinstance(frame, (ChatMessage, ChatAck, Bye)):
            raise ProtocolError("Invalid chat frame")
        with self._write_lock:
            if self.closed.is_set():
                raise ConnectionError("Session closed")
            try:
                send_frame(self.sock, frame, body)
            except OSError:
                self.close()
                raise

    def close(self) -> None:
        if not self.closed.is_set():
            self.closed.set()
            close_socket(self.sock)

    def leave(self) -> None:
        # Báo cho peer trước khi đóng kết nối.
        try:
            self.send(Bye())
        except OSError:
            pass
        finally:
            self.close()


# Điều phối listener, kết nối chủ động và các phiên TCP.
class TCPManager:
    """Network-only manager. Callbacks run on worker threads, not the GUI thread.

    identity returns the same peer identity used by UDP discovery. For chat,
    on_chat handles only ChatMessage/ChatAck. File handling owns an accepted
    socket only for the duration of on_file; manager closes it afterward.
    """
    def __init__(self, identity: Callable[[], Presence], *,
                 host: str = "0.0.0.0", port: int = 5000,
                 on_chat: Callable[[Session, Header, bytes], None] | None = None,
                 on_file: Callable[[socket.socket, Hello], None] | None = None,
                 on_close: Callable[[Session], None] | None = None,
                 max_workers: int = 32, timeout: float = 6.0):
        # Callback lấy thông tin định danh và xử lý sự kiện từ backend.
        self.identity = identity
        self.host, self.port = host, port
        self.on_chat, self.on_file, self.on_close = on_chat, on_file, on_close
        self.timeout = timeout
        # Bảo vệ dữ liệu dùng chung giữa các luồng.
        self._lock = RLock()
        self._stop = Event()
        self._listener: socket.socket | None = None
        self._threads: set[Thread] = set()
        self._sockets: set[socket.socket] = set()
        self._sessions: dict[str, Session] = {}
        self._slots = BoundedSemaphore(max_workers)  # Giới hạn số kết nối đang xử lý.

    def start(self) -> int:
        # Khởi động server TCP, trả về port thực tế.
        with self._lock:
            if self._stop.is_set():
                raise RuntimeError("TCPManager cannot restart after close")
            if self._listener is not None:
                return self.port
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                # Windows cần giữ độc quyền port, tránh bind trùng.
                if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                    listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
                else:
                    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                # Gắn IP/port, lắng nghe và đặt timeout để kiểm tra cờ dừng.
                listener.bind((self.host, self.port))
                listener.listen(32)
                listener.settimeout(0.25)
                self.port = listener.getsockname()[1]
                self._listener = listener
                self._spawn(self._accept, listener)  # Chờ kết nối ở luồng riêng.
                return self.port
            except BaseException:
                listener.close()
                self._listener = None
                raise

    def _spawn(self, target, *args) -> None:
        # Tạo worker và tự xóa khỏi danh sách khi kết thúc.
        def run():
            try:
                target(*args)
            finally:
                with self._lock:
                    self._threads.discard(current_thread())
        with self._lock:
            if self._stop.is_set():
                raise ConnectionError("TCP manager stopped")
            worker = Thread(target=run, daemon=True, name="lan-tcp")
            self._threads.add(worker)
            try:
                worker.start()
            except BaseException:
                self._threads.discard(worker)
                raise

    # Theo dõi socket để có thể đóng tất cả khi dừng manager.
    def _track(self, sock: socket.socket) -> None:
        with self._lock:
            if self._stop.is_set():
                raise ConnectionError("TCP manager stopped")
            self._sockets.add(sock)

    # Đóng socket và bỏ theo dõi.
    def _release(self, sock: socket.socket) -> None:
        close_socket(sock)
        with self._lock:
            self._sockets.discard(sock)

    def _hello(self, mode: str, session_id: UUID) -> Hello:
        # Tạo Hello ở tầng ứng dụng từ định danh dùng chung với UDP.
        p = self.identity()
        return Hello(peer_id=p.peer_id, name=p.name, avatar=p.avatar,
                     status=p.status, tcp_port=self.port, mode=mode,
                     session_id=session_id)

    def _check_hello(self, frame: Header, *, mode: str | None = None,
                     session_id: UUID | None = None,
                     peer_id: str | None = None) -> Hello:
        # Kiểm tra đúng loại frame, danh tính và thông tin phiên.
        if not isinstance(frame, Hello):
            raise ProtocolError("Expected Hello")
        if frame.peer_id == self.identity().peer_id:
            raise ProtocolError("Self connection")
        if mode is not None and frame.mode != mode:
            raise ProtocolError("Incorrect connection mode")
        if session_id is not None and frame.session_id != session_id:
            raise ProtocolError("Incorrect session ID")
        if peer_id is not None and str(frame.peer_id) != str(peer_id):
            raise ProtocolError("Unexpected peer ID")
        return frame

    def _accept(self, listener: socket.socket) -> None:
        # Vòng lặp nhận kết nối mới từ các peer.
        while not self._stop.is_set():
            try:
                sock, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self._stop.is_set():
                    log.exception("TCP accept failed")
                break
            # Từ chối kết nối mới nếu đã hết slot xử lý.
            if not self._slots.acquire(blocking=False):
                close_socket(sock)
                continue
            try:
                sock.settimeout(self.timeout)
                self._track(sock)
                self._spawn(self._incoming, sock)
            except (OSError, RuntimeError):
                self._release(sock)
                self._slots.release()

    def _incoming(self, sock: socket.socket) -> None:
        try:
            # Frame đầu tiên phải là Hello để xác định chat hay file.
            frame, _ = read_frame(sock)
            hello = self._check_hello(frame)
            if hello.mode == "file" and self.on_file is None:
                raise ProtocolError("File transfers unavailable")
            # Phản hồi Hello với cùng session_id để hoàn tất handshake.
            send_frame(sock, self._hello(hello.mode, hello.session_id))
            # Chat giữ kết nối lâu dài, file chuyển cho callback riêng.
            if hello.mode == "chat":
                session = Session(sock, hello)
                if self._register(session):
                    self._read_chat(session)
            else:
                self.on_file(sock, hello)
        except (OSError, EOFError, ProtocolError):
            if not self._stop.is_set():
                log.debug("Incoming TCP connection ended", exc_info=True)
        except Exception:
            log.exception("TCP callback failed")
        finally:
            self._release(sock)
            self._slots.release()

    def _dial(self, host: str, port: int, mode: str,
              peer_id: str | None) -> tuple[socket.socket, Hello]:
        # Chủ động kết nối TCP đến peer và trao đổi Hello.
        if self._listener is None or self._stop.is_set():
            raise ConnectionError("Start TCP manager first")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._track(sock)
            sock.settimeout(self.timeout)
            # Nếu đã chọn card mạng, dùng IP đó làm địa chỉ nguồn.
            if self.host != "0.0.0.0":
                sock.bind((self.host, 0))
            sock.connect((host, port))  # Hệ điều hành thực hiện TCP handshake.
            hello = self._hello(mode, uuid4())  # Tạo ID riêng cho phiên.
            send_frame(sock, hello)
            response, _ = read_frame(sock)
            remote = self._check_hello(response, mode=mode,
                                       session_id=hello.session_id, peer_id=peer_id)
            return sock, remote
        except BaseException:
            self._release(sock)
            raise

    def _register(self, session: Session) -> bool:
        # Nếu hai máy kết nối cùng lúc, giữ phiên có UUID nhỏ hơn.
        # Hai đầu dùng cùng quy tắc để loại bỏ kết nối trùng.
        with self._lock:
            if self._stop.is_set():
                return False
            old = self._sessions.get(session.peer_id)
            if old is not None and not old.closed.is_set():
                if old.session_id.int <= session.session_id.int:
                    return False
                old.close()
            self._sessions[session.peer_id] = session
            return True

    def get(self, peer_id: str) -> Session | None:
        # Chỉ trả về phiên còn hoạt động.
        with self._lock:
            s = self._sessions.get(str(peer_id))
            return s if s is not None and not s.closed.is_set() else None

    def connect(self, host: str, port: int, peer_id: str | None = None) -> Session:
        # Tái sử dụng phiên chat hiện có nếu còn hoạt động.
        if peer_id is not None:
            existing = self.get(peer_id)
            if existing is not None:
                return existing
        if not self._slots.acquire(blocking=False):
            raise ConnectionError("TCP worker limit reached")
        sock = None
        started = False
        try:
            sock, remote = self._dial(host, port, "chat", peer_id)
            session = Session(sock, remote)
            if not self._register(session):
                existing = self.get(session.peer_id)
                if existing is None:
                    raise ConnectionError("Session rejected")
                return existing
            # Đọc tin nhắn ở background để không chặn luồng gọi connect.
            self._spawn(self._outgoing_chat, session)
            started = True
            return session
        finally:
            # Nếu chưa khởi động worker, tự dọn socket và trả lại slot.
            if not started:
                if sock is not None:
                    self._release(sock)
                    with self._lock:
                        s = self._sessions.get(str(remote.peer_id))
                        if s is not None and s.sock is sock:
                            s.closed.set()
                            del self._sessions[s.peer_id]
                self._slots.release()

    # Worker nhận frame của phiên do máy này chủ động mở.
    def _outgoing_chat(self, session: Session) -> None:
        try:
            self._read_chat(session)
        finally:
            self._release(session.sock)
            self._slots.release()

    def _read_chat(self, session: Session) -> None:
        try:
            while not self._stop.is_set() and not session.closed.is_set():
                # Cho phép chờ tin nhắn mới khi phiên chat đang rảnh.
                frame, body = read_frame(session.sock, idle=True)
                if isinstance(frame, Bye):
                    break
                if not isinstance(frame, (ChatMessage, ChatAck)):
                    raise ProtocolError("Unexpected frame in chat session")
                if self.on_chat is None:
                    raise ProtocolError("Chat handler unavailable")
                self.on_chat(session, frame, body)  # Chuyển frame sang lớp xử lý chat.
        except (OSError, EOFError, ProtocolError):
            if not self._stop.is_set():
                log.debug("Chat session ended", exc_info=True)
        except Exception:
            log.exception("Chat callback failed")
        finally:
            # Dọn phiên đã kết thúc, không xóa nhầm phiên thay thế.
            session.close()
            with self._lock:
                if self._sessions.get(session.peer_id) is session:
                    del self._sessions[session.peer_id]
            if self.on_close:
                try:
                    self.on_close(session)
                except Exception:
                    log.exception("Close callback failed")

    @contextmanager
    def open_file(self, host: str, port: int,
                  peer_id: str | None = None) -> Iterator[tuple[socket.socket, Hello]]:
        # Mở socket file riêng để truyền tệp không chặn phiên chat.
        if not self._slots.acquire(blocking=False):
            raise ConnectionError("TCP worker limit reached")
        sock = None
        try:
            sock, remote = self._dial(host, port, "file", peer_id)
            yield sock, remote  # Bên gọi sử dụng socket trong khối with.
        finally:
            if sock is not None:
                self._release(sock)
            self._slots.release()

    # Ngắt một phiên chat theo ID của peer.
    def leave(self, peer_id: str) -> None:
        session = self.get(peer_id)
        if session is not None:
            session.leave()

    def close(self) -> None:
        # Báo dừng, đóng socket rồi chờ các worker kết thúc.
        with self._lock:
            self._stop.set()
            listener = self._listener
            sockets = list(self._sockets)
            sessions = list(self._sessions.values())
            threads = list(self._threads)
        if listener is not None:
            close_socket(listener)
        for s in sessions:
            s.close()
        for sock in sockets:
            close_socket(sock)
        for worker in threads:
            # Không join chính luồng đang chạy để tránh deadlock.
            if worker is not current_thread():
                worker.join(timeout=self.timeout + 1)
