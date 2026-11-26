from __future__ import annotations
import hashlib #ccap mã SHA-256 để 2 bên ktra xem dữ liệu gửi và nhận có khớp nhau hay ko
import logging #dùng để ghi lại thông tin cảnh báo và lỗi khi chương trình chạy
import os # ccap những thao tác liên quan đến hệ điều hành và file đang mở
import re
import socket
import sqlite3
import time 
from pathlib import Path , PureWindowsPath
from threading import BoundedSemaphore #giới hạn tổng số lượt gửi và nhận đang xử lý vdu A có 8 slot thì có thể gửi được 8 file cho B nhuwg nếu B chỉ còn 5 slot thì B chỉ nhận được 5 file đầu còn các file còn lại báo busy
from typing import TYPE_CHECKING, BinaryIO, Literal
from uuid import UUID, uuid4
from .udp_discovery import Peer
from .app_errors import LanError
from .wire_protocol import (Chunk_Size, FileAck, FileChunk, FileEnd, FileError, FileReady, FileStart, ProtocolError, now, read_frame, send_frame,)
from .sqlite_store import Record
if TYPE_CHECKING:
    from .lan_service import LanLink

log = logging.getLogger(__name__)
FileReason = Literal["invalid_filename", "file_limit", "integrity", "storage", "interrupted", "busy"]


def safe_filename(name: str) -> str:
    # Kiểm tra tên trước khi dùng làm tên đích, gồm cả ký tự phân cách đường dẫn
    # và tên dành riêng của Windows như CON hay NUL
    if (not name or len(name.encode("utf-8", errors="replace")) > 240 or name in {".", ".."} or
            name != name.strip() or name.endswith(".") or not name.isprintable() or
            re.search(r'[<>:"/\\|?*]', name) or PureWindowsPath(name).is_reserved()):
        raise LanError("invalid_filename")
    return name


def publish_file(part: Path, directory: Path, filename: str) -> str:
    original = Path(filename)
    for number in range(10000):
        # Nếu trùng tên, thử thêm số vào trước phần đuôi, ví dụ report (1).pdf
        name = filename if number == 0 else f"{original.stem} ({number}){original.suffix}"
        candidate = directory / name
        try:
            if os.name == "nt":
                # Windows đổi tên nguyên tử và báo lỗi nếu tên đích đã tồn tại
                part.rename(candidate)
            else:
                # Tạo thêm tên cho file đã xác minh mà không ghi đè tên đích.
                # Khi xóa tên tạm, dữ liệu vẫn tồn tại dưới tên đích.
                os.link(part, candidate)
            return name
        except FileExistsError:
            continue
    raise OSError("Too many files with the same name")


def cleanup(path: Path) -> None:
    try:
        # Sau khi đổi tên trên Windows, đường dẫn tạm có thể đã biến mất
        path.unlink(missing_ok=True)
    except OSError as exc:
        log.warning("Could not clean temporary file %s: %s", path, exc)


def percent(size: int, transferred: int, state: str) -> int:
    if state == "completed":
        return 100
     # Giữ tiến độ tối đa 99% cho đến khi lượt truyền hoàn tất.
    return min(99, (transferred * 100 // size) if size else 0)


class Transfers:
    # Gửi và nhận dùng chung 8 chỗ. Mỗi file giữ một chỗ cho cả lượt truyền
    # BoundedSemaphore báo lỗi nếu trả chỗ làm bộ đếm vượt quá 8
    def __init__(self, service: LanLink):
        self.service = service
        self.slots = BoundedSemaphore(8)

    def reserve(self) -> None:
        if not self.slots.acquire(blocking=False):
            raise LanError("busy") # báo bận nếu hết chỗ

    def send(self, peer: Peer, path: Path) -> None:
        filename = safe_filename(path.name)
        sessions = self.service.require_sessions()
        self.reserve()
        try:
            sessions.workers.launch("file-send", lambda: self._send_local(peer, path, filename))           # Chạy việc đọc và gửi file trong luồng nền.
        except LanError:
            self.slots.release()
            raise

    def _send_local(self, peer: Peer, path: Path, filename: str) -> None:
        try:
            if not path.is_file():
                raise LanError("file_missing")
            with path.open("rb") as source:
                # Lấy kích thước từ chính file đang mở và kiểm tra giới hạn gửi.
                size = os.fstat(source.fileno()).st_size
                if size > self.service.config.file_limit:
                    raise LanError("file_limit")
                # Dùng cùng mã này cho mọi thông điệp của lượt truyền.
                file_id = uuid4()
                row = self._record(peer.public(), file_id, filename, size, "out")
                self._send(peer, file_id, source, row)
        except LanError as exc:
            self.service.events.emit("action_error", error=exc.code)
        except OSError as exc:
            log.warning("Cannot read the selected file: %s", exc)
            self.service.events.emit("action_error", error="file_unreadable")
        finally:
            # Trả chỗ sau khi luồng gửi kết thúc, kể cả khi đọc hoặc truyền lỗi.
            self.slots.release()

    def _record(self, peer: Record, transfer_id: UUID, filename: str, size: int, direction: str) -> Record:
        row = {
            "transfer_id": str(transfer_id), "peer_id": peer["peer_id"], "direction": direction,
            "filename": filename, "size": size, "transferred": 0, "state": "waiting",
            "timestamp": now(), "error": None, "saved_name": None,
        }
        try:
            self.service.store.add_transfer(peer, row)
        except sqlite3.IntegrityError as exc:
            raise LanError("busy") from exc
        self._notify(row)
        return row

    def _update(self, row: Record, **changes: object) -> None:
        row.update(changes)
        self.service.store.update_transfer(row)
        self._notify(row)

    def _notify(self, row: Record) -> None:
        # Thêm phần trăm vào bản sao gửi cho giao diện, giữ nguyên bản ghi lưu trữ.
        snapshot = dict(row)
        snapshot["progress"] = percent(row["size"], row["transferred"], row["state"])
        self.service.events.emit("transfer_updated", transfer=snapshot)

    def _send(self, peer: Peer, file_id: UUID, source: BinaryIO, row: Record) -> None:
        sessions = self.service.require_sessions()
        sock = None
        try:
            # Tính SHA-256 trước vì FileStart cần gửi mã băm cho máy nhận.
            digest = hashlib.sha256()
            remaining = row["size"]
            while remaining:
                if sessions.workers.stop.is_set():
                    raise LanError("interrupted")
                chunk = source.read(min(Chunk_Size, remaining))
                if not chunk:
                    raise LanError("file_unreadable")
                digest.update(chunk)
                remaining -= len(chunk)
            # Quay về đầu file để đọc lại nội dung trong lượt gửi.
            source.seek(0)

            sock, _, _ = sessions.dial(peer, "file")
            size = row["size"]
            send_frame(sock, FileStart(file_id=file_id, filename=row["filename"], size=size, sha256=digest.hexdigest()))
            # Chờ máy nhận mở được file tạm rồi mới gửi nội dung.
            self._expect(sock, file_id, FileReady)
            self._update(row, state="transferring")
            transferred = 0
            next_update = 0.0
            # Đọc và gửi từng khối tối đa Chunk_Size byte (64 KiB).
            while transferred < size:
                chunk = source.read(min(Chunk_Size, size - transferred))
                if not chunk:
                    raise LanError("file_unreadable")
                send_frame(sock, FileChunk(file_id=file_id, body_length=len(chunk)), chunk)
                transferred += len(chunk)
                # Giãn các lần ghi tiến độ định kỳ để giảm tần suất ghi SQLite.
                if time.monotonic() >= next_update:
                    self._update(row, transferred=transferred)
                    next_update = time.monotonic() + 0.15
            send_frame(sock, FileEnd(file_id=file_id))
            self._update(row, transferred=transferred, state="verifying")
            # Chỉ báo hoàn tất ở máy gửi sau khi máy nhận xác nhận đã lưu file.
            self._expect(sock, file_id, FileAck)
            self._update(row, state="completed")
        except LanError as exc:
            self._update(row, state="failed", error=exc.code)
        except (OSError, EOFError, ProtocolError) as exc:
            log.info("File send failed for %s: %s", file_id, exc)
            self._update(row, state="failed", error="interrupted")
        finally:
            if sock:
                sessions.workers.release(sock)

    @staticmethod
    def _expect(sock: socket.socket, file_id: UUID, expected: type[FileAck] | type[FileReady]) -> None:
        header, _ = read_frame(sock)
        # Chỉ xử lý phản hồi thuộc đúng mã lượt truyền đang chờ.
        if isinstance(header, FileError) and header.file_id == file_id:
            raise LanError(header.reason)
        if not isinstance(header, expected) or header.file_id != file_id:
            raise ProtocolError("Unexpected transfer response")

    def receive(self, sock: socket.socket, peer: Record) -> None:
        header, _ = read_frame(sock)
        if not isinstance(header, FileStart):
            raise ProtocolError("Expected file metadata")
        row = None
        part = None
        acquired = False
        reason: FileReason = "interrupted"
        try:
            self.reserve()
            # Ghi nhận đã lấy chỗ để khối finally chỉ trả chỗ khi cần.
            acquired = True
            filename = safe_filename(header.filename)
            if header.size > self.service.config.file_limit:
                raise LanError("file_limit")
            row = self._record(peer, header.file_id, filename, header.size, "in")
            directory = self.service.config.downloads
            # Ứng dụng dùng thư mục tạm riêng trên cùng ổ với thư mục tải xuống.
            partials = self.service.partials
            partials.mkdir(parents=True, exist_ok=True)
            part = partials / f"{uuid4().hex}.part"
            digest = hashlib.sha256()
            # Chế độ xb chỉ tạo file mới. Mở được file tạm rồi mới gửi FileReady.
            with part.open("xb") as destination:
                send_frame(sock, FileReady(file_id=header.file_id))
                self._update(row, state="transferring")
                self._receive_body(sock, header, destination, digest, row)
                # Đẩy dữ liệu từ bộ đệm Python sang hệ điều hành rồi đồng bộ xuống đĩa.
                destination.flush()
                os.fsync(destination.fileno())
            self._update(row, state="verifying", transferred=header.size)
            # Đủ số byte vẫn cần so SHA-256 để kiểm tra nội dung trước khi lưu.
            if digest.hexdigest() != header.sha256:
                raise LanError("integrity")
            saved_name = publish_file(part, directory, filename)
            self._update(row, state="completed", saved_name=saved_name)
            # Xác nhận với máy gửi sau khi file đã có tên chính thức.
            send_frame(sock, FileAck(file_id=header.file_id))
            return
        except LanError as exc:
            if exc.code in ("invalid_filename", "file_limit", "integrity", "busy"):
                reason = exc.code
        except (ConnectionError, TimeoutError, EOFError, ProtocolError) as exc:
            log.info("File receive interrupted for %s: %s", header.file_id, exc)
        except OSError as exc:
            log.warning("Cannot save file %s: %s", header.file_id, exc)
            reason = "storage"
        finally:
            # Dọn tên tạm dù thành công hay thất bại, rồi trả chỗ nếu đã lấy.
            if part:
                cleanup(part)
            if acquired:
                self.slots.release()
        # Nếu đã lưu thành công mà gửi ACK lỗi, phía nhận vẫn giữ completed.
        if row and row["state"] != "completed":
            self._update(row, state="failed", error=reason)
        try:
            send_frame(sock, FileError(file_id=header.file_id, reason=reason))
        except OSError:
            log.debug("Transfer %s ended before an error could be returned", header.file_id)

    def _receive_body(self, sock: socket.socket, start: FileStart, destination, digest, row: Record) -> None:
        transferred = 0
        next_update = 0.0
        while True:
            header, body = read_frame(sock)
            # Chỉ nhận khối dữ liệu hoặc tín hiệu kết thúc của đúng lượt truyền.
            if not isinstance(header, (FileChunk, FileEnd)) or header.file_id != start.file_id:
                raise ProtocolError("Unexpected transfer frame")
            if isinstance(header, FileEnd):
                # FileEnd chỉ hợp lệ khi đã nhận đủ số byte khai báo.
                if transferred != start.size:
                    raise LanError("integrity")
                return
            transferred += len(body)
            # Chặn khối khiến tổng số byte vượt kích thước trước khi ghi xuống đĩa.
            if transferred > start.size:
                raise LanError("integrity")
            destination.write(body)
            # Tính SHA-256 ngay khi nhận để khỏi phải đọc lại file tạm.
            digest.update(body)
            if time.monotonic() >= next_update:
                self._update(row, transferred=transferred)
                next_update = time.monotonic() + 0.15
