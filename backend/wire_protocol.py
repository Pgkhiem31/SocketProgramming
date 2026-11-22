"QuY trình đóng , mở , đọc , đọc , nhận cấc gói"


from __future__ import annotations

import json
"Đọc phần json trong UDP và header TCP -> python"

import socket
import struct
import time
from datetime import datetime , timezone
"Lưu tgian lúc nhận hoặc gửi tin"
from typing import Annotated , Literal
"annotated : gắn quy tắc , Lit : gắn giá trị cho phép"
from uuid import UUID
"Tạo mã riêng cho device tin nhắn , ..... -> phân biệt trùng nhau"

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator
"Kiểm tra"

VERSION = 1
"giúp lọc những thiết bị dùng giao thức tương thích"
Max_header = 8192
"Chọn 8KiB cho headerJSON, thong tin mô tả của frame TCP"
Chunk_Size = 64*1024
"Gửi file , chia file thành các khối 64 "
Max_message = 16*1024
"nội dung tnhan tối đa 16384byte sau mã hóa UTF-8"
IO_TIMEOUT = 6.0
"giúp thoát khỏi hệ thống khi bên gửi một frame TCP rồi dừng"
MAX_DATAGRAM = 2048
"đặt giới hạn 2 KiB cho một gói UDP"
Port = Annotated[int , Field(ge = 1 , le = 65535)]
"cổng nằm từ 1 -> 65355"
PrecenseStatus = Literal["online" , "away"]
Avatar = Literal["teal", "blue", "amber", "rose"]
"Bốn màu cho ava"

def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
"Lấy giờ hiện tại theo UTC . độ chính xác mili giây"

class ProtocolError(ValueError):
    pass

class Identity(BaseModel):
    model_config = ConfigDict(extra="forbid" , strict = True , str_strip_whitespace = True)

    version: Literal[1] = VERSION
    "y/c giá trị = 1 , mặc định khi tạo model"
    peer_id: UUID
    name: str = Field(min_length = 1 , max_length = 40)
    tcp_port: Port
    status: PrecenseStatus
    avatar: Avatar = "teal"

    @field_validator("name")
    @classmethod
    def visible_name(identity_class , value: str)->str:
        if not value.isprintable():
            raise ValueError("Use a printable display name")
        return value

"Gửi gói UDP để báo là đang online hay away"
class Presence(Identity):
    type: Literal["presence"] = "presence"

def encode_presence(presence : Presence) -> bytes:
    return presence.model_dump_json().encode("utf-8")

def decode_presence(packet: bytes) -> Presence:
    if not 0 < len(packet) <= MAX_DATAGRAM:
        raise ProtocolError("Invalid presence size")
    try:
        header = json.loads(packet.decode("utf-8"))
        if (
            not isinstance(header, dict)
            or header.get("type") != "presence"
            or type(header.get("version")) is not int
            or header["version"] != VERSION
        ):
            raise ProtocolError("Incomplete presence")
        return Presence.model_validate_json(packet)
    except (ValueError, RecursionError) as exc:
        raise ProtocolError("Invalid presence") from exc
    "dECODE HEADER -> JSON"
class Frame(BaseModel):
    model_config = ConfigDict(extra = "forbid" , strict = True)
    version: Literal[1] = VERSION
    body_length: Literal[0] = 0

class Hello(Identity):
    type: Literal["hello"] = "hello"
    body_length: Literal[0] = 0
    mode : Literal["chat" , "file"]
    session_id : UUID
    "frame hello được gửi khi A gửi connect TCP "

class ChatMessage(Frame):
    type: Literal["chat_message"] = "chat_message"
    message_id: UUID
    body_length: int = Field(ge=1, le=Max_message)


class ChatAck(Frame):
    type: Literal["chat_ack"] = "chat_ack"
    message_id: UUID


class Bye(Frame):
    type: Literal["bye"] = "bye"


class FileStart(Frame):
    type: Literal["file_start"] = "file_start"
    file_id: UUID
    filename: str = Field(min_length=1, max_length=240)
    size: int = Field(ge=0, le=2**40)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class FileChunk(Frame):
    type: Literal["file_chunk"] = "file_chunk"
    file_id: UUID
    body_length: int = Field(ge=1, le=Chunk_Size)


class FileReady(Frame):
    type: Literal["file_ready"] = "file_ready"
    file_id: UUID


class FileEnd(Frame):
    type: Literal["file_end"] = "file_end"
    file_id: UUID


class FileAck(Frame):
    type: Literal["file_ack"] = "file_ack"
    file_id: UUID


class FileError(Frame):
    type: Literal["file_error"] = "file_error"
    file_id: UUID
    reason: Literal["invalid_filename", "file_limit", "integrity", "storage", "interrupted", "busy"]

Header = Hello | ChatMessage | ChatAck | Bye | FileStart | FileChunk | FileReady | FileEnd | FileAck | FileError
HEADERS = TypeAdapter(Annotated[Header , Field(discriminator="type")])
"Pydantic nhìn vào type để chọn lớp"


def recv_exact(sock : socket.socket , length: int , * , idle : bool = False) -> bytes:
    "Đọc dữ liệu cần đọc trong frame theo số byte , idle dùng để tiếp tục chờ tin nhắn bên kia"
    "Dữ liu gửi đi đc bâm ra thành các segment nhỏ(tùy) nên hàm dùng để đọc những dữ liệu theo segment rồi ghép lại hoàn chỉnh"
    if not 0 <= length <= Chunk_Size:
        raise ProtocolError("Invalid section length")
    result = bytearray()
    deadline = time.monotonic() + IO_TIMEOUT
    original_timeout = sock.gettimeout()
    try:
        while len(result) < length:
            if not idle:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Frame read timed out")
                timeout = remaining
                if original_timeout is not None:
                    timeout = min(original_timeout, remaining)
                sock.settimeout(timeout)
            try:
                block = sock.recv(length - len(result))
            except socket.timeout:
                # Chưa có byte đầu tiên: tiếp tục chờ nếu được phép idle.
                if idle or time.monotonic() < deadline:
                    continue
                raise TimeoutError("Frame read timed out") from None
            if not block:
                raise EOFError("Connection closed during read")
            result.extend(block)
            if idle:
                deadline = time.monotonic() + IO_TIMEOUT
                idle = False
    finally:
        sock.settimeout(original_timeout)
    return bytes(result)

def read_frame(sock : socket.socket , * , idle: bool = False) -> tuple[Header , bytes]:
     (length,) = struct.unpack("!I" , recv_exact(sock , 4 ,idle = idle))
     "Đọc 4 byte đầu để biết heade json dài bnhiu"
     if not 0 < length <= Max_header:
         raise ProtocolError("Invalid header length")
     raw = recv_exact(sock , length)
     try:
         header = json.loads(raw.decode("utf-8"))
         if (
             not isinstance(header, dict)
             or type(header.get("version")) is not int
             or header["version"] != VERSION
             or type(header.get("body_length")) is not int
         ):
             raise ProtocolError("Incomplete header")
         frame = HEADERS.validate_json(raw)
         "tạo đối tường ChatMessage , frame.body_length == 5"
     except (ValueError , RecursionError) as exc:
         raise ProtocolError("Invalid frame header") from exc
     body = recv_exact(sock , frame.body_length)
     return frame , body

def send_frame(sock: socket.socket , frame: Header, body : bytes = b"") -> None:
    if frame.body_length != len(body):
        raise ProtocolError("Payload length dont match header")
    header = frame.model_dump_json().encode("utf8")
    if len(header) > Max_header:
        raise ProtocolError("Header too large")
    sock.sendall(struct.pack("!I", len(header)) + header + body)

def message_body(text : str) -> bytes:
    try:
        body = text.encode("utf-8")
    except UnicodeError as exc:
        raise ProtocolError("message_invalid") from exc
    if not text.strip():
        raise ProtocolError("message_empty")
    if len(body) > Max_message:
        raise ProtocolError("message_limit")
    return body
