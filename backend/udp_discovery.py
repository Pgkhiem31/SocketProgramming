"giúp chúng mình tìm thấy nhau trog LAN , phát tín hiệu on/away"
from __future__ import annotations
import socket
import logging
import time
from collections.abc import Callable #mô tả tham số là một hàm mà lớp có thể gọi lại
from dataclasses import dataclass , replace 
from threading import Event #đồng bộ và báo hiệu giữa các thread
from threading import RLock #khóa đồng bộ có thể được cùng thread lấy nhiều lần
from threading import Thread #chạy nhiều tác vụ đồng thời
from .app_config import Config
from .sqlite_store import Record
from .wire_protocol import MAX_DATAGRAM , Presence , ProtocolError , decode_presence , encode_presence , now

log = logging.getLogger(__name__) #giúp truy đc lỗi xuất phát từ code nào 

@dataclass(frozen=True) #cấm gán lại
class Peer:
    peer_id : str
    name : str
    avatar : str
    ip : str
    tcp_port: int
    status : str
    last_seen : str
    seen_at: float

    @classmethod
    def seen(cls , presence : Presence , ip : str , seen_at : float) -> Peer:
        return cls(peer_id = str(presence.peer_id) , name = presence.name, avatar = presence.avatar , ip = ip , tcp_port = presence.tcp_port , status = presence.status , last_seen = now() , seen_at = seen_at,)

    def public(self) -> Record:
        return {key: value for key, value in vars(self).items() if key != "seen_at"}

    def to_dict(self) -> Record:
        return self.public()


class Registry: #qli cac thiet bi hoat dong trong he thong
    def __init__(self, own_id: str, expiry: float, changed: Callable[[str, Peer], None]):
        self.own_id = own_id
        self.expiry = expiry
        self.changed = changed
        self.lock = RLock()
        self.peers: dict[str, Peer] = {}

    def ingest(self, packet: bytes, ip: str, seen_at: float | None = None) -> None:
        try:
            presence = decode_presence(packet)
        except ProtocolError:
            return
        if str(presence.peer_id) == self.own_id:
            return
        if seen_at is None:
            timetmp = time.monotonic()
        else:
            timetmp = seen_at
        peer = Peer.seen(presence, ip, timetmp) #tạo một peer mới mới được xuất hiện trog hệ thống

        with self.lock:
            old = self.peers.get(peer.peer_id)

            if old and (peer.ip , peer.tcp_port) != (old.ip , old.tcp_port): #kiểm tra những thiết bị nào đã từng kết nối nếu check trùng thì bỏ qua
                return
            self.peers[peer.peer_id] = peer

            if not old:
                self.changed("peer_online" , peer)
            elif old.status != peer.status: #ktra trạng thái on,off của thiết bị đã kết nối
                self.changed("peer_status_changed", peer)
            elif (old.name, old.avatar) != (peer.name, peer.avatar): #kiểm tra tên hoặc màu avatar có đổi không
                self.changed("peer_updated", peer)

    def expire(self , at: float | None = None)-> None: #hàm kiểm tra các thiết bị nào còn trog tgian cho phép để onl , còn hết thì gán trạng thái ngoại tuyến
        if at is None:
            at = time.monotonic()
        else:
            at = at
        with self.lock:
            for peer in list(self.peers.values()):
                if at - peer.seen_at >= self.expiry: # nếu tgian gần nhất kiểm tra trừ cho thời gian gần nhất nhận đc gói tin mà lớn hơn móc tgian hết hạn
                    del self.peers[peer.peer_id]
                    self.changed("peer_offline" , replace(peer , status= "offline"))

    def get(self , peer_id : str) -> Peer | None: #tìm người đang onl thì trả ra còn ko thì trả none
        with self.lock:
            return self.peers.get(peer_id)

    def snapshot(self) -> list[Record]:
        with self.lock:
            return [peer.public() for peer in self.peers.values()]

class Discovery:
    def __init__(self , config : Config , ip : str , broadcast : str , registry : Registry , presence: Callable[[] , Presence] ,network_error: Callable[[], None] ):
        self.config = config
        self.registry = registry
        self.presence = presence
        self.network_error = network_error
        if config.broadcast:
            addresses = (config.broadcast,)
        else:
            addresses = (broadcast, "255.255.255.255")

        self.destinations = config.targets or tuple((address, config.udp_port) for address in dict.fromkeys(addresses))
        self.stop = Event() 
        self.wakeup = Event()
        #event thuộc thread , giúp nhiều thread giao tiếp và động bộ với nhau qua các trạng thái cờ

        self.sender = socket.socket(socket.AF_INET,socket.SOCK_DGRAM) #gửi UDP presence
        self.listener = socket.socket(socket.AF_INET,socket.SOCK_DGRAM) #nhận UDP presence
        try:
            self.sender.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self.sender.bind((ip, 0))
            self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.listener.bind(("", config.udp_port))
            self.listener.settimeout(0.25)
        except OSError:
            self.sender.close()
            self.listener.close()
            raise
        self.workers = [
            Thread(target=self._announce, name="udp-broadcast", daemon=True),
            Thread(target=self._listen, name="udp-listen", daemon=True),
            Thread(target=self._monitor, name="udp-expiry", daemon=True),
        ]

    def start(self) -> None:
        for worker in self.workers:
            worker.start()

    def refresh(self) -> None:
        self.wakeup.set()

    def _announce(self) -> None:
        previous_failure = False
        while not self.stop.is_set():
            failed = not self._broadcast_presence()
            if failed and not previous_failure and not self.stop.is_set():
                self.network_error()
            previous_failure = failed
            self.wakeup.wait(self.config.heartbeat)
            self.wakeup.clear()

    def _broadcast_presence(self) -> bool:
        packet = encode_presence(self.presence())
        sent = False
        for endpoint in self.destinations:
            try:
                self.sender.sendto(packet, endpoint)
                sent = True
            except OSError as exc:
                if not self.stop.is_set():
                    log.debug("UDP announcement to %s failed: %s", endpoint, exc)
        return sent

    def _listen(self) -> None:
        while not self.stop.is_set():
            try:
                packet, address = self.listener.recvfrom(MAX_DATAGRAM + 1)
                self.registry.ingest(packet, address[0])
            except socket.timeout:
                continue
            except OSError as exc:
                if not self.stop.is_set():
                    log.warning("UDP receive failed: %s", exc)
                    self.stop.wait(0.2)

    def _monitor(self) -> None:
        while not self.stop.wait(0.2):
            self.registry.expire()

    def close(self) -> None:
        self.stop.set()
        self.wakeup.set()
        self.sender.close()
        self.listener.close()
        for worker in self.workers:
            if worker.ident is not None:
                worker.join()


