"Tìm và chọn ra cấu hình của card mạng của máy của mình"
from __future__ import annotations

import ipaddress
import socket
from dataclasses import dataclass
from pathlib import Path
import psutil
from typing_extensions import TypedDict

ROOT = Path(__file__).resolve().parents[2]
FILE_LIMIT = 512*1024*1024

class Interface(TypedDict):
    name : str
    ip : str
    broadcast : str

def interfaces() -> list[Interface]:
    result: list[Interface] = []
    active = psutil.net_if_stats() #Kiểm tra card mạng
    temp = psutil.net_if_addrs()
    for name , addresses in temp.items():
        if name in active and not active[name].isup:
            continue #Ktra card mạng có ttin trạng thái và có đag ngừng hoạt động ko
        for address in addresses:
            if address.family != socket.AF_INET or not address.netmask: #ktra address có phải IPV4 ko nếu ko thì next
                continue
            network = ipaddress.IPv4Interface(f"{address.address}/{address.netmask}")
            if network.ip.is_loopback:
                continue
            result.append({"name": name  , "ip" : address.address , "broadcast": str(network.network.broadcast_address)})
    return sorted(result, key=lambda item: (item["name"].casefold(), item["ip"]))

def choose_interface(selected : str) -> Interface | None:
    if selected and ipaddress.IPv4Address(selected).is_loopback:
        return {"name" : "Loopback" , "ip" : selected , "broadcast": "127.255.255.255"}
    available = interfaces()
    if selected:
        for item in available:
            if item["ip"] == selected:
                return item

        return None
    if not available:
        return None
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        try:
            probe.connect(("192.0.2.1", 9))
            source = probe.getsockname()[0]
        except OSError:
            source = ""
    return next((item for item in available if item["ip"] == source), available[0])

@dataclass(frozen=True)
class Config:
    data_dir: Path
    downloads: Path
    tcp_port: int = 5000
    udp_port: int = 9999
    interface: str | None = None
    broadcast: str | None = None
    file_limit: int = FILE_LIMIT
    name: str | None = None
    targets: tuple[tuple[str, int], ...] = ()
    heartbeat: float = 3.0  #chu kì kiểm tra mỗi lần gửi tín hiếu
    expiry: float = 10.0    # Nếu quá 10 giây ko thấy thì báo off
    frontend: Path = ROOT / "frontend" / "dist"
    @property
    def origins(self) -> set[str]:
        return {
            f"http://{host}:{port}"
            for host in ("127.0.0.1", "localhost")
            for port in (self.web_port, 5173)
        }