"""Start one LANLink peer."""

import argparse
import logging
import sys
from ipaddress import IPv4Address
from pathlib import Path

from PySide6.QtCore import QSettings
from PySide6.QtWidgets import QApplication, QDialog, QMessageBox

from backend.app_config import Config
from frontend.appearance import theme
from frontend.main_window import MainWindow
from frontend.settings_dialog import SettingsDialog
from frontend.signals import NetworkSignals
from backend.lan_service import LanLink

ROOT = Path(__file__).resolve().parent
MIB = 1024 * 1024


def port(value):
    number = int(value)
    if not 1 <= number <= 65535:
        raise argparse.ArgumentTypeError("Cổng phải nằm trong khoảng 1–65535.")
    return number


def target(value):
    address, separator, number = value.partition(":")
    if not separator:
        raise argparse.ArgumentTypeError("Nhập đích discovery theo dạng IPv4:CỔNG.")
    return str(IPv4Address(address)), port(number)


def display_name(value):
    name = value.strip()
    if not name or len(name) > 40 or not name.isprintable():
        raise argparse.ArgumentTypeError("Tên hiển thị phải có 1–40 ký tự hiển thị được.")
    return name


def limit(value):
    number = int(value)
    if not 1 <= number <= 4096:
        raise argparse.ArgumentTypeError("Giới hạn tệp phải nằm trong khoảng 1–4096 MiB.")
    return number


def launch_options():
    parser = argparse.ArgumentParser(description="LANLink Desktop · Chat và truyền tệp trong LAN")
    parser.add_argument("--name", type=display_name, help="Tên hiển thị")
    parser.add_argument("--tcp-port", type=port, help="Cổng TCP, mặc định 5000")
    parser.add_argument("--udp-port", type=port, help="Cổng UDP, mặc định 9999")
    parser.add_argument("--interface", type=IPv4Address, help="IPv4 của card mạng")
    parser.add_argument("--broadcast", type=IPv4Address, help="Địa chỉ broadcast IPv4")
    parser.add_argument("--data-dir", type=Path, help="Thư mục dữ liệu của phiên LANLink")
    parser.add_argument("--downloads", type=Path, help="Thư mục nhận tệp")
    parser.add_argument("--file-limit-mib", type=limit, help="Kích thước tối đa mỗi tệp, mặc định 512 MiB")
    parser.add_argument("--discovery-target", type=target, action="append", default=[],
                        help="Đích UDP IPv4:CỔNG để kiểm thử nhiều phiên")
    return parser.parse_args()


# Tham số dòng lệnh được ưu tiên hơn thiết lập đã lưu và giá trị mặc định.
def read_parameters(settings, options):
    values = {
        "tcp_port": options.tcp_port or settings.value("tcp_port", 5000, type=int),
        "udp_port": options.udp_port or settings.value("udp_port", 9999, type=int),
        "broadcast": str(options.broadcast or settings.value("broadcast", "", type=str)),
        "downloads": str(options.downloads or settings.value("downloads", str(ROOT / "downloads"), type=str)),
        "file_limit_mib": options.file_limit_mib or settings.value("file_limit_mib", 512, type=int),
    }
    values["downloads"] = str(Path(values["downloads"]).expanduser().absolute())
    return values


# Đổi giới hạn MiB sang byte và gom cấu hình cho một phiên LANLink.
def make_service(options, parameters, directory, signals):
    config = Config(
        directory, Path(parameters["downloads"]),
        tcp_port=parameters["tcp_port"], udp_port=parameters["udp_port"],
        interface=str(options.interface) if options.interface else None,
        broadcast=parameters["broadcast"] or None,
        file_limit=parameters["file_limit_mib"] * MIB, name=options.name,
        targets=tuple(options.discovery_target),
    )
    return LanLink(config, signals)


def main():
    # Dùng UTF-8 để giữ tiếng Việt khi đầu ra console trên Windows bị chuyển hướng.
    for output in (sys.stdout, sys.stderr):
        if hasattr(output, "reconfigure"):
            output.reconfigure(encoding="utf-8", errors="backslashreplace")
    options = launch_options()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s · %(message)s")
    app = QApplication([])
    app.setApplicationName("LANLink Desktop")
    app.setStyle("Fusion")
    directory = (options.data_dir or ROOT / ".lanlink" / str(options.tcp_port or "default")).expanduser().absolute()
    settings = QSettings(str(directory / "desktop.ini"), QSettings.Format.IniFormat)
    signals = NetworkSignals()
    service = None

    def save_parameters(parameters):
        for key, value in parameters.items():
            settings.setValue(key, value)
        settings.sync()

    try:
        parameters = read_parameters(settings, options)
        service = make_service(options, parameters, directory, signals)
        profile = service.profile()
        theme(profile["theme"])
        # Mở thiết lập lần đầu nếu chưa có tên; tạo lại dịch vụ với cấu hình vừa lưu.
        if not profile["name"]:
            dialog = SettingsDialog(profile, service.network(), parameters, first_run=True)
            if dialog.exec() != QDialog.DialogCode.Accepted:
                service.close()
                return 0
            profile, parameters = dialog.result_values
            save_parameters(parameters)
            service.close()
            service = None
            service = make_service(options, parameters, directory, signals)
            service.update(profile)
        window = MainWindow(service, signals, parameters, save_parameters)
        window.show()
    except Exception as exc:
        logging.exception("Cannot start LANLink")
        if service is not None:
            service.close()
        QMessageBox.critical(None, "LANLink", f"Không thể mở LANLink.\n{exc}")
        return 1
    # Chạy vòng lặp sự kiện Qt cho đến khi ứng dụng thoát.
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())

