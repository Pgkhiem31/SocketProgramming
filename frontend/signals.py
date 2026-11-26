"""Send updates to the Qt thread and run service commands in the background."""

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Thread

from PySide6.QtCore import QObject, Signal

from backend.app_errors import LanError

log = logging.getLogger(__name__)


class NetworkSignals(QObject):
    # Chuyển sự kiện từ luồng nền về nơi xử lý giao diện qua tín hiệu Qt.
    received = Signal(str, object)

    def emit(self, kind: str, **payload: object) -> None:
        self.received.emit(kind, payload)


class Actions:
    def __init__(self, signals: NetworkSignals):
        self.signals = signals
        # Chạy tuần tự các lệnh từ giao diện trên một luồng nền để giữ thứ tự thao tác.
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="lanlink-action")
        self.closing = False

    def run(self, action: Callable, *args: object, finished: str = "", **context: object) -> None:
        if self.closing:
            return

        def execute():
            try:
                result = action(*args)
            except LanError as exc:
                self.signals.emit("action_error", error=exc.code, **context)
            except Exception:
                log.exception("Desktop action failed")
                self.signals.emit("action_error", error="unexpected", **context)
            else:
                if finished:
                    self.signals.emit(finished, result=result, **context)

        self.executor.submit(execute)

    def stop(self, close: Callable[[], None]) -> None:
        if self.closing:
            return
        self.closing = True

        def finish():
            try:
                # Hủy lệnh còn chờ và đợi lệnh đang chạy xong trước khi đóng dịch vụ.
                self.executor.shutdown(cancel_futures=True)
                close()
            except Exception:
                log.exception("LANLink shutdown failed")
            finally:
                self.signals.emit("shutdown_complete")

        Thread(target=finish, name="lanlink-shutdown").start()

