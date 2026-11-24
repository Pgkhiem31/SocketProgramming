"""Kiểm thử TCP Session độc lập, không cần chat, file transfer, UDP hay GUI."""
import queue
import socket
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from unittest.mock import patch
from uuid import uuid4

from backend import wire_protocol as wire
from backend.tcp_sessions import TCPManager


class TCPSessionTests(unittest.TestCase):
    def setUp(self):
        self.a, self.a_id, self.a_frames, self.a_closed = self.make_peer("A")
        self.b, self.b_id, self.b_frames, self.b_closed = self.make_peer("B")

    def make_peer(self, name, **kwargs):
        # Port 0 để hệ điều hành tự chọn port rảnh, tránh xung đột.
        identity = uuid4()
        frames, closed = queue.Queue(), queue.Queue()
        manager = TCPManager(
            lambda: wire.Presence(peer_id=identity, name=name,
                                  tcp_port=manager.port, status="online"),
            host="127.0.0.1", port=0, timeout=1.0,
            on_chat=lambda session, frame, body: frames.put((session, frame, body)),
            on_close=lambda session: closed.put(session), **kwargs,
        )
        self.addCleanup(manager.close)
        manager.start()
        return manager, str(identity), frames, closed

    def wait_for(self, predicate, timeout=3.0):
        # Chờ worker hoàn thành thay vì giả định một khoảng sleep cố định.
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            time.sleep(0.01)
        self.fail("Hết thời gian chờ điều kiện TCP")

    def connect_pair(self):
        session = self.a.connect("127.0.0.1", self.b.port, self.b_id)
        remote = self.wait_for(lambda: self.b.get(self.a_id))
        return session, remote

    def test_01_start_listener(self):
        # start() gọi lại không tạo listener mới.
        self.assertGreater(self.a.port, 0)
        self.assertEqual(self.a.start(), self.a.port)
        with socket.create_connection(("127.0.0.1", self.a.port), timeout=1):
            pass

    def test_02_connect_and_hello(self):
        a, b = self.connect_pair()
        self.assertEqual(a.peer_id, self.b_id)
        self.assertEqual(b.peer_id, self.a_id)
        self.assertEqual(a.session_id, b.session_id)
        self.assertEqual(a.remote.mode, "chat")
        self.assertEqual(a.remote.tcp_port, self.b.port)

    def test_03_bidirectional_unicode_and_ack_frame(self):
        # Chỉ kiểm tra vận chuyển frame, không kiểm tra logic lưu chat.
        a, b = self.connect_pair()
        body = "Xin chào TCP 🚀".encode("utf-8")
        msg = wire.ChatMessage(message_id=uuid4(), body_length=len(body))
        a.send(msg, body)
        session, frame, received = self.b_frames.get(timeout=3)
        self.assertIs(session, b)
        self.assertEqual((frame, received), (msg, body))
        b.send(wire.ChatAck(message_id=msg.message_id))
        session, frame, received = self.a_frames.get(timeout=3)
        self.assertIs(session, a)
        self.assertEqual((frame, received), (wire.ChatAck(message_id=msg.message_id), b""))

    def test_04_multiple_peers(self):
        c, c_id, _, _ = self.make_peer("C")
        self.connect_pair()
        c.connect("127.0.0.1", self.a.port, self.a_id)
        self.wait_for(lambda: self.a.get(c_id))
        self.assertIsNotNone(self.a.get(self.b_id))
        self.assertNotEqual(self.a.get(c_id).session_id, self.a.get(self.b_id).session_id)

    def test_05_reuse_existing_session(self):
        a, _ = self.connect_pair()
        self.assertIs(self.a.connect("127.0.0.1", self.b.port, self.b_id), a)

    def test_06_simultaneous_connect(self):
        # Hai đầu cùng dial; sau khi ổn định phải giữ cùng một session ID.
        barrier = Barrier(2)
        def dial(manager, other, peer_id):
            barrier.wait(timeout=3)
            return manager.connect("127.0.0.1", other.port, peer_id)
        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(dial, self.a, self.b, self.b_id)
            f2 = pool.submit(dial, self.b, self.a, self.a_id)
            f1.result(timeout=4)
            f2.result(timeout=4)
        def same_session():
            a, b = self.a.get(self.b_id), self.b.get(self.a_id)
            return a if a and b and a.session_id == b.session_id else None
        a = self.wait_for(same_session)
        body = b"after race"
        frame = wire.ChatMessage(message_id=uuid4(), body_length=len(body))
        a.send(frame, body)
        self.assertEqual(self.b_frames.get(timeout=3)[1:], (frame, body))

    def test_07_self_connection_rejected(self):
        with self.assertRaises((wire.ProtocolError, EOFError, OSError)):
            self.a.connect("127.0.0.1", self.a.port, self.a_id)
        self.assertIsNone(self.a.get(self.a_id))

    def test_08_wrong_peer_id(self):
        with self.assertRaises(wire.ProtocolError):
            self.a.connect("127.0.0.1", self.b.port, str(uuid4()))
        self.assertIsNone(self.a.get(self.b_id))

    def test_09_first_frame_must_be_hello(self):
        with socket.create_connection(("127.0.0.1", self.b.port), timeout=2) as sock:
            wire.send_frame(sock, wire.Bye())
            # Peer có thể trả EOF hoặc reset tùy hệ điều hành.
            try:
                self.assertEqual(sock.recv(1), b"")
            except (ConnectionResetError, ConnectionAbortedError):
                pass
        self.assertIsNone(self.b.get(self.a_id))

    def test_10_leave_and_reconnect(self):
        old_a, old_b = self.connect_pair()
        self.a.leave(self.b_id)
        self.assertTrue(old_a.closed.wait(3))
        self.assertTrue(old_b.closed.wait(3))
        self.wait_for(lambda: self.a.get(self.b_id) is None and self.b.get(self.a_id) is None)
        new_a, new_b = self.connect_pair()
        self.assertNotEqual(old_a.session_id, new_a.session_id)
        self.assertEqual(new_a.session_id, new_b.session_id)

    def test_11_abrupt_disconnect(self):
        a, b = self.connect_pair()
        a.close()  # Không gửi Bye, mô phỏng kết nối bị đóng đột ngột.
        self.assertTrue(b.closed.wait(3))
        self.assertIs(self.b_closed.get(timeout=3), b)
        self.assertIsNone(self.b.get(self.a_id))

    def test_12_handshake_timeout(self):
        # Rút ngắn timeout giao thức riêng trong test để không mất 6 giây.
        with patch.object(wire, "IO_TIMEOUT", 0.2):
            with socket.create_connection(("127.0.0.1", self.b.port), timeout=2) as sock:
                try:
                    self.assertEqual(sock.recv(1), b"")
                except (ConnectionResetError, ConnectionAbortedError):
                    pass
        self.connect_pair()  # Listener vẫn hoạt động sau kết nối lỗi.

    def test_13_separate_file_socket(self):
        # Callback giả lập file: chỉ kiểm tra phân luồng socket, không truyền file thật.
        file_seen = Event()
        def on_file(sock, hello):
            frame, body = wire.read_frame(sock)
            if isinstance(frame, wire.FileChunk) and body == b"abc":
                wire.send_frame(sock, wire.FileAck(file_id=frame.file_id))
                file_seen.set()
        c, c_id, c_frames, _ = self.make_peer("C", on_file=on_file)
        chat = self.a.connect("127.0.0.1", c.port, c_id)
        file_id = uuid4()
        with self.a.open_file("127.0.0.1", c.port, c_id) as (sock, hello):
            self.assertEqual(hello.mode, "file")
            self.assertIsNot(sock, chat.sock)
            wire.send_frame(sock, wire.FileChunk(file_id=file_id, body_length=3), b"abc")
            self.assertEqual(wire.read_frame(sock), (wire.FileAck(file_id=file_id), b""))
            self.assertTrue(file_seen.wait(3))
        self.assertIs(self.a.get(c_id), chat)
        body = b"still connected"
        msg = wire.ChatMessage(message_id=uuid4(), body_length=len(body))
        chat.send(msg, body)
        self.assertEqual(c_frames.get(timeout=3)[1:], (msg, body))

    def test_14_close_manager(self):
        a, b = self.connect_pair()
        self.a.close()
        self.assertTrue(a.closed.is_set())
        self.assertTrue(b.closed.wait(3))
        self.assertIsNone(self.a.get(self.b_id))
        with self.assertRaises(RuntimeError):
            self.a.start()
        with self.assertRaises(OSError):
            socket.create_connection(("127.0.0.1", self.a.port), timeout=1)

    def test_15_concurrent_frame_sends(self):
        a, _ = self.connect_pair()
        # Nhiều luồng dùng chung Session.send(); frame không được xen lẫn.
        payloads = [f"message-{i}".encode() * 100 for i in range(12)]
        frames = [wire.ChatMessage(message_id=uuid4(), body_length=len(p)) for p in payloads]
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(a.send, f, p) for f, p in zip(frames, payloads)]
            for future in futures:
                future.result(timeout=3)
        received = {}
        for _ in frames:
            _, frame, body = self.b_frames.get(timeout=3)
            received[frame.message_id] = body
        self.assertEqual(received, {f.message_id: p for f, p in zip(frames, payloads)})

    def test_16_reject_file_frame_in_chat(self):
        a, _ = self.connect_pair()
        with self.assertRaises(wire.ProtocolError):
            a.send(wire.FileChunk(file_id=uuid4(), body_length=1), b"x")
        self.assertIs(self.a.get(self.b_id), a)


if __name__ == "__main__":
    unittest.main()
