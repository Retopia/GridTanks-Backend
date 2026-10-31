"""Run with python -m unittest test_coop_signaling; no database or live server needed."""
import asyncio
import importlib.util
import json
import os
import unittest

from fastapi import WebSocketDisconnect

if os.environ.get("ROUTES_FILE"):
    spec = importlib.util.spec_from_file_location("app.routes_p2p_test", os.environ["ROUTES_FILE"])
    routes = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(routes)
else:
    from app import routes


class Socket:
    def __init__(self, messages=()):
        self.messages = iter(messages)
        self.sent = []

    async def accept(self):
        pass

    async def close(self, **kwargs):
        pass

    async def send_text(self, text):
        self.sent.append(json.loads(text))

    async def receive_text(self):
        try:
            return json.dumps(next(self.messages))
        except StopIteration:
            raise WebSocketDisconnect()


class SignalingTests(unittest.TestCase):
    def setUp(self):
        self.peer = Socket()
        self.room = {
            "room_code": "P2P123", "run_id": "run", "game_started": True,
            "host": {"name": "Host", "token": "host", "websocket": self.peer},
            "guest": {"name": "Guest", "token": "guest", "websocket": self.peer},
        }
        routes.ACTIVE_ROOMS["P2P123"] = self.room

    def tearDown(self):
        routes.ACTIVE_ROOMS.pop("P2P123", None)

    def run_messages(self, role, messages):
        socket = Socket(messages)
        asyncio.run(routes.room_socket(socket, "P2P123", role))
        return socket

    def signal(self, kind="ready", run="run"):
        return {"type": "rtc_signal", "run_id": run, "payload": {"kind": kind, "id": "peer"}}

    def test_forward_signaling_and_configuration(self):
        socket = self.run_messages("host", [self.signal("offer")])
        signals = [m for m in self.peer.sent if m["type"] == "rtc_signal"]
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["run_id"], "run")
        self.assertTrue(any(m["type"] == "rtc_config" for m in socket.sent))

    def test_reject_wrong_run_role_and_invalid_messages(self):
        self.run_messages("guest", [[], self.signal("offer"), self.signal(run="other"),
                                    {"type": "rtc_signal", "run_id": "run", "payload": None}])
        self.assertFalse(any(m["type"] == "rtc_signal" for m in self.peer.sent))

    def test_requires_membership(self):
        socket = self.run_messages("outsider", [self.signal()])
        self.assertEqual(socket.sent[0]["type"], "error")
        self.assertFalse(any(m["type"] == "rtc_config" for m in socket.sent))

    def test_relay_preserves_order_metadata(self):
        self.run_messages("guest", [{"type": "coop_input", "payload": {"keys": {"w": True}},
                                    "stream": "peer", "seq": 8}])
        message = next(m for m in self.peer.sent if m["type"] == "coop_input")
        self.assertEqual((message["stream"], message["seq"], message["run_id"]), ("peer", 8, "run"))

    def test_signaling_rate_limit(self):
        self.run_messages("guest", [self.signal() for _ in range(120)])
        self.assertEqual(sum(m["type"] == "rtc_signal" for m in self.peer.sent), 100)


if __name__ == "__main__":
    unittest.main()
