"""Coordinator for a small TCP-based distributed task-processing system."""
from __future__ import annotations

import json
import queue
import socket
import threading
import time
from typing import Any

HOST = "0.0.0.0"
PORT = 5000
WORKER_TIMEOUT = 15
WAIT_SECONDS = 1

_task_queue: queue.Queue[dict[str, Any]] = queue.Queue()
_pending: dict[int, tuple[str, dict[str, Any], float]] = {}
_active_workers: set[str] = set()
_lock = threading.Lock()


def send_json(conn: socket.socket, payload: dict[str, Any]) -> None:
    """Send one newline-delimited JSON message."""
    conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))


def read_json(reader: Any) -> dict[str, Any] | None:
    """Read one JSON message; return None when the peer closes the connection."""
    line = reader.readline()
    if not line:
        return None
    message = json.loads(line)
    if not isinstance(message, dict):
        raise ValueError("Protocol message must be a JSON object")
    return message


def requeue_worker_tasks(worker_id: str) -> None:
    """Return unfinished tasks for a disconnected worker to the shared queue."""
    with _lock:
        failed_ids = [task_id for task_id, (wid, _, _) in _pending.items() if wid == worker_id]
        for task_id in failed_ids:
            _, task, _ = _pending.pop(task_id)
            _task_queue.put(task)
        _active_workers.discard(worker_id)
    if failed_ids:
        print(f"[Coordinator] Requeued unfinished task(s) from {worker_id}: {failed_ids}")


def handle_worker(conn: socket.socket, address: tuple[str, int]) -> None:
    worker_id: str | None = None
    registered = False
    reader = None
    try:
        conn.settimeout(WORKER_TIMEOUT)
        reader = conn.makefile("r", encoding="utf-8", newline="\n")
        registration = read_json(reader)
        if not registration or registration.get("type") != "REGISTER":
            send_json(conn, {"type": "ERROR", "message": "First message must be REGISTER"})
            return

        worker_id = str(registration.get("worker_id", "")).strip()
        if not worker_id:
            send_json(conn, {"type": "ERROR", "message": "worker_id is required"})
            return
        with _lock:
            if worker_id in _active_workers:
                send_json(conn, {"type": "ERROR", "message": "worker_id is already connected"})
                return
            _active_workers.add(worker_id)
            registered = True
        print(f"[Coordinator] Worker {worker_id} registered from {address[0]}:{address[1]}")

        while True:
            try:
                task = _task_queue.get_nowait()
            except queue.Empty:
                send_json(conn, {"type": "WAIT"})
                time.sleep(WAIT_SECONDS)
                continue

            with _lock:
                _pending[task["id"]] = (worker_id, task, time.time())
            send_json(conn, {"type": "TASK", "task": task})

            response = read_json(reader)
            if response is None:
                raise ConnectionError("worker disconnected before returning result")
            if response.get("type") != "RESULT":
                raise ValueError("Expected RESULT message")
            if response.get("worker_id") != worker_id or response.get("task_id") != task["id"]:
                raise ValueError("Result does not match assigned worker/task")

            with _lock:
                _pending.pop(task["id"], None)
            print(f"[Coordinator] Task {task['id']} completed by {worker_id}: {response.get('result')}")
    except (OSError, ConnectionError, json.JSONDecodeError, ValueError, socket.timeout) as exc:
        print(f"[Coordinator] Worker {worker_id or address} connection ended: {exc}")
    finally:
        if worker_id and registered:
            requeue_worker_tasks(worker_id)
        if reader:
            try:
                reader.close()
            except OSError:
                pass
        conn.close()


def submit_tasks(task_count: int = 10) -> None:
    """Create integer-square tasks numbered from 1 through task_count."""
    for number in range(1, task_count + 1):
        _task_queue.put({"id": number, "operation": "square", "value": number})
    print(f"[Coordinator] {task_count} tasks submitted.")


def serve(host: str = HOST, port: int = PORT) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen()
        print(f"[Coordinator] Listening on {host}:{port}")
        while True:
            conn, address = server.accept()
            threading.Thread(target=handle_worker, args=(conn, address), daemon=True).start()


if __name__ == "__main__":
    submit_tasks()
    try:
        serve()
    except KeyboardInterrupt:
        print("\n[Coordinator] Shutting down.")
