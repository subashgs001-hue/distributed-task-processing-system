"""Worker node that connects to the coordinator and executes assigned tasks."""
from __future__ import annotations

import json
import socket
import sys
import time
from typing import Any

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5000
RECONNECT_SECONDS = 2


def send_json(conn: socket.socket, payload: dict[str, Any]) -> None:
    conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))


def read_json(reader: Any) -> dict[str, Any] | None:
    line = reader.readline()
    if not line:
        return None
    message = json.loads(line)
    if not isinstance(message, dict):
        raise ValueError("Protocol message must be a JSON object")
    return message


def execute_task(task: dict[str, Any]) -> Any:
    operation = task.get("operation")
    value = task.get("value")
    if not isinstance(value, int):
        raise ValueError("Task value must be an integer")
    if operation == "square":
        return value ** 2
    raise ValueError(f"Unsupported operation: {operation}")


def run_worker(worker_id: str, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    while True:
        try:
            with socket.create_connection((host, port), timeout=5) as conn:
                conn.settimeout(None)
                reader = conn.makefile("r", encoding="utf-8", newline="\n")
                try:
                    send_json(conn, {"type": "REGISTER", "worker_id": worker_id})
                    print(f"[{worker_id}] Connected to coordinator at {host}:{port}")
                    while True:
                        message = read_json(reader)
                        if message is None:
                            raise ConnectionError("coordinator closed the connection")
                        if message.get("type") == "ERROR":
                            print(f"[{worker_id}] Registration rejected: {message.get('message')}")
                            return
                        if message.get("type") == "WAIT":
                            time.sleep(1)
                            continue
                        if message.get("type") != "TASK":
                            raise ValueError(f"Unexpected message: {message}")

                        task = message.get("task", {})
                        result = execute_task(task)
                        send_json(conn, {
                            "type": "RESULT",
                            "task_id": task["id"],
                            "worker_id": worker_id,
                            "result": result,
                        })
                        print(f"[{worker_id}] Task {task['id']}: {task['value']}² = {result}")
                finally:
                    reader.close()
        except (OSError, ConnectionError, json.JSONDecodeError, ValueError, KeyError) as exc:
            print(f"[{worker_id}] Coordinator unavailable or connection ended: {exc}. Retrying in {RECONNECT_SECONDS}s...")
            time.sleep(RECONNECT_SECONDS)


if __name__ == "__main__":
    worker_id = sys.argv[1] if len(sys.argv) > 1 else "W1"
    host = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_HOST
    port = int(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_PORT
    run_worker(worker_id, host, port)
