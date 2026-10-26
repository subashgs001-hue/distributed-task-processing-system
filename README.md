# Mini Distributed Application — Distributed Task Processing System

A small Python 3 demonstration of distributed task processing using a coordinator and multiple worker nodes. The coordinator sends independent integer-square tasks to workers over TCP sockets using newline-delimited JSON messages.

## Features

- Coordinator/worker architecture over TCP.
- JSON messages: `REGISTER`, `TASK`, `RESULT`, `WAIT`, and `ERROR`.
- Shared in-memory task queue and concurrent worker connections.
- Requeues unfinished work when a worker disconnects.
- Worker reconnect attempts when the coordinator is unavailable.
- Rejects a second simultaneous connection using the same worker ID.
- Basic unit tests for worker task execution.

## Requirements

- Python 3.10 or newer (no third-party packages required).
- Coordinator and workers must be able to reach each other over TCP port `5000` (or the configured port).

## Run locally

Open a terminal in this folder.

**Terminal 1 — start the coordinator:**

```bash
python coordinator.py
```

**Terminal 2 — start worker 1:**

```bash
python worker.py W1
```

**Terminal 3 — start worker 2:**

```bash
python worker.py W2
```

The coordinator submits 10 tasks by default. Workers request tasks and print results such as `Task 3: 3² = 9`. Stop a process with `Ctrl+C`.

## Run on multiple computers

1. Run `coordinator.py` on the coordinator computer.
2. Find that computer's LAN IP address.
3. Ensure TCP port `5000` is allowed by the firewall on the coordinator computer.
4. On each worker computer, run `python worker.py W1 <COORDINATOR_LAN_IP> 5000`, using a unique worker ID for each worker.
5. Use only on a trusted network unless you add authentication and TLS.

`0.0.0.0` is the coordinator's bind address; workers should connect to the coordinator's actual LAN IP, not `0.0.0.0`.

## Tests

Run from the project root:

```bash
python -m unittest discover -s tests -v
```

## Protocol overview

Each message is a JSON object followed by a newline. The coordinator first receives `{"type":"REGISTER","worker_id":"W1"}`. It sends either `WAIT` or a `TASK` containing a task ID, operation, and value. The worker responds with a `RESULT` containing the task ID, worker ID, and result.

## Repository structure

```text
.
├── coordinator.py
├── worker.py
├── tests/
│   └── test_worker.py
├── README.md
├── requirements.txt
└── .gitignore
```

## Current limitations

This is an educational prototype, not a production job queue. Tasks and pending state exist only in memory; the coordinator is a single point of failure; traffic is not encrypted; workers are not authenticated; and the system provides at-least-once execution rather than exactly-once execution. A worker can be retried after a failure, so real tasks should be idempotent. The coordinator currently submits a fixed set of 10 square tasks when it starts.

## Suggested future improvements

- Durable queue and result storage.
- TLS and worker authentication.
- Heartbeats, explicit acknowledgements, and retry limits.
- Configurable task submission and supported operations.
- Metrics, logging, and load-aware scheduling.
