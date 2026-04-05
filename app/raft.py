import asyncio
import json
import os
import random
import time
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

import httpx


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    key: str
    value: str


class RaftNode:
    def __init__(self) -> None:
        self.node_id = os.getenv("NODE_ID", "node1")
        peers_raw = os.getenv("PEERS", "")
        self.peers = [p.strip().rstrip("/") for p in peers_raw.split(",") if p.strip()]
        self.election_timeout_min_ms = int(os.getenv("ELECTION_TIMEOUT_MIN_MS", "150"))
        self.election_timeout_max_ms = int(os.getenv("ELECTION_TIMEOUT_MAX_MS", "300"))
        self.heartbeat_interval_ms = int(os.getenv("HEARTBEAT_INTERVAL_MS", "50"))
        self.state_file = os.getenv("STATE_FILE", "/data/state.json")

        self.current_term = 0
        self.voted_for: str | None = None
        self.log: list[LogEntry] = []

        self.commit_index = -1
        self.last_applied = -1
        self.role = Role.FOLLOWER
        self.leader_id: str | None = None

        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self.state_machine: dict[str, str] = {}

        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._last_rpc_time = time.monotonic()
        self._election_timeout_s = self._new_election_timeout()

        timeout = max(0.2, (self.heartbeat_interval_ms / 1000.0) * 2)
        self._http = httpx.AsyncClient(timeout=timeout)
        self._tasks: list[asyncio.Task[Any]] = []

    def _new_election_timeout(self) -> float:
        return random.uniform(self.election_timeout_min_ms / 1000.0, self.election_timeout_max_ms / 1000.0)

    def _majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def _last_log_index(self) -> int:
        return len(self.log) - 1

    def _last_log_term(self) -> int:
        if not self.log:
            return 0
        return self.log[-1].term

    def _reset_election_timer(self) -> None:
        self._last_rpc_time = time.monotonic()
        self._election_timeout_s = self._new_election_timeout()

    def _load_state(self) -> None:
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                raw = json.load(f)
            self.current_term = int(raw.get("currentTerm", 0))
            self.voted_for = raw.get("votedFor")
            self.log = [LogEntry(**entry) for entry in raw.get("log", [])]
        except FileNotFoundError:
            return

    def _persist_state(self) -> None:
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        payload = {
            "currentTerm": self.current_term,
            "votedFor": self.voted_for,
            "log": [asdict(e) for e in self.log],
        }
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(payload, f)

    async def start(self) -> None:
        self._load_state()
        self._tasks = [
            asyncio.create_task(self._election_loop()),
            asyncio.create_task(self._heartbeat_loop()),
        ]

    async def stop(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._http.aclose()

    async def _election_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(0.01)
            async with self._lock:
                timed_out = (time.monotonic() - self._last_rpc_time) >= self._election_timeout_s
                should_elect = timed_out and self.role != Role.LEADER
            if should_elect:
                await self._start_election()

    async def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.heartbeat_interval_ms / 1000.0)
            async with self._lock:
                if self.role != Role.LEADER:
                    continue
            await self._replicate_to_all()

    async def _start_election(self) -> None:
        async with self._lock:
            self.role = Role.CANDIDATE
            self.current_term += 1
            term = self.current_term
            self.voted_for = self.node_id
            self.leader_id = None
            self._persist_state()
            self._reset_election_timer()
            votes = 1
            last_log_index = self._last_log_index()
            last_log_term = self._last_log_term()

        tasks = [
            self._send_request_vote(peer, term, last_log_index, last_log_term)
            for peer in self.peers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        async with self._lock:
            if self.current_term != term or self.role != Role.CANDIDATE:
                return
            for result in results:
                if isinstance(result, Exception) or result is None:
                    continue
                if result.get("term", 0) > self.current_term:
                    await self._become_follower(result["term"], None)
                    return
                if result.get("voteGranted"):
                    votes += 1

            if votes >= self._majority():
                self.role = Role.LEADER
                self.leader_id = self.node_id
                next_idx = len(self.log)
                self.next_index = {peer: next_idx for peer in self.peers}
                self.match_index = {peer: -1 for peer in self.peers}
            else:
                self.role = Role.FOLLOWER

    async def _send_request_vote(self, peer: str, term: int, last_log_index: int, last_log_term: int) -> dict[str, Any] | None:
        payload = {
            "term": term,
            "candidateId": self.node_id,
            "lastLogIndex": last_log_index,
            "lastLogTerm": last_log_term,
        }
        try:
            response = await self._http.post(f"{peer}/raft/request-vote", json=payload)
            if response.status_code != 200:
                return None
            return response.json()
        except Exception:
            return None

    async def _send_append_entries(self, peer: str) -> bool:
        while True:
            async with self._lock:
                if self.role != Role.LEADER:
                    return False
                term = self.current_term
                next_index = self.next_index.get(peer, len(self.log))
                prev_log_index = next_index - 1
                prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0
                entries = [asdict(e) for e in self.log[next_index:]]
                leader_commit = self.commit_index

            payload = {
                "term": term,
                "leaderId": self.node_id,
                "prevLogIndex": prev_log_index,
                "prevLogTerm": prev_log_term,
                "entries": entries,
                "leaderCommit": leader_commit,
            }
            try:
                response = await self._http.post(f"{peer}/raft/append-entries", json=payload)
            except Exception:
                return False

            if response.status_code != 200:
                return False

            data = response.json()
            async with self._lock:
                if data.get("term", 0) > self.current_term:
                    await self._become_follower(data["term"], None)
                    return False
                if self.role != Role.LEADER or term != self.current_term:
                    return False

                if data.get("success"):
                    match_index = data.get("matchIndex", prev_log_index + len(entries))
                    self.match_index[peer] = match_index
                    self.next_index[peer] = match_index + 1
                    self._advance_commit_index()
                    return True

                self.next_index[peer] = max(0, next_index - 1)

    async def _replicate_to_all(self) -> None:
        tasks = [self._send_append_entries(peer) for peer in self.peers]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _advance_commit_index(self) -> None:
        for idx in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[idx].term != self.current_term:
                continue
            replicated = 1
            for peer in self.peers:
                if self.match_index.get(peer, -1) >= idx:
                    replicated += 1
            if replicated >= self._majority():
                self.commit_index = idx
                self._apply_entries()
                return

    def _apply_entries(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            self.state_machine[entry.key] = entry.value

    async def _become_follower(self, new_term: int, leader_id: str | None) -> None:
        self.role = Role.FOLLOWER
        self.current_term = new_term
        self.voted_for = None
        self.leader_id = leader_id
        self._persist_state()
        self._reset_election_timer()

    async def handle_request_vote(self, payload: dict[str, Any]) -> dict[str, Any]:
        async with self._lock:
            term = int(payload["term"])
            candidate_id = payload["candidateId"]
            last_log_index = int(payload["lastLogIndex"])
            last_log_term = int(payload["lastLogTerm"])

            if term < self.current_term:
                return {"term": self.current_term, "voteGranted": False}

            if term > self.current_term:
                await self._become_follower(term, None)

            up_to_date = (
                (last_log_term > self._last_log_term())
                or (last_log_term == self._last_log_term() and last_log_index >= self._last_log_index())
            )

            can_vote = self.voted_for is None or self.voted_for == candidate_id
            vote_granted = bool(can_vote and up_to_date)

            if vote_granted:
                self.voted_for = candidate_id
                self._persist_state()
                self._reset_election_timer()

            return {"term": self.current_term, "voteGranted": vote_granted}

    async def handle_append_entries(self, payload: dict[str, Any]) -> dict[str, Any]:
        async with self._lock:
            term = int(payload["term"])
            leader_id = payload["leaderId"]
            prev_log_index = int(payload["prevLogIndex"])
            prev_log_term = int(payload["prevLogTerm"])
            entries = payload.get("entries", [])
            leader_commit = int(payload["leaderCommit"])

            if term < self.current_term:
                return {"term": self.current_term, "success": False, "matchIndex": self._last_log_index()}

            if term > self.current_term:
                await self._become_follower(term, leader_id)
            else:
                self.role = Role.FOLLOWER
                self.leader_id = leader_id
                self._reset_election_timer()

            if prev_log_index >= 0:
                if prev_log_index >= len(self.log):
                    return {"term": self.current_term, "success": False, "matchIndex": self._last_log_index()}
                if self.log[prev_log_index].term != prev_log_term:
                    self.log = self.log[:prev_log_index]
                    self._persist_state()
                    return {"term": self.current_term, "success": False, "matchIndex": self._last_log_index()}

            insert_idx = prev_log_index + 1
            changed = False
            for i, raw in enumerate(entries):
                entry = LogEntry(term=int(raw["term"]), key=raw["key"], value=raw["value"])
                target_idx = insert_idx + i
                if target_idx < len(self.log):
                    if self.log[target_idx].term != entry.term:
                        self.log = self.log[:target_idx]
                        self.log.append(entry)
                        changed = True
                else:
                    self.log.append(entry)
                    changed = True

            if changed:
                self._persist_state()

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self._last_log_index())
                self._apply_entries()

            return {"term": self.current_term, "success": True, "matchIndex": self._last_log_index()}

    async def handle_command(self, key: str, value: str) -> dict[str, Any]:
        async with self._lock:
            if self.role != Role.LEADER:
                return {
                    "accepted": False,
                    "reason": "not_leader",
                    "leaderId": self.leader_id,
                    "term": self.current_term,
                }
            entry = LogEntry(term=self.current_term, key=key, value=value)
            self.log.append(entry)
            entry_index = len(self.log) - 1
            self._persist_state()

        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            await self._replicate_to_all()
            async with self._lock:
                if self.commit_index >= entry_index:
                    return {
                        "accepted": True,
                        "committed": True,
                        "index": entry_index,
                        "term": self.current_term,
                    }
            await asyncio.sleep(self.heartbeat_interval_ms / 1000.0)

        async with self._lock:
            self.log = self.log[:entry_index]
            self._persist_state()
            return {
                "accepted": False,
                "reason": "quorum_not_reached",
                "term": self.current_term,
            }

    async def read_value(self, key: str) -> dict[str, Any]:
        async with self._lock:
            return {
                "key": key,
                "value": self.state_machine.get(key),
                "term": self.current_term,
                "leaderId": self.leader_id,
            }

    async def status(self) -> dict[str, Any]:
        async with self._lock:
            return {
                "nodeId": self.node_id,
                "role": self.role.value,
                "term": self.current_term,
                "leaderId": self.leader_id,
                "commitIndex": self.commit_index,
                "lastApplied": self.last_applied,
                "logLength": len(self.log),
                "stateMachine": dict(self.state_machine),
                "votedFor": self.voted_for,
                "peers": list(self.peers),
            }
