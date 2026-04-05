import argparse
import json
import random
import time
import urllib.error
from urllib.parse import urlparse
import urllib.request


def post_json(url: str, payload: dict, timeout: int = 5) -> tuple[int, dict | str]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, body
    except Exception as e:
        return 0, str(e)


def get_json(url: str, timeout: int = 2) -> tuple[int, dict | str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body)
    except Exception as e:
        return 0, str(e)


def leader_hint_to_url(leader_id: str, nodes: list[str]) -> str | None:
    mapping = {
        "node1": "localhost:8001",
        "node2": "localhost:8002",
        "node3": "localhost:8003",
    }
    target_hostport = mapping.get(leader_id)
    if not target_hostport:
        return None

    for node in nodes:
        parsed = urlparse(node)
        if parsed.netloc == target_hostport:
            return node
    return None


def find_leader(nodes: list[str]) -> str | None:
    """Определяет лидера через GET /health."""
    random.shuffle(nodes)
    for node in nodes:
        code, body = get_json(f"{node}/health", timeout=2)
        if code == 200 and isinstance(body, dict):
            role = body.get("role")
            if role == "leader":
                return node
            leader_id = body.get("leaderId")
            if leader_id:
                leader_url = leader_hint_to_url(leader_id, nodes)
                if leader_url:
                    return leader_url
    return None


def run_loop(nodes: list[str], delay: float = 1.0) -> None:
    ok = 0
    inconsistent = 0
    errors = 0
    leader_hint = None
    iteration = 0

    try:
        while True:
            iteration += 1
            key = f"key_{iteration}_{random.randint(1000, 9999)}"
            value = f"value_{random.randint(1000, 9999)}"

            if leader_hint:
                leader = leader_hint
            else:
                leader = find_leader(nodes)

            if not leader:
                errors += 1
                print(f"\rok: {ok} | inconsistent: {inconsistent} | errors: {errors}", end="", flush=True)
                time.sleep(delay)
                continue

            code, body = post_json(f"{leader}/command", {"key": key, "value": value}, timeout=5)

            if code == 200 and isinstance(body, dict) and body.get("committed"):
                time.sleep(0.15)
                read_node = random.choice(nodes)
                read_code, read_body = get_json(f"{read_node}/state/{key}", timeout=2)

                if read_code == 200 and isinstance(read_body, dict):
                    read_value = read_body.get("value")
                    if read_value == value:
                        ok += 1
                    else:
                        inconsistent += 1
                else:
                    errors += 1
            elif code == 409 or (isinstance(body, dict) and body.get("detail", {}).get("reason") == "quorum_not_reached"):
                errors += 1
            else:
                errors += 1

            print(f"\rok: {ok} | inconsistent: {inconsistent} | errors: {errors}", end="", flush=True)
            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\nStopped. Final: ok={ok}, inconsistent={inconsistent}, errors={errors}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple Raft client")
    parser.add_argument("--nodes", default="http://localhost:8001,http://localhost:8002,http://localhost:8003")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_loop = sub.add_parser("loop")
    p_loop.add_argument("--delay", type=float, default=1.0, help="Delay between iterations in seconds")

    p_cmd = sub.add_parser("command")
    p_cmd.add_argument("--key", required=True)
    p_cmd.add_argument("--value", required=True)

    p_get = sub.add_parser("get")
    p_get.add_argument("--key", required=True)

    sub.add_parser("health")

    args = parser.parse_args()
    nodes = [n.strip().rstrip("/") for n in args.nodes.split(",") if n.strip()]

    if args.cmd == "loop":
        run_loop(nodes, args.delay)
        return

    if args.cmd == "command":
        leader_hint = None
        last_error = None
        for _ in range(2):
            check_nodes = [leader_hint] + nodes if leader_hint else nodes
            for node in check_nodes:
                if not node:
                    continue
                code, body = post_json(f"{node}/command", {"key": args.key, "value": args.value})
                if code == 200 and isinstance(body, dict) and body.get("committed"):
                    print(json.dumps({"node": node, "response": body}, ensure_ascii=False))
                    return
                last_error = body
                if isinstance(body, dict):
                    detail = body.get("detail", {})
                    if isinstance(detail, dict):
                        reason = detail.get("reason")
                        if reason == "quorum_not_reached":
                            raise SystemExit("Quorum not reached: leader cannot replicate to enough nodes")
                        if detail.get("leaderId"):
                            leader_hint = leader_hint_to_url(detail["leaderId"], nodes)
            leader_hint = None
        raise SystemExit(f"Command failed on all nodes: {last_error}")

    if args.cmd == "get":
        for node in nodes:
            code, body = get_json(f"{node}/state/{args.key}")
            if code == 200:
                print(json.dumps({"node": node, "response": body}, ensure_ascii=False))
                return
        raise SystemExit("Read failed on all nodes")

    if args.cmd == "health":
        for node in nodes:
            code, body = get_json(f"{node}/health")
            print(json.dumps({"node": node, "status": code, "response": body}, ensure_ascii=False))


if __name__ == "__main__":
    main()
