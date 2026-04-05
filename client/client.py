import argparse
import json
import urllib.error
from urllib.parse import urlparse
import urllib.request


def post_json(url: str, payload: dict) -> tuple[int, dict | str]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
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


def get_json(url: str) -> tuple[int, dict | str]:
    try:
        with urllib.request.urlopen(url, timeout=2) as resp:
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple Raft client")
    parser.add_argument("--nodes", default="http://localhost:8001,http://localhost:8002,http://localhost:8003")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_cmd = sub.add_parser("command")
    p_cmd.add_argument("--key", required=True)
    p_cmd.add_argument("--value", required=True)

    p_get = sub.add_parser("get")
    p_get.add_argument("--key", required=True)

    sub.add_parser("health")

    args = parser.parse_args()
    nodes = [n.strip().rstrip("/") for n in args.nodes.split(",") if n.strip()]

    if args.cmd == "command":
        leader_hint = None
        for _ in range(2):
            check_nodes = [leader_hint] + nodes if leader_hint else nodes
            for node in check_nodes:
                if not node:
                    continue
                code, body = post_json(f"{node}/command", {"key": args.key, "value": args.value})
                if code == 200:
                    print(json.dumps({"node": node, "response": body}, ensure_ascii=False))
                    return
                if isinstance(body, dict):
                    detail = body.get("detail", {})
                    if isinstance(detail, dict) and detail.get("leaderId"):
                        leader_hint = leader_hint_to_url(detail["leaderId"], nodes)
            leader_hint = None
        raise SystemExit("Command failed on all nodes")

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
