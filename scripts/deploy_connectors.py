import os
import sys
import json
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any, cast

CONNECT_URL = "http://localhost:8083"


def load_env() -> dict[str, str]:
    """Manually parse .env file and merge with system environment variables."""
    env = {}
    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                env[key.strip()] = val.strip()
    for k, v in os.environ.items():
        env[k] = v
    return env


def make_request(
    url: str, method: str = "GET", data: Any = None, headers: dict[str, str] | None = None
) -> tuple[int, Any]:
    """Execute HTTP requests without external dependencies."""
    if headers is None:
        headers = {}
    if data is not None:
        if isinstance(data, dict):
            data = json.dumps(data).encode("utf-8")
            headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, method=method, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        try:
            body = json.loads(err.read().decode("utf-8"))
        except Exception:
            body = err.reason
        return err.code, body
    except urllib.error.URLError as err:
        return 500, str(err.reason)


def get_connector_config(name: str) -> dict[str, Any] | None:
    code, res = make_request(f"{CONNECT_URL}/connectors/{name}/config")
    if code == 200:
        return cast(dict[str, Any], res)
    return None


def mask_sensitive(text: str, secret: str) -> str:
    """Mask sensitive passwords in log output."""
    if secret and secret != "CHANGE_ME_NEO4J_PASSWORD":
        return text.replace(secret, "[hidden]")
    return text


def deploy_connector(name: str, config: dict[str, Any], secret_val: str) -> bool:
    current_config = get_connector_config(name)
    is_drift = False

    if current_config is None:
        is_drift = True
        print(f"Connector '{name}' does not exist. Deploying fresh...")
    else:
        # Inspect drift (ignore password values which are masked by Connect as '[hidden]')
        for k, v in config.items():
            if "password" in k.lower() or "secret" in k.lower():
                if k not in current_config:
                    is_drift = True
                    print(f"Drift detected for sensitive key '{k}': missing in current configuration.")
                    break
            else:
                if current_config.get(k) != v:
                    is_drift = True
                    print(f"Drift detected for key '{k}': target '{v}', current '{current_config.get(k)}'")
                    break
        for k in current_config:
            if k == "name":
                continue
            if k not in config:
                is_drift = True
                print(f"Drift detected: key '{k}' is in current configuration but not target.")
                break

    if is_drift:
        if current_config is None:
            # Create fresh
            payload = {"name": name, "config": config}
            code, res = make_request(f"{CONNECT_URL}/connectors", method="POST", data=payload)
            if code not in [200, 201]:
                err_msg = mask_sensitive(str(res), secret_val)
                print(f"Failed to create connector '{name}': {err_msg}")
                return False
            print(f"Connector '{name}' created successfully.")
        else:
            # Update config
            code, res = make_request(f"{CONNECT_URL}/connectors/{name}/config", method="PUT", data=config)
            if code not in [200, 201]:
                err_msg = mask_sensitive(str(res), secret_val)
                print(f"Failed to update connector '{name}': {err_msg}")
                return False
            print(f"Connector '{name}' updated successfully (drift resolved).")
    else:
        print(f"Connector '{name}' configuration matches target. No redeployment needed.")
    return True


def wait_for_running(name: str, secret_val: str, timeout: int = 60) -> bool:
    print(f"Waiting for connector '{name}' and its tasks to transition to RUNNING...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        code, res = make_request(f"{CONNECT_URL}/connectors/{name}/status")
        if code == 200:
            connector_state = res.get("connector", {}).get("state", "UNKNOWN")
            tasks = res.get("tasks", [])

            all_running = True
            if connector_state != "RUNNING":
                all_running = False

            for task in tasks:
                task_state = task.get("state", "UNKNOWN")
                if task_state == "FAILED":
                    trace = mask_sensitive(task.get("trace", ""), secret_val)
                    print(f"[ERROR] Task {task.get('id')} in connector '{name}' FAILED: {trace}")
                    return False
                if task_state != "RUNNING":
                    all_running = False

            if all_running and tasks:
                print(f"Connector '{name}' tasks are all RUNNING.")
                return True
        time.sleep(2)
    print(f"Timeout waiting for connector '{name}' to reach RUNNING status.")
    return False


def main() -> None:
    env = load_env()
    secret_val = env.get("NEO4J_PASSWORD", "")

    connector_dir = Path("infra/kafka-connect/connectors")
    if not connector_dir.exists():
        print(f"Connectors config directory not found: {connector_dir}")
        sys.exit(1)

    success = True
    for f in sorted(connector_dir.glob("*.json")):
        try:
            raw_content = f.read_text(encoding="utf-8")
            # Replace placeholder variables with env values
            for k, v in env.items():
                raw_content = raw_content.replace(f"${{{k}}}", v)

            data = json.loads(raw_content)
            name = data.get("name")
            config = data.get("config")

            if not name or not config:
                print(f"Invalid config format in {f.name}")
                success = False
                continue

            if not deploy_connector(name, config, secret_val):
                success = success and False
                continue

            if not wait_for_running(name, secret_val):
                success = success and False
                continue
        except Exception as exc:
            print(f"Error deploying connector config {f.name}: {exc}")
            success = False

    if success:
        print("All connectors successfully verified and deployed.")
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
