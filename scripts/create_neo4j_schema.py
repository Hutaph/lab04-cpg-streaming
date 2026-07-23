import sys
import time
import subprocess
from pathlib import Path


def load_env() -> dict[str, str]:
    """Manually parse .env file to extract environment variables without external dependencies."""
    env = {}
    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                env[key.strip()] = val.strip()
    return env


def wait_for_neo4j(password: str) -> bool:
    """Probes the Neo4j container using cypher-shell until it accepts connections."""
    print("Waiting for Neo4j to start and accept Cypher commands...")
    max_retries = 30
    for i in range(1, max_retries + 1):
        try:
            res = subprocess.run(
                [
                    "docker",
                    "exec",
                    "-i",
                    "cpg-neo4j",
                    "cypher-shell",
                    "-u",
                    "neo4j",
                    "-p",
                    password,
                    "RETURN 1;",
                ],
                capture_output=True,
                text=True,
            )
            if res.returncode == 0:
                print("Neo4j is healthy and responsive.")
                return True
            else:
                print(f"Probe failed with code {res.returncode}. Stderr: {res.stderr.strip()}")
        except Exception as e:
            print(f"Exception during probe: {e}")
        print(f"Neo4j is not ready yet. Retrying in 2 seconds... ({i}/{max_retries})")
        time.sleep(2)
    print("Error: Neo4j did not become healthy in time.")
    return False


def main() -> None:
    env = load_env()
    password = env.get("NEO4J_PASSWORD", "CHANGE_ME_NEO4J_PASSWORD")

    if not wait_for_neo4j(password):
        sys.exit(1)

    print("Applying Neo4j schema constraints and indexes...")
    queries = """
    // Enforce unique node IDs for idempotency
    CREATE CONSTRAINT cpg_node_id_unique IF NOT EXISTS
    FOR (node:CPGNode) REQUIRE node.id IS UNIQUE;

    // Enforce unique node tombstone keys
    CREATE CONSTRAINT cpg_tombstone_unique IF NOT EXISTS
    FOR (t:CPGNodeTombstone) REQUIRE (t.id, t.generation_id) IS UNIQUE;

    // Enforce unique edge tombstone keys (prevents duplicate tombstones on replay)
    CREATE CONSTRAINT cpg_edge_tombstone_unique IF NOT EXISTS
    FOR (t:CPGEdgeTombstone) REQUIRE (t.id, t.generation_id) IS UNIQUE;

    // Index on file_id for quick querying/filtering
    CREATE INDEX file_id_idx IF NOT EXISTS
    FOR (node:CPGNode) ON (node.file_id);

    // Index on relationships if supported by Cypher version
    CREATE INDEX edge_id_idx IF NOT EXISTS
    FOR ()-[r:CPG_EDGE]-() ON (r.edge_id);
    """

    # Execute Cypher script inside Neo4j container
    res = subprocess.run(
        ["docker", "exec", "-i", "cpg-neo4j", "cypher-shell", "-u", "neo4j", "-p", password],
        input=queries,
        capture_output=True,
        text=True,
    )

    if res.returncode == 0:
        print("Schema constraints and indexes bootstrapped successfully.")
        print(res.stdout)
        sys.exit(0)
    else:
        print(f"Failed to bootstrap Neo4j schema: {res.stderr}")
        print(res.stdout)
        sys.exit(1)


if __name__ == "__main__":
    main()
