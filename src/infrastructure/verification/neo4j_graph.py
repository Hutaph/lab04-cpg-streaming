"""Neo4j CPG graph validation and verification utilities."""

import csv
from dataclasses import dataclass
import subprocess
from typing import Any


def _cypher_string(value: str) -> str:
    """Escapes a Python string for a single-quoted Cypher literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def run_cypher(query: str, password: str) -> list[list[str]]:
    """Runs a Cypher query using cypher-shell via docker exec and parses CSV output."""
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
                "--format",
                "plain",
            ],
            input=query,
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode != 0:
            raise RuntimeError(f"Cypher query failed with exit code {res.returncode}: {res.stderr.strip()}")

        # cypher-shell --format plain prints header and lines. Parse it with csv reader.
        lines = res.stdout.strip().splitlines()
        results: list[list[str]] = []
        reader = csv.reader(lines, skipinitialspace=True)
        for row in reader:
            if row:
                results.append(row)
        return results
    except Exception as exc:
        raise RuntimeError(f"Failed to execute Cypher query: {exc}")


def get_constraints(password: str) -> list[dict[str, str]]:
    """Retrieves all defined constraints from Neo4j."""
    rows = run_cypher("SHOW CONSTRAINTS;", password)
    constraints = []
    if len(rows) > 1:
        headers = [h.strip().lower() for h in rows[0]]
        for row in rows[1:]:
            c_dict = {}
            for idx, val in enumerate(row):
                if idx < len(headers):
                    c_dict[headers[idx]] = val.replace('"', "").strip()
            constraints.append(c_dict)
    return constraints


def verify_required_constraints(password: str) -> None:
    """Asserts that required uniqueness constraints are present in the Neo4j schema."""
    constraints = get_constraints(password)
    constraint_names = {c.get("name") for c in constraints if c.get("name")}

    required = ["cpg_node_id_unique", "cpg_tombstone_unique", "cpg_edge_tombstone_unique"]
    for req in required:
        if req not in constraint_names:
            raise AssertionError(f"Required uniqueness constraint '{req}' is missing from Neo4j schema")


@dataclass(frozen=True)
class GraphCounts:
    """Node and edge totals scoped to one source file."""

    node_count: int
    edge_count: int


def get_graph_counts(password: str, file_id: str) -> GraphCounts:
    """Gets the count of nodes and edges matching a specific file_id."""
    file_id_literal = _cypher_string(file_id)
    node_res = run_cypher(
        f"MATCH (n:CPGNode) WHERE n.file_id = {file_id_literal} RETURN count(n);",
        password,
    )
    edge_res = run_cypher(
        f"MATCH ()-[r:CPG_EDGE]->() WHERE r.file_id = {file_id_literal} RETURN count(r);",
        password,
    )

    node_cnt = int(node_res[1][0]) if len(node_res) > 1 else 0
    edge_cnt = int(edge_res[1][0]) if len(edge_res) > 1 else 0
    return GraphCounts(node_cnt, edge_cnt)


def find_duplicate_nodes(password: str, file_id: str | None = None) -> list[str]:
    """Finds duplicate CPGNode IDs globally or scoped to a file_id."""
    scope = f"WHERE n.file_id = {_cypher_string(file_id)}" if file_id else ""
    query = f"MATCH (n:CPGNode) {scope} WITH n.id AS nid, count(n) AS c WHERE c > 1 RETURN nid;"
    rows = run_cypher(query, password)
    duplicates = []
    if len(rows) > 1:
        duplicates = [row[0] for row in rows[1:] if row]
    return duplicates


def find_duplicate_edges(password: str, file_id: str | None = None) -> list[str]:
    """Finds duplicate CPG_EDGE edge_ids globally or scoped to a file_id."""
    scope = f"WHERE r.file_id = {_cypher_string(file_id)}" if file_id else ""
    query = f"MATCH ()-[r:CPG_EDGE]->() {scope} WITH r.edge_id AS eid, count(r) AS c WHERE c > 1 RETURN eid;"
    rows = run_cypher(query, password)
    duplicates = []
    if len(rows) > 1:
        duplicates = [row[0] for row in rows[1:] if row]
    return duplicates


def find_null_graph_properties(password: str, file_id: str | None = None) -> dict[str, list[str]]:
    """Finds nodes or edges with null critical properties (file_id or generation_id)."""
    scope_n = f"n.file_id = {_cypher_string(file_id)} AND" if file_id else ""
    node_query = f"MATCH (n:CPGNode) WHERE {scope_n} (n.file_id IS NULL OR n.generation_id IS NULL) RETURN n.id;"
    node_rows = run_cypher(node_query, password)
    null_nodes = [row[0] for row in node_rows[1:] if row] if len(node_rows) > 1 else []

    scope_r = f"r.file_id = {_cypher_string(file_id)} AND" if file_id else ""
    edge_query = (
        f"MATCH ()-[r:CPG_EDGE]->() WHERE {scope_r} (r.file_id IS NULL OR r.generation_id IS NULL) RETURN r.edge_id;"
    )
    edge_rows = run_cypher(edge_query, password)
    null_edges = [row[0] for row in edge_rows[1:] if row] if len(edge_rows) > 1 else []

    return {"nodes": null_nodes, "edges": null_edges}


def find_placeholders(password: str, file_id: str | None = None) -> list[str]:
    """Finds unresolved placeholder CPGNodes globally or scoped to a file_id."""
    scope = f"AND n.file_id = {_cypher_string(file_id)}" if file_id else ""
    query = f"MATCH (n:CPGNode {{placeholder: true}}) WHERE 1=1 {scope} RETURN n.id;"
    rows = run_cypher(query, password)
    placeholders = []
    if len(rows) > 1:
        placeholders = [row[0] for row in rows[1:] if row]
    return placeholders


def get_tombstone_summary(password: str, file_id: str | None = None) -> dict[str, Any]:
    """Summarizes node and edge tombstone counts and checks for duplicate/malformed tombstones."""
    scope_n = f"WHERE t.file_id = {_cypher_string(file_id)}" if file_id else ""
    node_ts = run_cypher(f"MATCH (t:CPGNodeTombstone) {scope_n} RETURN count(t);", password)
    node_ts_cnt = int(node_ts[1][0]) if len(node_ts) > 1 else 0

    scope_e = f"WHERE t.file_id = {_cypher_string(file_id)}" if file_id else ""
    edge_ts = run_cypher(f"MATCH (t:CPGEdgeTombstone) {scope_e} RETURN count(t);", password)
    edge_ts_cnt = int(edge_ts[1][0]) if len(edge_ts) > 1 else 0

    dup_node_query = (
        f"MATCH (t:CPGNodeTombstone) {scope_n} "
        "WITH t.id AS id, t.generation_id AS gen, count(t) AS c "
        "WHERE c > 1 RETURN count(id);"
    )
    dup_node_res = run_cypher(dup_node_query, password)
    dup_node_cnt = int(dup_node_res[1][0]) if len(dup_node_res) > 1 else 0

    dup_edge_query = (
        f"MATCH (t:CPGEdgeTombstone) {scope_e} "
        "WITH t.id AS id, t.generation_id AS gen, count(t) AS c "
        "WHERE c > 1 RETURN count(id);"
    )
    dup_edge_res = run_cypher(dup_edge_query, password)
    dup_edge_cnt = int(dup_edge_res[1][0]) if len(dup_edge_res) > 1 else 0

    mal_n_scope = (
        f"(t.file_id IS NULL OR t.generation_id IS NULL) AND t.file_id = {_cypher_string(file_id)}"
        if file_id
        else "t.file_id IS NULL OR t.generation_id IS NULL"
    )
    mal_query = f"MATCH (t) WHERE (t:CPGNodeTombstone OR t:CPGEdgeTombstone) AND ({mal_n_scope}) RETURN count(t);"
    mal_res = run_cypher(mal_query, password)
    mal_cnt = int(mal_res[1][0]) if len(mal_res) > 1 else 0

    return {
        "node_tombstone_count": node_ts_cnt,
        "edge_tombstone_count": edge_ts_cnt,
        "duplicate_node_tombstones": dup_node_cnt,
        "duplicate_edge_tombstones": dup_edge_cnt,
        "malformed_tombstones": mal_cnt,
    }
