// Constraints and Indexes for Code Property Graph on Neo4j

// Enforce unique node IDs for idempotency
CREATE CONSTRAINT unique_node_id IF NOT EXISTS
FOR (n:CodeNode) REQUIRE n.node_id IS UNIQUE;

// Index on file_id for quick querying/filtering
CREATE INDEX file_id_idx IF NOT EXISTS
FOR (n:CodeNode) ON (n.file_id);

// Index on relationships if supported by Cypher version
CREATE INDEX edge_id_idx IF NOT EXISTS
FOR ()-[r:CPG_EDGE]-() ON (r.edge_id);
