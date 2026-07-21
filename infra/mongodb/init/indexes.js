// MongoDB Indexes initialization script
// Target database: cpg_metadata, target collection: file_statistics

db = db.getSiblingDB("cpg_metadata");

// Enforce unique index on file_id
db.file_statistics.createIndex(
    { "file_id": 1 },
    { "unique": true }
);

// Enforce unique index on combined repository_id and file_path
db.file_statistics.createIndex(
    { "repository_id": 1, "file_path": 1 },
    { "unique": true }
);
