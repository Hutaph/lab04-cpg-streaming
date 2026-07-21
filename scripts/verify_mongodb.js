// Verification shell commands for MongoDB stats collection state
// TODO: Implement checks in Phase 6 / Phase 13

db = db.getSiblingDB("cpg_metadata");

// Count total documents
print("Total metadata documents: " + db.file_statistics.countDocuments());

// Show sample records
printjson(db.file_statistics.find().limit(3).toArray());
