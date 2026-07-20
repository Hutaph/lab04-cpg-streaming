# Sơ đồ Kiến trúc Hệ thống

Báo cáo chi tiết về kiến trúc hệ thống trích xuất Code Property Graph streaming tăng dần.

*(Nội dung đồng bộ với tài liệu `docs/system_architecture.md`)*

## Sơ đồ luồng tổng thể

```mermaid
graph TD
    SourceRepo["Source Repository (huggingface/transformers-pr-agent)"] -->|"shallow clone"| FileDiscovery["File Discovery"]
    FileDiscovery -->|"từng file Python"| CpgParser["CPG Parser Service"]
    
    subgraph Parser Service Internal
        CpgParser --> AST["AST Builder"]
        CpgParser --> CFG["CFG Builder"]
        CpgParser --> DFG["DFG Builder"]
        CpgParser --> Call["Call Graph Builder"]
        CpgParser --> Meta["Metadata Extractor"]
        CpgParser --> StableId["Stable ID Generator"]
        CpgParser --> StateStore[("SQLite State Store")]
    end
    
    CpgParser -->|"Publish"| KafkaBroker{"Apache Kafka Broker"}
    
    subgraph Kafka Topics
        KafkaBroker --> TopicNodes["cpg.nodes"]
        KafkaBroker --> TopicEdges["cpg.edges"]
        KafkaBroker --> TopicMetadata["source.metadata"]
        KafkaBroker --> TopicErrors["parser.errors"]
    end
    
    TopicNodes --> Neo4jSink["Neo4j Kafka Sink Connector"]
    TopicEdges --> Neo4jSink
    Neo4jSink -->|"MERGE Cypher"| Neo4jDb[("Neo4j Graph Database")]
    
    TopicMetadata --> SparkStreaming["Spark Structured Streaming"]
    SparkStreaming -->|"MongoDB Spark Connector"| MongoDb[("MongoDB Document Database")]
```
