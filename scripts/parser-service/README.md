# Parser Service

Service nay doc cac file Python trong repo muc tieu, tao su kien CPG bang `ast`,
roi ghi ra JSONL khi `--dry-run` hoac publish len Kafka khi chay that.

Chay demo nho:

```bash
python scripts/parser-service/parser.py --repo transformers-pr-agent --limit 2 --dry-run --out-dir outputs/parser-output-demo
```
