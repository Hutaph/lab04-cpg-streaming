from pathlib import Path
import subprocess

# def show_tree(path:Path, prefix: str = "", max_depth: int = 3, depth: int = 0):
#     if depth > max_depth:
#         return

#     items = sorted(
#         [p for p in path.iterdir() if p.name not in exclude_dirs],
#         key=lambda p: (p.is_file(), p.name.lower())
#     )

#     for index, item in enumerate(items):
#         connector = "└── " if index == len(items) - 1 else "├── "
#         print(prefix + connector + item.name)

#         if item.is_dir():
#             extension = "    " if index == len(items) - 1 else "│   "
#             show_tree(item, prefix + extension, max_depth, depth + 1)

repo_path = Path("transformers-pr-agent")

exclude_dirs = {".git", "tests", "__pycache__", ".pytest_cache"}

py_files = [
    file for file in repo_path.rglob("*.py")
    if not any(part in exclude_dirs for part in file.parts)
]

commit_hash = subprocess.check_output(
    ["git", "-C", str(repo_path), "rev-parse", "HEAD"],
    text=True
).strip()

print("Repository:", repo_path)
# show_tree(repo_path, max_depth=3)
print("Commit hash:", commit_hash)
print("Total Python files:", len(py_files))

print("\nFirst 20 Python files:")
for file in py_files[:20]:
    print(file)