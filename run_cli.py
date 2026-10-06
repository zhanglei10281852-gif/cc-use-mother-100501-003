"""跨节点算力预留与履约命令行入口。

不带参数时执行基础契约冒烟；完整功能见子命令（--help）。
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from compute_reservation.cli import main

if __name__ == "__main__":
    argv = sys.argv[1:] or ["smoke"]
    raise SystemExit(main(argv))
