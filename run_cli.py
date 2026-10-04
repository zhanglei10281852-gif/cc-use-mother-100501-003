"""跨节点算力预留与履约命令行冒烟入口。"""

import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from compute_reservation import ComputeReservation


def main() -> None:
    item = ComputeReservation(reservation_code='reservation-code-001', tenant_code='tenant-code-001', capacity_batch='capacity-batch-001', state='state-001')
    print(json.dumps({"item": asdict(item), "fingerprint": item.fingerprint()}, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
