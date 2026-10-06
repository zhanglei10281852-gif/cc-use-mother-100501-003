"""进程重启恢复：继续未决迁移并补跑到期清扫。

所有恢复动作都建立在持久化状态机上，且本身幂等：
重复执行 recover 不会产生额外副作用。
"""

from __future__ import annotations


def recover(backend) -> dict:
    """恢复入口：先续跑 PLANNED 迁移，再执行到期清扫。"""
    resumed = backend.lifecycle.recover_pending_migrations()
    sweep = backend.lifecycle.sweep()
    return {
        "pending_migrations_resumed": resumed,
        "sweep": sweep,
    }
