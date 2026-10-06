"""统一领域错误定义。"""

from __future__ import annotations


class DomainError(Exception):
    """携带业务错误码与 HTTP 状态码的领域异常。"""

    def __init__(self, code: str, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status

    def to_dict(self) -> dict:
        return {"error": {"code": self.code, "message": self.message}}


def not_found(message: str) -> DomainError:
    return DomainError("not_found", message, 404)


def conflict(message: str, code: str = "conflict") -> DomainError:
    return DomainError(code, message, 409)


def validation(message: str) -> DomainError:
    return DomainError("validation", message, 422)


def forbidden(message: str) -> DomainError:
    return DomainError("forbidden", message, 403)
