from functools import wraps
from typing import Any, Callable

from flask import current_app, request

from domain.error import AuthenticationError


class SourceAuthVerifier:
    def __init__(self, token: str) -> None:
        self._token = token

    def verify(self, token: str) -> bool:
        return token == self._token


def require_auth(f: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(f)
    def decorated(*args: Any, **kwargs: Any) -> Any:
        verifier = current_app.config['auth_verifier']
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            raise AuthenticationError()
        token = auth_header.split(' ')[1]
        if not verifier.verify(token):
            raise AuthenticationError()
        return f(*args, **kwargs)

    return decorated
