from typing import Any

from flask import Flask, jsonify
from werkzeug.exceptions import HTTPException

from domain.error import (
    AuthenticationError,
    InvalidPayloadError,
    MissingFieldError,
    PublishError,
)
from usecase.error import PublishFailedError


def register_error_handlers(app: Flask) -> None:
    @app.errorhandler(InvalidPayloadError)
    def handle_invalid_payload(error: Exception) -> tuple[Any, int]:
        return jsonify({'error': 'bad_request'}), 400

    @app.errorhandler(MissingFieldError)
    def handle_missing_field(error: Exception) -> tuple[Any, int]:
        return jsonify({'error': 'unprocessable_entity'}), 422

    @app.errorhandler(AuthenticationError)
    def handle_auth_error(error: Exception) -> tuple[Any, int]:
        return jsonify({'error': 'unauthorized'}), 401

    @app.errorhandler(PublishError)
    def handle_publish_error(error: Exception) -> tuple[Any, int]:
        return jsonify({'error': 'service_unavailable'}), 503

    @app.errorhandler(PublishFailedError)
    def handle_publish_failed_error(error: Exception) -> tuple[Any, int]:
        return jsonify({'error': 'service_unavailable'}), 503

    @app.errorhandler(HTTPException)
    def handle_http_exception(error: HTTPException) -> tuple[Any, int]:
        return jsonify({'error': error.name.lower().replace(' ', '_')}), error.code or 500
