from domain.error import (
    AuthenticationError,
    IngestError,
    InvalidPayloadError,
    MissingFieldError,
    PublishError,
)


def test_errors_instantiation():
    assert isinstance(IngestError(), Exception)
    assert isinstance(InvalidPayloadError(), IngestError)
    assert isinstance(MissingFieldError(), IngestError)
    assert isinstance(AuthenticationError(), IngestError)
    assert isinstance(PublishError(), IngestError)
