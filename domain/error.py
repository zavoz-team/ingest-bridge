class IngestError(Exception):
    pass


class InvalidPayloadError(IngestError):
    pass


class MissingFieldError(IngestError):
    pass


class AuthenticationError(IngestError):
    pass


class PublishError(IngestError):
    pass
