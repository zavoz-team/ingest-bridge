from domain.error import IngestError


class ValidationError(IngestError):
    pass


class PublishFailedError(IngestError):
    pass
