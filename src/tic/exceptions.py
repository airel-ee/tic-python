class TicError(Exception):
    pass


class CommunicationError(TicError):
    pass


class ReceiveTimeout(TicError):
    pass


class EncodingError(TicError):
    pass


class DecodingError(TicError):
    pass


class DeviceErrorResponse(TicError):
    def __init__(self, message: str, error_code: str):
        if message:
            super().__init__(f"{error_code}: {message}")
        else:
            super().__init__(error_code)

