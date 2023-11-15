class TicError(Exception):
    """
    Base class for all Tic exceptions
    """

    pass


class CommunicationError(TicError):
    """
    Communication error related to the virtual serial port
    """

    pass


class ReceiveTimeout(TicError):
    """
    Timeout error
    """

    pass


class EncodingError(TicError):
    """
    Error when encoding message to be transmitted to the device
    """

    pass


class DecodingError(TicError):
    """
    Error when decoding message from the device
    """

    pass


class DeviceErrorResponse(TicError):
    """
    Error sent from the device as a response to a command
    """

    def __init__(self, message: str, error_code: str):
        if message:
            super().__init__(f"{error_code}: {message}")
        else:
            super().__init__(error_code)
