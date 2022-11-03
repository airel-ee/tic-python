from typing import Union

import serial

from .encoding import decode, encode
from .exceptions import *


class SerialInterface:
    def __init__(self, port_name: Union[str, None], debug: bool = False):
        try:
            self.port = None
            self.port = serial.Serial(port_name, timeout=0.1)
            self.buf = bytearray()
        except serial.SerialException as e:
            raise CommunicationError(f"init error: {e}") from e

        self.debug = debug

    def write(self, data: bytes):
        bytes_out = encode(data) + b"\x00"
        if self.debug:
            print(f"write: {bytes_out}")
        try:
            self.port.write(bytes_out)
        except serial.SerialException as e:
            raise CommunicationError(f"write error: {e}") from e

    def read(self) -> bytes:
        while True:
            try:
                pos = self.buf.index(0)
                break
            except ValueError:
                pass

            try:
                bytes_in = self.port.read_until(b"\x00", size=4096)
                if self.debug and bytes_in:
                    print(f"read: {bytes(bytes_in)}")
                self.buf += bytes_in
            except serial.SerialException as e:
                raise CommunicationError(f"read error: {e}") from e

        packet = self.buf[:pos]
        del self.buf[:pos + 1]
        return decode(packet)

    def flush_read(self):
        try:
            for _ in range(100):
                bytes_in = self.port.read_until(b"\x00", size=4096)
                if self.debug and bytes_in:
                    print(f"flush read: {bytes(bytes_in)}")
                if (len(bytes_in) < 4096) and (0 not in bytes_in):
                    break
                self.buf += bytes_in
        except serial.SerialException as e:
            raise CommunicationError(f"read error: {e}") from e

        try:
            pos = self.buf.rindex(0)
            del self.buf[:pos + 1]
        except ValueError:
            del self.buf[:]

    def close(self):
        if self.port is not None:
            try:
                self.port.close()
            except serial.SerialException as e:
                raise CommunicationError(f"device close error: {e}") from e

            self.port = None

    def __del__(self):
        self.close()
