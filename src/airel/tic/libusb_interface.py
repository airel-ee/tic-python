import dataclasses
from typing import Union, Set

import usb.core
import usb.util

from .encoding import decode, encode
from .exceptions import *

VUSB_VENDOR_ID = 0x16C0
VUSB_PRODUCT_ID = 0x27DD

TIC_IN_EP = 0x82
TIC_OUT_EP = 0x01
TIC_INTERFACE = 0

RECEIVE_BUFFER_SIZE = 10 * 1024


@dataclasses.dataclass
class DevUsbAddress:
    bus: int
    address: int
    serial_number: str


def find_all(exclude_bus_address: Union[set[tuple[(int, int)]], None] = None) -> [DevUsbAddress]:
    devices = []

    if exclude_bus_address is None:
        exclude_bus_address = {}

    def match(d):
        if (d.bus, d.address) in exclude_bus_address:
            return False

        if d.idVendor != VUSB_VENDOR_ID or d.idProduct != VUSB_PRODUCT_ID:
            return False

        if d.manufacturer != "Airel":
            return False

        if d.product != "TIC":
            return False

        return True

    try:
        for d in usb.core.find(find_all=True, custom_match=match):
            devices.append(DevUsbAddress(bus=d.bus, address=d.address, serial_number=d.serial_number))
    except ValueError as e:
        raise TicError(f"USB error: {e}") from None

    return devices


def _open_libusb_device(
    serial_number: Union[str, None], bus_address: Union[tuple[(int, int)], None] = None
) -> usb.core.Device:
    if serial_number in ["", "*", None]:
        serial_number = None

    devices = []

    def match(d):
        if bus_address is not None and (d.bus, d.address) != bus_address:
            return False

        if d.idVendor != VUSB_VENDOR_ID or d.idProduct != VUSB_PRODUCT_ID:
            return False

        if d.manufacturer != "Airel":
            return False

        if d.product != "TIC":
            return False

        if (serial_number is not None) and (d.serial_number != serial_number):
            return False

        return True

    for d in usb.core.find(find_all=True, custom_match=match):
        devices.append(d)

    if not devices:
        raise TicError(f"device not found")
    elif len(devices) > 1:
        devices_str = [d.serial_number for d in devices]
        raise TicError("found multiple matching devices: " + ", ".join(devices_str))
    else:
        return devices[0]


class LibusbInterface:
    def __init__(
        self, serial_number: Union[str, None], bus_address: Union[tuple[(int, int)], None] = None, debug: bool = False
    ):
        try:
            self.device = None
            self.device = _open_libusb_device(serial_number=serial_number, bus_address=bus_address)
            self.buf = bytearray()
            try:
                if self.device.is_kernel_driver_active(TIC_INTERFACE):
                    self.device.detach_kernel_driver(TIC_INTERFACE)
            except:
                usb.util.dispose_resources(self.device)
                raise
        except usb.core.USBError as e:
            raise CommunicationError(f"init error: {e}") from e
        except ValueError as e:
            raise CommunicationError(f"init error: {e}") from e

        self.debug = debug

    def write(self, data: bytes):
        bytes_out = encode(data) + b"\x00"
        if self.debug:
            print(f"write: {bytes_out}")
        try:
            self.device.write(TIC_OUT_EP, bytes_out, timeout=100)
        except usb.core.USBTimeoutError as e:
            raise ReceiveTimeout(f"write timeout: {e}") from e
        except usb.core.USBError as e:
            raise CommunicationError(f"write error: {e}") from e

    def read(self) -> bytes:
        while True:
            try:
                zero_pos = self.buf.index(0)
                break
            except ValueError:
                pass

            try:
                bytes_in = self.device.read(TIC_IN_EP, RECEIVE_BUFFER_SIZE, timeout=100)
                if self.debug and bytes_in:
                    print(f"read: {bytes(bytes_in)}")
                self.buf += bytes_in
            except usb.core.USBTimeoutError as e:
                raise ReceiveTimeout(f"read timeout: {e}") from e
            except usb.core.USBError as e:
                raise CommunicationError(f"read error: {e}") from e

        packet = self.buf[:zero_pos]
        del self.buf[: zero_pos + 1]
        return decode(packet)

    def flush_read(self):
        try:
            for _ in range(100):
                bytes_in = self.device.read(TIC_IN_EP, RECEIVE_BUFFER_SIZE, timeout=1)
                if self.debug and bytes_in:
                    print(f"flush read: {bytes(bytes_in)}")
                self.buf += bytes_in
        except usb.core.USBTimeoutError as e:
            pass
        except usb.core.USBError as e:
            raise CommunicationError(f"read error: {e}") from e

        try:
            pos = self.buf.rindex(0)
            del self.buf[: pos + 1]
        except ValueError:
            del self.buf[:]

    def close(self):
        if self.device is not None:
            try:
                usb.util.dispose_resources(self.device)
                self.device.attach_kernel_driver(TIC_INTERFACE)
                self.device = None
            except usb.core.USBError as e:
                # Ignore ENODEV and ENOENT errors
                if e.errno == 19 or e.errno == 2:
                    pass
                else:
                    raise CommunicationError(f"device close error: {e}") from e

            self.device = None

    def __del__(self):
        self.close()
