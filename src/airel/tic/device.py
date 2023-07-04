import collections
import json
import time
from typing import Any, Dict, Optional, Union
import random

try:
    from .libusb_interface import LibusbInterface
except ImportError:
    LibusbInterface = None

try:
    from .serial_interface import SerialInterface
except ImportError:
    SerialInterface = None

from .exceptions import EncodingError, ReceiveTimeout, DecodingError, DeviceErrorResponse, TicError

CONNECTION_INIT_TIMEOUT = 1.0


class Tic:
    """
    Tiny Ion Counter connection class
    """

    def __init__(self, port_name: Union[str, None] = None):
        """
        Opens a connection to a TIC device

        :param port_name: A string specifying what device to use.

        May throw TicError.

        ``port_name`` may be one of:

            * ``None`` or ``""``:
                Automatically finds a TIC connected to the computer and connects to that using libusb.
                Requires pyusb package.
                Throws an error if more than one or no TICs are found.

            * serial number of the device:
                Connects to the TIC with the specified using libusb.
                Throws an error if device with given serial number is not found.

                Eg. ``"0107E60A0101"``

            * serial number prefixed with ``"usb:"``:
                Same as previous but explicitly tells to use libusb connection

                Eg. ``"usb:0107E60A0101"``

            * name of a serial port prefixed with ``"serial:"``:
                Connects to the TIC as a serial port device.
                Requires pyserial package.

                Eg. ``"serial:/dev/ttyACM0"`` or ``"serial:/dev/serial/by-id/usb-Airel_TIC_0107E60A0101-if00"``

        """

        self.port = None
        debug_comm = False

        if port_name and port_name.startswith("usb:"):
            if LibusbInterface is None:
                raise TicError(
                    "LibUSB connection not supported. Please check if pyusb package is installed.")
            self.port = LibusbInterface(port_name[4:], debug=debug_comm)
        elif port_name and port_name.startswith("serial:"):
            if SerialInterface is None:
                raise TicError(
                    "Serial connection not supported. Please check if pyserial package is installed.")
            self.port = SerialInterface(port_name[7:], debug=debug_comm)
        else:
            if LibusbInterface is None:
                raise TicError(
                    "LibUSB connection not supported. Please check if pyusb package is installed or use serial connection")
            self.port = LibusbInterface(port_name, debug=debug_comm)

        self.message_queue = collections.deque()
        self._init_connection()

    def close(self):
        """
        Closes connection to the device and releases all hardware resources held by the connection

        The method is automatically called when the object is destroyed. However, it is useful to explicitly call this
        method when it is necessary to restart the connection to the device (e.g. due to an error response) to make
        sure that the previous connection is closed and the device is available for a new connection.
        """

        if self.port:
            self.port.close()

    def __del__(self):
        self.close()

    def _receive_response(self, timeout: float = 1.0) -> Any:
        tend = time.time() + timeout
        while time.time() < tend:
            try:
                payload = self.port.read()
                msg = json.loads(payload)
            except ReceiveTimeout:
                continue
            except json.JSONDecodeError as e:
                raise DecodingError(f"invalid json message") from e

            if "result" in msg:
                return msg["result"]
            elif "error" in msg:
                error = msg["error"]
                raise DeviceErrorResponse(error.get("msg", ""), error_code=error.get("code", None))
            else:
                self.message_queue.append(msg)

        raise ReceiveTimeout

    def receive_message(self, timeout: float = 1.0) -> Union[Any, None]:
        """
        Returns next message received from the device

        The message may be returned from the internal FIFO buffer of the object where it was stored when a response to a
        command to the device was being expected.

        Raises :py:exc:`ReceiveTimeout` if no message is received within the timeout period.

        Raises DeviceErrorResponse exception if the message is an error response.

        :param timeout: timeout in seconds
        :return: result message received from the device
        """
        if self.message_queue:
            return self.message_queue.popleft()
        elif timeout == 0:
            return None
        else:
            tend = time.time() + timeout
            while time.time() < tend:
                try:
                    payload = self.port.read()
                    if len(payload) == 0:
                        continue
                    msg = json.loads(payload)
                except ReceiveTimeout:
                    continue
                except json.JSONDecodeError as e:
                    raise DecodingError(f"invalid json message") from e

                if "error" in msg:
                    error = msg["error"]
                    raise DeviceErrorResponse(error.get("msg", "unknown"), error_code=error.get("code", None))

                return msg

            return None

    def _wait_ok_response(self, timeout: float = 1.0):
        response = self._receive_response(timeout)
        if response != "ok":
            raise TicError(f"Unexpected response: {response!r}")

    def _send_json_msg(self, value: Any):
        try:
            msg = json.dumps(value, allow_nan=False).encode("utf8")
        except ValueError as e:
            raise EncodingError("Failed to encode message") from e

        self.port.write(msg)

    def _init_connection(self):
        self.port.write(b"")
        self.port.flush_read()
        ping_payload = str(random.randrange(0, 1000000000))
        self._send_json_msg({"method": "ping", "params": ping_payload})
        tend = time.time() + CONNECTION_INIT_TIMEOUT
        while time.time() < tend:
            try:
                resp = self.receive_message(timeout=0.1)
            except DecodingError:
                continue

            if resp is not None:
                if resp.get("result", "") == ping_payload:
                    return

        raise ReceiveTimeout

    def ping(self, payload: str) -> str:
        """
        Sends a ping command to the device

        :param payload: string payload of the ping message
        :return: the same payload received from the device
        """
        self._send_json_msg({"method": "ping", "params": payload})
        return self._receive_response()

    def get_system_info(self) -> Dict[str, Any]:
        """
        Requests system information from the device

        :return: system information dict
        """
        self._send_json_msg({"method": "get_system_info"})
        return self._receive_response()

    def get_debug_info(self) -> Dict[str, Any]:
        """
        Requests debug information from the device

        :return: debug information dict
        """
        self._send_json_msg({"method": "get_debug_info"})
        return self._receive_response()

    def get_settings(self) -> Dict[str, Any]:
        """
        Requests user settings from the device

        :return: debug information dict
        """
        self._send_json_msg({"method": "get_settings"})
        return self._receive_response()

    def set_settings(self, settings_map: Dict[str, Any]):
        """
        Updates user settings on the device

        :param settings_map: dict of settings
        """
        self._send_json_msg({"method": "set_settings", "params": settings_map})
        self._wait_ok_response()

    def reset_settings(self, settings_map: Optional[Dict[str, Any]] = None):
        """
        Resets and update user settings on the device

        :param settings_map: (Optional) dict of settings to apply after reset
        """
        if settings_map:
            self._send_json_msg({"method": "reset_settings", "params": settings_map})
        else:
            self._send_json_msg({"method": "reset_settings"})
        self._wait_ok_response()

    def store_settings(self):
        """
        Stores the currently active user settings in the non-volatile memory of the device
        """
        self._send_json_msg({"method": "store_settings"})
        self._wait_ok_response()

    def hard_reset(self):
        """
        Requests an MCU reset of the device

        The Tic will restart and connection will be lost. After this method :py:func:`close` should be called and the
        object destroyed. Some exceptions may still be thrown.
        """
        self._send_json_msg({"method": "hard_reset"})

    def enter_dfu(self):
        """
        Requests the MCU to reset and enter firware update mode

        The Tic will restart and connection will be lost. After this method :py:func:`close` should be called and the
        object destroyed. Some exceptions may still be thrown.
        """
        self._send_json_msg({"method": "enter_dfu"})

    def set_mode(self, mode: str):
        """
        Sets the device operating mode

        :param mode: New operating mode of the device. Valid modes are: run, run_swapped, zero, stop.
        """
        self._send_json_msg({"method": "set_mode", "params": mode})
        self._wait_ok_response()

    def set_custom_mode(self, params: Dict[str, Any]):
        """
        Sets the device operating mode to a custom mode
        """
        self._send_json_msg({"method": "set_custom_mode", "params": params})
        self._wait_ok_response()

    def get_flag_descriptions(self):
        """
        Requests textual descriptions of the record flags that the device uses

        :return: dict of flag descriptions
        """
        self._send_json_msg({"method": "get_flag_descriptions"})
        response = self._receive_response()
        return {k: v for (k, v) in response}
