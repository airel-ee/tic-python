from cobs import cobs
from .exceptions import DecodingError


def update_checksum(crc, data):
    for byte in data:
        crc = crc ^ (byte << 8)

        for i in range(8):
            if crc & 0x8000 != 0:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF

    return crc


def decode(packet: bytes) -> bytes:
    if len(packet) == 0:
        return b""

    try:
        contents = cobs.decode(packet)
    except cobs.DecodeError as e:
        print(packet)
        raise DecodingError(f"decoding error: {e}")

    if len(contents) < 2:
        raise DecodingError("packet too short")

    crc = update_checksum(0, contents[:-2])
    crc_bytes = crc.to_bytes(2, 'little')
    if crc_bytes != contents[-2:]:
        raise DecodingError("invalid crc")

    return contents[:-2]


def encode(payload: bytes) -> bytes:
    if len(payload) == 0:
        return b""
    crc = update_checksum(0, payload)
    return cobs.encode(payload + crc.to_bytes(2, 'little'))
