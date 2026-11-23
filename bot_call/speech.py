"""Bounded, call-owned synthesis values; no Provider, network or startup IO."""

from dataclasses import dataclass, field

MAX_PUSH_AUDIO_BYTES = 48 * 1024
MAX_SPEECH_BYTES = 8388608
MAX_SPEECH_TEXT = 6000


class SpeechUnavailable(RuntimeError):
    def __init__(self):
        super().__init__("bot_call.tts_unavailable")


@dataclass(frozen=True, slots=True)
class SpeechJob:
    call_id: str = field(repr=False)
    generation_id: str = field(repr=False)
    state: str
    data: bytes | None = field(default=None, repr=False)

    def public(self):
        return {"generation_id": self.generation_id, "state": self.state}


def check_wav(data):
    if (
        type(data) is not bytes
        or not 44 <= len(data) <= MAX_SPEECH_BYTES
        or data[:4] != b"RIFF"
        or data[8:12] != b"WAVE"
    ):
        raise SpeechUnavailable() from None
    return data


def segment_audio_mime(data):
    """Bounded WAV/MP3 signature check, independent of borrowed extensions."""
    if type(data) is not bytes or not 24 <= len(data) <= MAX_SPEECH_BYTES:
        raise SpeechUnavailable()
    if data[:4] == b"RIFF":
        check_wav(data)
        return b"audio/wav"
    offset = 0
    if data[:3] == b"ID3":
        if data[3] not in {2, 3, 4} or any(v & 128 for v in data[6:10]):
            raise SpeechUnavailable()
        size = sum(v << (7 * (3 - i)) for i, v in enumerate(data[6:10]))
        offset = 10 + size + (10 if data[3] == 4 and data[5] & 16 else 0)
    frame = data[offset : offset + 4]
    if (
        len(frame) == 4
        and frame[0] == 255
        and frame[1] & 224 == 224
        and (frame[1] >> 3) & 3 != 1
        and (frame[1] >> 1) & 3 == 1  # Layer III
        and 1 <= frame[2] >> 4 <= 14
        and (frame[2] >> 2) & 3 != 3
    ):
        return b"audio/mpeg"
    raise SpeechUnavailable()
