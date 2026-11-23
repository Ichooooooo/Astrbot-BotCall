"""Trusted event-to-binding conversion without importing AstrBot."""

from dataclasses import dataclass
from typing import Protocol
from uuid import uuid4


class BindingError(ValueError):
    """The trusted event has no usable, owned conversation."""


class EventReader(Protocol):
    unified_msg_origin: str

    def get_sender_id(self) -> str: ...
    def get_self_id(self) -> str: ...
    def get_platform_id(self) -> str: ...
    def get_platform_name(self) -> str: ...
    def get_message_type(self) -> object: ...
    def get_session_id(self) -> str: ...


@dataclass(frozen=True, slots=True, repr=False)
class ExpectedEntry:
    """Trusted in-memory identity only. Never contains a request or its text."""

    identity: tuple[str, ...]
    message_id: str | int
    conversation_id: str

    # Minimal event-reader surface lets prepare retain only this snapshot,
    # never the original event.extra request/history while waiting for its lock.
    @property
    def unified_msg_origin(self):
        return self.identity[6]

    def get_sender_id(self):
        return self.identity[0]

    def get_self_id(self):
        return self.identity[1]

    def get_platform_id(self):
        return self.identity[2]

    def get_platform_name(self):
        return self.identity[3]

    def get_message_type(self):
        return self.identity[4]

    def get_session_id(self):
        return self.identity[5]

    @property
    def message_obj(self):
        from types import SimpleNamespace

        return SimpleNamespace(message_id=self.message_id)

    def check_event(self, event):
        if (
            private_event_identity(event) != self.identity
            or getattr(event.message_obj, "message_id", None) != self.message_id
            or type(event.message_obj.message_id) is not type(self.message_id)
        ):
            raise BindingError("Initiating event changed")


def reliable_message_id(value):
    return (type(value) is str and bool(value) and not value.isspace()) or (
        type(value) is int and value != 0
    )


class ConversationReader(Protocol):
    async def get_curr_conversation_id(self, umo: str) -> str | None: ...
    async def get_conversation(self, umo: str, cid: str) -> object | None: ...


@dataclass(frozen=True, slots=True)
class CallBinding:
    call_id: str
    sender_id: str
    self_id: str
    platform_id: str
    platform_type: str
    message_type: str
    session_id: str
    unified_msg_origin: str
    conversation_id: str
    persona_id: str | None


def validate_conversation(
    conversation: object | None, *, umo: str, cid: str, platform_id: str
) -> str | None:
    """Check ownership explicitly; the core getter only looks up by cid."""
    if conversation is None:
        raise BindingError("Conversation no longer exists")
    if (
        getattr(conversation, "cid", None) != cid
        or getattr(conversation, "user_id", None) != umo
        or getattr(conversation, "platform_id", None) != platform_id
    ):
        raise BindingError("Conversation ownership mismatch")
    persona = getattr(conversation, "persona_id", None)
    if persona is not None and not isinstance(persona, str):
        raise BindingError("Invalid conversation persona identifier")
    return persona


def private_event_identity(event: EventReader) -> tuple[str, ...]:
    """Keep opaque framework identities and the complete private-session origin."""
    sender = event.get_sender_id()
    self_id = event.get_self_id()
    platform = event.get_platform_id()
    platform_type = event.get_platform_name()
    message_type = event.get_message_type()
    message_type = getattr(message_type, "value", message_type)
    session = event.get_session_id()
    umo = event.unified_msg_origin
    values = (sender, self_id, platform, platform_type, message_type, session, umo)
    if any(not isinstance(value, str) or not value or value.isspace() for value in values):
        raise BindingError("Missing event identity")
    if message_type != "FriendMessage":
        raise BindingError("Only private-chat initiation is supported")
    if umo != f"{platform}:{message_type}:{session}":
        raise BindingError("Event origin does not match its platform and session")
    return sender, self_id, platform, platform_type, message_type, session, umo


async def bind_call(
    event: EventReader, reader: ConversationReader, *, expected: ExpectedEntry | None = None
) -> CallBinding:
    """Capture one private-chat target from trusted server interfaces.

    A missing/empty persona identifier means no explicit conversation override;
    effective defaults and session persona rules belong to the read adapter.
    This function reads only. It neither creates nor switches conversations.
    Its arguments are trusted Python interfaces, not authenticated web input.
    """
    sender, self_id, platform, platform_type, message_type, session, umo = private_event_identity(
        event
    )
    if expected is not None:
        if type(expected) is not ExpectedEntry:
            raise BindingError("Invalid expected entry")
        expected.check_event(event)
    cid = await reader.get_curr_conversation_id(umo)
    if expected is not None:
        expected.check_event(event)
        if cid != expected.conversation_id:
            raise BindingError("Original request conversation is no longer current")
    if not isinstance(cid, str) or not cid or cid.isspace():
        raise BindingError("No current conversation")
    conversation = await reader.get_conversation(umo, cid)
    persona = validate_conversation(conversation, umo=umo, cid=cid, platform_id=platform)
    if expected is not None:
        expected.check_event(event)
    return CallBinding(
        uuid4().hex,
        sender,
        self_id,
        platform,
        platform_type,
        message_type,
        session,
        umo,
        cid,
        persona,
    )
