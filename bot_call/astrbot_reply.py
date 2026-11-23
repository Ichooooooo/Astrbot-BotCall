"""Legacy policy decoding and the native Agent reply facade."""

from dataclasses import asdict, dataclass, field
from .binding import CallBinding
from .astrbot_review import _text
from .native_provider import NATIVE_CONTRACT
from .phone_style import DEFAULT_REPLY_STYLE
from .astrbot_agent import SourceAgent, SourceAgentPolicy


class ReplyNotApplied(RuntimeError):
    pass


class ReplyResponseUncertain(RuntimeError):
    pass


def _reject():
    raise ReplyNotApplied("reply_adapter.not_applied") from None


@dataclass(frozen=True, slots=True)
class ReplyModel:
    provider_id: str = field(repr=False)
    model: str = field(repr=False)
    context_window: int
    output_token_limit: int
    cap_parameter: str = "max_completion_tokens"

    def __post_init__(self):
        try:
            _text(self.provider_id)
            _text(self.model)
            # Native <=0 disables token guarding. No invented fallback window.
            if (
                type(self.context_window) is not int
                or type(self.output_token_limit) is not int
                or self.output_token_limit <= 0
                or self.cap_parameter
                not in {"max_tokens", "max_completion_tokens", NATIVE_CONTRACT}
            ):
                raise ValueError
        except Exception:
            _reject()


@dataclass(frozen=True, slots=True)
class ReplyPolicy:
    binding: CallBinding = field(repr=False)
    main: ReplyModel = field(repr=False)
    compression: ReplyModel | None = field(default=None, repr=False)

    def __post_init__(self):
        try:
            if (
                type(self.binding) is not CallBinding
                or type(self.main) is not ReplyModel
                or (self.compression is not None and type(self.compression) is not ReplyModel)
            ):
                raise ValueError
            for key, value in asdict(self.binding).items():
                if key == "persona_id" and value is None:
                    continue
                _text(value)
            b = self.binding
            if (
                b.message_type != "FriendMessage"
                or b.unified_msg_origin != f"{b.platform_id}:{b.message_type}:{b.session_id}"
            ):
                raise ValueError
        except Exception:
            _reject()


class AstrBotReplyAdapter(SourceAgent):
    @classmethod
    async def bind(
        cls,
        context,
        policy,
        *,
        read_adapter=None,
        read_call_state,
        agent_api=None,
        agent_journal=None,
        call_reply_style=DEFAULT_REPLY_STYLE,
        call_chat_provider_id="",
        call_skip_plugin_hooks=(),
        call_history_turns=8,
        call_history_summary=True,
        call_history_max_chars=8000,
        session_lock_manager=None,
        privacy=None,
        **unused,
    ):
        if type(policy) is not SourceAgentPolicy:
            _reject()
        try:
            result = cls(
                context,
                policy,
                api=agent_api,
                journal=agent_journal,
                read_state=read_call_state,
                locks=session_lock_manager,
                privacy=privacy,
                call_reply_style=call_reply_style,
                call_chat_provider_id=call_chat_provider_id,
                call_skip_plugin_hooks=call_skip_plugin_hooks,
                call_history_turns=call_history_turns,
                call_history_summary=call_history_summary,
                call_history_max_chars=call_history_max_chars,
            )
            privacy.check()
            return result
        except Exception:
            _reject()
