"""Native private-call entry, opt-in and bound to the original QQ session."""

import asyncio

from .bot_call.astrbot_compat import plugin_api

logger, filter, Star, StarTools = plugin_api()

from .bot_call.plugin import PLUGIN_NAME, PluginLifecycle, PluginLifecycleError

PLUGIN_VERSION = "1.0.0"


def _kind(event):
    try:
        value = event.get_message_type()
        value = getattr(value, "value", value)
        return {"FriendMessage": "private", "GroupMessage": "group", "OtherMessage": "other"}.get(
            value, "unknown"
        )
    except Exception:
        return "unknown"


def _stopped(event):
    try:
        return "yes" if event.is_stopped() else "no"
    except Exception:
        return "unknown"


def _entry_note(stage, *, ready="unknown", kind="unknown", stopped="unknown", result="none"):
    """Fixed diagnostic vocabulary only; observability cannot break the entry."""
    try:
        stage = stage if stage in {"load", "matched", "entered", "result", "notice"} else "unknown"
        ready = "yes" if ready is True else "no" if ready is False else "unknown"
        kind = kind if kind in {"private", "group", "other"} else "unknown"
        stopped = stopped if stopped in {"yes", "no"} else "unknown"
        result = (
            result
            if result
            in {
                "none",
                "ready",
                "not_ready",
                "unsupported_event",
                "submitted",
                "confirmed",
                "not_applied",
                "uncertain",
                "tts_unavailable",
                "chat_unavailable",
                "stt_unavailable",
                "error",
                "cancelled",
                "attempted",
                "returned",
                "failed",
                "skipped",
            }
            else "unknown"
        )
        logger.info(
            "bot_call.entry stage=%s ready=%s kind=%s stopped=%s result=%s",
            stage,
            ready,
            kind,
            stopped,
            result,
        )
    except BaseException:
        pass


class _BotCallProbe(filter.CustomFilter):
    def filter(self, event, cfg):
        # Registered after CommandFilter: ordinary messages never reach here.
        _entry_note("matched", kind=_kind(event), stopped=_stopped(event))
        return True


class BotCallPlugin(Star):
    def __init__(self, context, config=None):
        super().__init__(context, config)
        self._lifecycle = PluginLifecycle(
            config,
            lambda: StarTools.get_data_dir(PLUGIN_NAME),
            context=context,
            version=PLUGIN_VERSION,
        )

    async def initialize(self):
        try:
            await self._lifecycle.initialize()
        except BaseException:
            _entry_note("load", ready=False, result="error")
            raise
        _entry_note(
            "load",
            ready=self._lifecycle.ready,
            result="ready" if self._lifecycle.ready else "not_ready",
        )

    async def _notice(self, event, text):
        try:
            # Mark before awaiting a send: unknown delivery is never retried.
            if not event.get_extra("bot_call_failure_notice", False):
                event.set_extra("bot_call_failure_notice", True)
                _entry_note("notice", result="attempted")
                await event.send(event.plain_result(text))
                _entry_note("notice", result="returned")  # Normal return is not a receipt.
            else:
                _entry_note("notice", result="skipped")
        except asyncio.CancelledError:
            _entry_note("notice", result="cancelled")
            raise
        except Exception as error:
            _entry_note("notice", result="failed")
            try:
                from .bot_call.prepare_diagnostics import prepare_note

                logger.warning("%s", prepare_note("send", error))
            except BaseException:
                pass

    async def _unconfirmed_notice(self, event):
        await self._notice(event, "本次通话未能确认就绪，请先不要重复发送，等待排查。")

    @filter.custom_filter(_BotCallProbe)
    @filter.command("botcall")
    async def botcall(self, event):
        # AstrBot's waking/whitelist stages authorize before ProcessStage
        # invokes this handler; there is no separate plugin user whitelist.
        event.stop_event()  # Before first await: no ordinary LLM fallback.
        kind = _kind(event)
        _entry_note("entered", ready=self._lifecycle.ready, kind=kind, stopped=_stopped(event))
        if kind != "private":
            _entry_note("result", kind=kind, result="unsupported_event")
            await self._notice(
                event, "当前来源暂不支持发起通话，请使用已配置的 OneBot 或官方 QQ 私聊。"
            )
            return "unsupported_event"
        try:
            result = await self._lifecycle.initiate(event)
        except asyncio.CancelledError:
            _entry_note("result", kind=kind, result="cancelled")
            raise
        except PluginLifecycleError as error:
            _entry_note("result", kind=kind, result="error")
            await self._unconfirmed_notice(event)
            raise error from None
        from .bot_call.publication import Outcome

        label = (
            result.value
            if isinstance(result, Outcome)
            else result
            if type(result) is str
            else "unknown"
        )
        _entry_note("result", kind=kind, result=label)
        if result == "stt_unavailable":
            await self._notice(
                event,
                "本来源会话尚未选用可用STT，未开始通话或录音。请在AstrBot选择已有Whisper兼容STT；失效、禁用或不支持的来源不会自动换成其他服务。",
            )
        elif result == "tts_unavailable":
            await self._notice(
                event,
                "本来源会话尚未选用可用TTS。请在AstrBot为此会话选择已配置的OpenAI兼容TTS；插件不会开启全局语音或代选服务。",
            )
        elif result == "chat_unavailable":
            await self._notice(
                event,
                "通话专用模型不存在、未启用或不是聊天模型。请在插件配置中选择已启用的聊天模型；本次未创建邀请，不自动回退。",
            )
        elif result == "unsupported_event":
            await self._notice(
                event, "当前来源暂不支持发起通话，请使用已配置的 OneBot 或官方 QQ 私聊。"
            )
        elif result in ("not_ready", Outcome.NOT_APPLIED, Outcome.UNCERTAIN):
            await self._unconfirmed_notice(event)
        return result

    @filter.llm_tool(name="bot_call_start")
    async def bot_call_start(self, event):
        """仅在当前私聊用户明确要求现在开始通话时登记发起请求。

        不用于咨询、否定、引用或未来计划。邀请由系统另行发送；返回登记不代表发送成功。
        """
        return await self._lifecycle.initiate_tool(event)

    async def terminate(self):
        await self._lifecycle.terminate()
