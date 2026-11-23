"""Build the installed version's real config layout without importing AstrBot."""

from importlib.metadata import version

NEW = tuple(int(v) for v in version("astrbot").split(".")[:2]) >= (4, 29)
FIELDS = {
    "max_context_length": ("compression", "max_turns"),
    "dequeue_context_length": ("compression", "trim_turns"),
    "context_limit_reached_strategy": ("compression", "overflow_strategy"),
    "llm_compress_instruction": ("compression", "instruction"),
    "llm_compress_keep_recent_ratio": ("compression", "keep_recent_ratio"),
    "llm_compress_provider_id": ("compression", "provider_id"),
    "fallback_max_context_tokens": ("compression", "fallback_max_tokens"),
    "default_personality": ("persona", "persona_id"),
    "llm_safety_mode": ("persona", "safety_mode"),
    "safety_mode_strategy": ("persona", "safety_mode_strategy"),
    "max_agent_step": ("misc", "max_steps"),
    "tool_call_timeout": ("misc", "tool_call_timeout"),
    "tool_schema_mode": ("misc", "tool_schema_mode"),
    "sanitize_context_by_modalities": ("misc", "sanitize_context_by_modalities"),
    "fallback_provider_ids": ("model", "fallback_provider_ids"),
    "request_max_retries": ("model", "request_max_retries"),
}


def migrate(conf):
    if NEW:
        config = {name: {} for name in ("model", "persona", "compression", "misc")}
        for key, (group, target) in FIELDS.items():
            if key in conf["provider_settings"]:
                config[group][target] = conf["provider_settings"].pop(key)
        conf["agent_runner"] = {"runner_type": "local", "config": config}
    return conf


def set_setting(conf, key, value):
    if NEW and key in FIELDS:
        group, target = FIELDS[key]
        conf["agent_runner"]["config"][group][target] = value
    else:
        conf["provider_settings"][key] = value
