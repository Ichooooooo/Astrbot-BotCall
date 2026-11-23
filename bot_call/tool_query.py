"""Read-only legacy tool-policy data. No registry, SDK, SQL or tool execution."""

from dataclasses import dataclass, field
import hashlib
import json


def canonical(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class QueryResult:
    state: str
    body: str = field(repr=False)
    facts_json: str = field(default="{}", repr=False)


def policy_details(value):
    parsed = json.loads(value)
    if (
        type(parsed) is not dict
        or canonical(parsed) != value
        or type(parsed.get("policy")) is not dict
    ):
        raise ValueError("legacy.invalid_policy")
    policy = parsed["policy"]
    contract = policy.get("contract")
    if type(contract) is not str or not contract:
        raise ValueError("legacy.invalid_policy")
    # Saved fingerprint and original bytes are checked by Store. Do not rebind
    # or rebuild this obsolete policy against any installed third-party plugin.
    return {"contract": contract, "source": policy.get("source", "native_tasks"), "legacy": True}
