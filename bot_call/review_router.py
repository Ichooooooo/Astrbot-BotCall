"""One business-loop resolver/router, with detached worker handoffs only."""

import asyncio
from dataclasses import dataclass, field

from .astrbot_review import AstrBotReviewAdapter, ReviewRequestPolicy
from .review_policy import ReviewManifest, ReviewPolicyUnavailable
from .review_work import NotApplied


@dataclass(frozen=True, slots=True)
class ReviewPolicySelection:
    policy: ReviewRequestPolicy = field(repr=False)
    counter_rule_id: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class DetachedReviewRoute:
    manifest: ReviewManifest = field(repr=False)
    count_tokens: object = field(repr=False)


class ReviewPolicyRouter:
    """Resolver(binding, saved_manifest) and bind_adapter(policy) are trusted.

    Both callbacks run on this business loop. Only this object holds adapters.
    Resolve finishes registration before returning a detached policy/counter;
    a worker cannot claim before its business adapter is visible.
    """

    def __init__(self, resolve, bind_adapter):
        self.business_loop = asyncio.get_running_loop()
        if not callable(resolve) or not callable(bind_adapter):
            raise ReviewPolicyUnavailable() from None
        self._resolve, self._bind = resolve, bind_adapter
        self._adapters = {}

    def _check(self):
        if asyncio.get_running_loop() is not self.business_loop:
            raise ReviewPolicyUnavailable() from None

    async def resolve(self, binding, saved_manifest):
        self._check()
        try:
            selection = await self._resolve(binding, saved_manifest)
            if type(selection) is not ReviewPolicySelection or selection.policy.binding != binding:
                raise ReviewPolicyUnavailable("mismatch")
            manifest = ReviewManifest.from_policy(selection.policy, selection.counter_rule_id)
            if saved_manifest is not None and manifest != saved_manifest:
                raise ReviewPolicyUnavailable("mismatch")
            adapter = await self._bind(selection.policy)
            if type(adapter) is not AstrBotReviewAdapter or adapter.policy is not selection.policy:
                raise ReviewPolicyUnavailable("mismatch")
            self._adapters[binding.call_id] = (manifest, adapter)
            return DetachedReviewRoute(manifest, selection.policy.count_request)
        except asyncio.CancelledError:
            raise asyncio.CancelledError() from None
        except ReviewPolicyUnavailable as error:
            raise ReviewPolicyUnavailable(error.code) from None
        except Exception:
            raise ReviewPolicyUnavailable() from None

    async def generate(self, node, manifest):
        self._check()
        entry = self._adapters.get(node.call_id)
        if entry is None or entry[0] != manifest or manifest.binding.call_id != node.call_id:
            raise NotApplied("Review route is unavailable") from None
        return await entry[1].generate(node)
