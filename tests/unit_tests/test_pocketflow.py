import asyncio
from dataclasses import dataclass, field

from pocketflow import (
    Flow,
    FlowContext,
    Node,
    RetryPolicy,
    StepResult,
)


@dataclass(slots=True)
class DemoContext(FlowContext):
    values: dict[str, object] = field(default_factory=dict)


class AddValueNode(Node):
    def __init__(self, key: str, value: int, action: str = "") -> None:
        super().__init__()
        self.key = key
        self.value = value
        self.action = action

    async def run_step(self, ctx: DemoContext) -> StepResult:
        ctx.values[self.key] = self.value
        return StepResult(output=self.value, action=self.action)


class ParamNode(Node):
    async def run_step(self, ctx: DemoContext) -> StepResult:
        prefix = str(ctx.params["prefix"])
        value = int(ctx.params["value"])
        prepared = f"{prefix}:{value}"
        ctx.values["prepared"] = prepared
        ctx.values["value"] = value
        return StepResult(output=value)


class RetryNode(Node):
    def __init__(self) -> None:
        super().__init__(retry=RetryPolicy(max_attempts=3, wait=0))
        self.attempts = 0

    async def run_step(self, ctx: DemoContext) -> StepResult:
        self.attempts += 1
        if self.attempts < 3:
            raise ValueError("temporary error")
        ctx.values["retry_result"] = "done"
        return StepResult(output="done")


class FallbackNode(Node):
    def __init__(self) -> None:
        super().__init__(retry=RetryPolicy(max_attempts=2, wait=0))
        self.attempts = 0

    async def run_step(self, ctx: DemoContext) -> StepResult:
        self.attempts += 1
        raise RuntimeError("always fail")

    async def exec_fallback(
        self,
        ctx: DemoContext,
        exc: Exception,
    ) -> StepResult:
        result = f"fallback:{exc}"
        ctx.values["fallback_result"] = result
        return StepResult(output=result)


def test_flow_uses_then_and_connect_on() -> None:
    async def run_test() -> None:
        first = AddValueNode("first", 1, action="branch")
        second = AddValueNode("second", 2)
        third = AddValueNode("third", 3)

        first.then(second)
        first.connect_on("branch", third)

        flow = Flow(start=first)
        ctx = DemoContext()

        result = await flow.run(ctx)

        assert result == 3
        assert ctx.values == {"first": 1, "third": 3}

    asyncio.run(run_test())


def test_flow_set_start_configures_entry_node() -> None:
    async def run_test() -> None:
        start = AddValueNode("value", 10)
        flow = Flow()
        ctx = DemoContext()

        returned = flow.set_start(start)
        result = await flow.run(ctx)

        assert returned is start
        assert result == 10
        assert ctx.values == {"value": 10}

    asyncio.run(run_test())


def test_node_retries_until_success() -> None:
    async def run_test() -> None:
        node = RetryNode()
        ctx = DemoContext()

        result = await node.run(ctx)

        assert node.attempts == 3
        assert result.output == "done"
        assert ctx.values["retry_result"] == "done"

    asyncio.run(run_test())


def test_node_uses_fallback_after_final_retry() -> None:
    async def run_test() -> None:
        node = FallbackNode()
        ctx = DemoContext()

        result = await node.run(ctx)

        assert node.attempts == 2
        assert result.output == "fallback:always fail"
        assert ctx.values["fallback_result"] == "fallback:always fail"

    asyncio.run(run_test())


def test_prep_reads_params_from_context() -> None:
    async def run_test() -> None:
        node = ParamNode()
        flow = Flow(start=node)
        ctx = DemoContext(params={"prefix": "item", "value": 7})

        result = await flow.run(ctx)

        assert result == 7
        assert ctx.values == {"prepared": "item:7", "value": 7}

    asyncio.run(run_test())
