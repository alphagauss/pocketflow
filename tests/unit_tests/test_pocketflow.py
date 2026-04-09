import asyncio

from pocketflow import (
    Flow,
    FlowContext,
    Node,
    RetryPolicy,
    StepResult,
)


class AddValueNode(Node[dict[str, object], int, int]):
    def __init__(self, key: str, value: int, action: str = "") -> None:
        super().__init__()
        self.key = key
        self.value = value
        self.action = action

    async def run_step(self, ctx: FlowContext[dict[str, object]]) -> StepResult[int]:
        ctx.state[self.key] = self.value
        return StepResult(output=self.value, action=self.action)


class ParamNode(Node[dict[str, object], str, int]):
    async def run_step(self, ctx: FlowContext[dict[str, object]]) -> StepResult[int]:
        prefix = str(ctx.params["prefix"])
        value = int(ctx.params["value"])
        prepared = f"{prefix}:{value}"
        ctx.state["prepared"] = prepared
        ctx.state["value"] = value
        return StepResult(output=value)


class RetryNode(Node[dict[str, object], None, str]):
    def __init__(self) -> None:
        super().__init__(retry=RetryPolicy(max_retries=3, wait=0))
        self.attempts = 0

    async def run_step(self, ctx: FlowContext[dict[str, object]]) -> StepResult[str]:
        self.attempts += 1
        if self.attempts < 3:
            raise ValueError("temporary error")
        ctx.state["retry_result"] = "done"
        return StepResult(output="done")


class FallbackNode(Node[dict[str, object], None, str]):
    def __init__(self) -> None:
        super().__init__(retry=RetryPolicy(max_retries=2, wait=0))
        self.attempts = 0

    async def run_step(self, ctx: FlowContext[dict[str, object]]) -> StepResult[str]:
        self.attempts += 1
        raise RuntimeError("always fail")

    async def exec_fallback(
        self,
        ctx: FlowContext[dict[str, object]],
        exc: Exception,
    ) -> StepResult[str]:
        result = f"fallback:{exc}"
        ctx.state["fallback_result"] = result
        return StepResult(output=result)


def test_flow_uses_then_and_connect_on() -> None:
    async def run_test() -> None:
        first = AddValueNode("first", 1, action="branch")
        second = AddValueNode("second", 2)
        third = AddValueNode("third", 3)

        first.then(second)
        first.connect_on("branch", third)

        flow = Flow[dict[str, object]](start=first)
        state: dict[str, object] = {}

        result = await flow.run(state)

        assert result == 3
        assert state == {"first": 1, "third": 3}

    asyncio.run(run_test())


def test_flow_set_start_configures_entry_node() -> None:
    async def run_test() -> None:
        start = AddValueNode("value", 10)
        flow = Flow[dict[str, object]]()
        state: dict[str, object] = {}

        returned = flow.set_start(start)
        result = await flow.run(state)

        assert returned is start
        assert result == 10
        assert state == {"value": 10}

    asyncio.run(run_test())


def test_node_retries_until_success() -> None:
    async def run_test() -> None:
        node = RetryNode()
        ctx = FlowContext(state={})

        result = await node.run(ctx)

        assert node.attempts == 3
        assert result.output == "done"
        assert ctx.state["retry_result"] == "done"

    asyncio.run(run_test())


def test_node_uses_fallback_after_final_retry() -> None:
    async def run_test() -> None:
        node = FallbackNode()
        ctx = FlowContext(state={})

        result = await node.run(ctx)

        assert node.attempts == 2
        assert result.output == "fallback:always fail"
        assert ctx.state["fallback_result"] == "fallback:always fail"

    asyncio.run(run_test())


def test_prep_reads_params_from_context() -> None:
    async def run_test() -> None:
        node = ParamNode()
        flow = Flow[dict[str, object]](start=node)
        state: dict[str, object] = {}

        result = await flow.run(
            state,
            params={"prefix": "item", "value": 7},
        )

        assert result == 7
        assert state == {"prepared": "item:7", "value": 7}

    asyncio.run(run_test())
