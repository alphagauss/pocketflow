import asyncio

from pocketflow import (
    AsyncFlow,
    AsyncNode,
    Flow,
    Node,
)


class AddValueNode(Node[None, int, str | None]):
    def __init__(self, key: str, value: int, action: str | None = None) -> None:
        super().__init__()
        self.key = key
        self.value = value
        self.action = action

    def prep(self, shared: dict[str, object]) -> None:
        return None

    def exec(self, prep_result: None) -> int:
        return self.value

    def post(
        self,
        shared: dict[str, object],
        prep_result: None,
        exec_result: int,
    ) -> str | None:
        shared[self.key] = exec_result
        return self.action


class RetryNode(Node[None, str, str]):
    def __init__(self) -> None:
        super().__init__(max_retries=3, wait=0)
        self.attempts = 0

    def prep(self, shared: dict[str, object]) -> None:
        return None

    def exec(self, prep_result: None) -> str:
        self.attempts += 1
        if self.attempts < 3:
            raise ValueError("temporary error")
        return "done"

    def post(
        self,
        shared: dict[str, object],
        prep_result: None,
        exec_result: str,
    ) -> str:
        shared["retry_result"] = exec_result
        return exec_result


class FallbackNode(Node[None, str, str]):
    def __init__(self) -> None:
        super().__init__(max_retries=2, wait=0)
        self.attempts = 0

    def prep(self, shared: dict[str, object]) -> None:
        return None

    def exec(self, prep_result: None) -> str:
        self.attempts += 1
        raise RuntimeError("always fail")

    def exec_fallback(self, prep_result: None, exc: Exception) -> str:
        return f"fallback:{exc}"

    def post(
        self,
        shared: dict[str, object],
        prep_result: None,
        exec_result: str,
    ) -> str:
        shared["fallback_result"] = exec_result
        return exec_result


class AsyncRecordNode(AsyncNode[None, str, str | None]):
    def __init__(self, key: str, value: str, action: str | None = None) -> None:
        super().__init__()
        self.key = key
        self.value = value
        self.action = action

    async def prep_async(self, shared: dict[str, object]) -> None:
        return None

    async def exec_async(self, prep_result: None) -> str:
        await asyncio.sleep(0)
        return self.value

    async def post_async(
        self,
        shared: dict[str, object],
        prep_result: None,
        exec_result: str,
    ) -> str | None:
        shared[self.key] = exec_result
        return self.action


def test_flow_uses_connect_and_connect_on() -> None:
    first = AddValueNode("first", 1, action="branch")
    second = AddValueNode("second", 2)
    third = AddValueNode("third", 3)

    first.connect(second)
    first.connect_on("branch", third)

    flow = Flow[None, str | None](start=first)
    shared: dict[str, object] = {}

    result = flow.run(shared)

    assert result is None
    assert shared == {"first": 1, "third": 3}


def test_flow_set_start_configures_entry_node() -> None:
    start = AddValueNode("value", 10)
    flow = Flow[None, str | None]()
    shared: dict[str, object] = {}

    returned = flow.set_start(start)
    result = flow.run(shared)

    assert returned is start
    assert result is None
    assert shared == {"value": 10}


def test_node_retries_until_success() -> None:
    node = RetryNode()
    shared: dict[str, object] = {}

    result = node.run(shared)

    assert node.attempts == 3
    assert result == "done"
    assert shared["retry_result"] == "done"


def test_node_uses_fallback_after_final_retry() -> None:
    node = FallbackNode()
    shared: dict[str, object] = {}

    result = node.run(shared)

    assert node.attempts == 2
    assert result == "fallback:always fail"
    assert shared["fallback_result"] == "fallback:always fail"


def test_async_flow_runs_async_nodes() -> None:
    first = AsyncRecordNode("first", "A", action="next")
    second = AsyncRecordNode("second", "B")
    first.connect_on("next", second)

    flow = AsyncFlow[None, str | None](start=first)
    shared: dict[str, object] = {}

    result = asyncio.run(flow.run_async(shared))

    assert result is None
    assert shared == {"first": "A", "second": "B"}
