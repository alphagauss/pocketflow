from __future__ import annotations

import asyncio
import time
import warnings
from typing import Generic, TypeAlias, TypeVar, cast

ParamValue: TypeAlias = (
    str | int | float | bool | None | list["ParamValue"] | dict[str, "ParamValue"]
)
Params: TypeAlias = dict[str, ParamValue]
SharedData: TypeAlias = dict[str, object]

PrepResultT = TypeVar("PrepResultT")
ExecResultT = TypeVar("ExecResultT")
PostResultT = TypeVar("PostResultT")

NodeLike: TypeAlias = "BaseNode[object, object, object]"

__all__ = [
    "ParamValue",
    "Params",
    "SharedData",
    "BaseNode",
    "Node",
    "BatchNode",
    "Flow",
    "BatchFlow",
    "AsyncNode",
    "AsyncBatchNode",
    "AsyncParallelBatchNode",
    "AsyncFlow",
    "AsyncBatchFlow",
    "AsyncParallelBatchFlow",
]


class BaseNode(Generic[PrepResultT, ExecResultT, PostResultT]):
    """流程节点基类。

    一个节点的典型生命周期如下：

    1. `prep(shared)`：从共享上下文中准备输入。
    2. `exec(prep_result)`：执行核心逻辑。
    3. `post(shared, prep_result, exec_result)`：处理结果，并返回流转动作。

    如果节点未接入 `Flow`，可以直接调用 `run()` 单独执行。
    """

    params: Params
    successors: dict[str, NodeLike]

    def __init__(self) -> None:
        """初始化节点。"""
        self.params = {}
        self.successors = {}

    def set_params(self, params: Params) -> None:
        """设置当前节点运行时可见的参数。"""
        self.params = params

    def connect(self, node: NodeLike, action: str = "default") -> NodeLike:
        """连接后继节点。

        Args:
            node: 后继节点。
            action: 当前节点 `post()` 返回的动作名。

        Returns:
            返回传入的后继节点，便于链式配置。
        """
        if action in self.successors:
            warnings.warn(
                f"动作 '{action}' 的后继节点将被覆盖。",
                stacklevel=2,
            )

        self.successors[action] = node
        return node

    def connect_on(self, action: str, node: NodeLike) -> NodeLike:
        """为指定动作连接后继节点。"""
        return self.connect(node=node, action=action)

    def prep(self, shared: SharedData) -> PrepResultT:
        """准备执行所需的输入数据。"""
        return cast(PrepResultT, None)

    def exec(self, prep_result: PrepResultT) -> ExecResultT:
        """执行节点核心逻辑。"""
        return cast(ExecResultT, None)

    def post(
        self,
        shared: SharedData,
        prep_result: PrepResultT,
        exec_result: ExecResultT,
    ) -> PostResultT:
        """处理执行结果，并返回流转动作或最终结果。"""
        return cast(PostResultT, None)

    def _exec(self, prep_result: PrepResultT) -> ExecResultT:
        """执行核心逻辑的内部入口。"""
        return self.exec(prep_result)

    def _run(self, shared: SharedData) -> PostResultT:
        """执行单个节点的完整生命周期。"""
        prep_result = self.prep(shared)
        exec_result = self._exec(prep_result)
        return self.post(shared, prep_result, exec_result)

    def run(self, shared: SharedData) -> PostResultT:
        """直接运行当前节点。

        如果当前节点已连接后继节点，会发出警告，因为节点自身不会继续驱动流程。
        """
        if self.successors:
            warnings.warn(
                "当前节点已配置后继节点，但 `run()` 不会继续驱动流程；请改用 Flow。",
                stacklevel=2,
            )

        return self._run(shared)


class Node(BaseNode[PrepResultT, ExecResultT, PostResultT]):
    """支持失败重试的同步节点。"""

    max_retries: int
    wait: int | float

    def __init__(self, max_retries: int = 1, wait: int | float = 0) -> None:
        """初始化节点。

        Args:
            max_retries: 最大执行次数，至少应为 1。
            wait: 失败后重试前的等待秒数。
        """
        super().__init__()
        self.max_retries = max_retries
        self.wait = wait

    def exec_fallback(self, prep_result: PrepResultT, exc: Exception) -> ExecResultT:
        """在最后一次重试仍失败时执行兜底逻辑。"""
        raise exc

    def _exec(self, prep_result: PrepResultT) -> ExecResultT:
        """带重试地执行核心逻辑。"""
        for retry_index in range(self.max_retries):
            try:
                return self.exec(prep_result)
            except Exception as exc:
                is_last_retry = retry_index == self.max_retries - 1
                if is_last_retry:
                    return self.exec_fallback(prep_result, exc)

                if self.wait > 0:
                    time.sleep(self.wait)

        raise RuntimeError("节点执行未产生结果，请检查 `max_retries` 配置。")


class BatchNode(Node[list[PrepResultT] | None, list[ExecResultT], PostResultT]):
    """串行批量处理节点。"""

    def _exec(self, items: list[PrepResultT] | None) -> list[ExecResultT]:
        """对输入列表逐项执行同步逻辑。"""
        if not items:
            return []

        results: list[ExecResultT] = []
        for item in items:
            results.append(super()._exec(item))

        return results


class Flow(BaseNode[PrepResultT, object, PostResultT]):
    """同步流程控制器。

    `Flow` 负责驱动节点之间的流转。每个节点执行后，框架会根据
    `post()` 的返回值选择后继节点：

    - 返回 `None` 时，等价于动作 `"default"`
    - 返回字符串时，按该动作名查找后继节点
    """

    start_node: NodeLike | None

    def __init__(self, start: NodeLike | None = None) -> None:
        """初始化流程。"""
        super().__init__()
        self.start_node = start

    def set_start(self, node: NodeLike) -> NodeLike:
        """设置流程起始节点。"""
        self.start_node = node
        return node

    def get_next_node(
        self,
        current_node: NodeLike,
        action: str | None,
    ) -> NodeLike | None:
        """根据动作名获取后继节点。"""
        resolved_action = action or "default"
        next_node = current_node.successors.get(resolved_action)

        if next_node is None and current_node.successors:
            warnings.warn(
                f"流程结束：未找到动作 '{resolved_action}' 对应的后继节点，"
                f"可选动作为 {list(current_node.successors)}。",
                stacklevel=2,
            )

        return next_node

    def _orchestrate(
        self,
        shared: SharedData,
        params: Params | None = None,
    ) -> object:
        """从起始节点开始，持续驱动整个流程。"""
        current_node = self.start_node
        if current_node is None:
            return None

        runtime_params = params if params is not None else dict(self.params)
        last_action: object = None

        while current_node is not None:
            current_node.set_params(runtime_params)
            last_action = current_node._run(shared)

            next_action = last_action if isinstance(last_action, str) else None
            current_node = self.get_next_node(current_node, next_action)

        return last_action

    def _run(self, shared: SharedData) -> PostResultT:
        """执行整个同步流程。"""
        prep_result = self.prep(shared)
        orchestrate_result = self._orchestrate(shared)
        return self.post(shared, prep_result, orchestrate_result)

    def post(
        self,
        shared: SharedData,
        prep_result: PrepResultT,
        exec_result: object,
    ) -> PostResultT:
        """默认返回流程编排结果。"""
        return cast(PostResultT, exec_result)


class BatchFlow(Flow[list[Params] | None, PostResultT]):
    """同步批量流程控制器。"""

    def _run(self, shared: SharedData) -> PostResultT:
        """按批次参数逐个执行流程。"""
        batch_params = self.prep(shared)
        if batch_params is None:
            batch_params = []

        for single_params in batch_params:
            merged_params = dict(self.params)
            merged_params.update(single_params)
            self._orchestrate(shared, merged_params)

        return self.post(shared, batch_params, None)


class AsyncNode(Node[PrepResultT, ExecResultT, PostResultT]):
    """异步节点基类。"""

    async def prep_async(self, shared: SharedData) -> PrepResultT:
        """异步准备执行所需的输入数据。"""
        return cast(PrepResultT, None)

    async def exec_async(self, prep_result: PrepResultT) -> ExecResultT:
        """异步执行节点核心逻辑。"""
        return cast(ExecResultT, None)

    async def exec_fallback_async(
        self,
        prep_result: PrepResultT,
        exc: Exception,
    ) -> ExecResultT:
        """在最后一次重试仍失败时执行异步兜底逻辑。"""
        raise exc

    async def post_async(
        self,
        shared: SharedData,
        prep_result: PrepResultT,
        exec_result: ExecResultT,
    ) -> PostResultT:
        """异步处理执行结果，并返回流转动作或最终结果。"""
        return cast(PostResultT, None)

    async def _exec(self, prep_result: PrepResultT) -> ExecResultT:
        """带重试地执行异步核心逻辑。"""
        for retry_index in range(self.max_retries):
            try:
                return await self.exec_async(prep_result)
            except Exception as exc:
                is_last_retry = retry_index == self.max_retries - 1
                if is_last_retry:
                    return await self.exec_fallback_async(prep_result, exc)

                if self.wait > 0:
                    await asyncio.sleep(self.wait)

        raise RuntimeError("异步节点执行未产生结果，请检查 `max_retries` 配置。")

    async def run_async(self, shared: SharedData) -> PostResultT:
        """直接运行当前异步节点。"""
        if self.successors:
            warnings.warn(
                "当前节点已配置后继节点，但 `run_async()` 不会继续驱动流程；请改用 AsyncFlow。",
                stacklevel=2,
            )

        return await self._run_async(shared)

    async def _run_async(self, shared: SharedData) -> PostResultT:
        """执行单个异步节点的完整生命周期。"""
        prep_result = await self.prep_async(shared)
        exec_result = await self._exec(prep_result)
        return await self.post_async(shared, prep_result, exec_result)

    def _run(self, shared: SharedData) -> PostResultT:
        """阻止误用同步接口执行异步节点。"""
        raise RuntimeError("异步节点请使用 `run_async()`。")


class AsyncBatchNode(
    AsyncNode[list[PrepResultT] | None, list[ExecResultT], PostResultT]
):
    """串行异步批量处理节点。"""

    async def _exec(self, items: list[PrepResultT] | None) -> list[ExecResultT]:
        """对输入列表逐项执行异步逻辑。"""
        if not items:
            return []

        results: list[ExecResultT] = []
        for item in items:
            results.append(await super()._exec(item))

        return results


class AsyncParallelBatchNode(
    AsyncNode[list[PrepResultT] | None, list[ExecResultT], PostResultT]
):
    """并行异步批量处理节点。"""

    async def _exec(self, items: list[PrepResultT] | None) -> list[ExecResultT]:
        """并发执行异步批量逻辑。"""
        if not items:
            return []

        coroutines = [super()._exec(item) for item in items]
        return await asyncio.gather(*coroutines)


class AsyncFlow(
    Flow[PrepResultT, PostResultT],
    AsyncNode[PrepResultT, object, PostResultT],
):
    """异步流程控制器。

    流程中的节点可以混合同步节点与异步节点：

    - 遇到 `AsyncNode` 时使用异步生命周期
    - 遇到普通 `BaseNode` 时仍按同步方式执行
    """

    async def _orchestrate_async(
        self,
        shared: SharedData,
        params: Params | None = None,
    ) -> object:
        """从起始节点开始，持续驱动整个异步流程。"""
        current_node = self.start_node
        if current_node is None:
            return None

        runtime_params = params if params is not None else dict(self.params)
        last_action: object = None

        while current_node is not None:
            current_node.set_params(runtime_params)

            if isinstance(current_node, AsyncNode):
                last_action = await current_node._run_async(shared)
            else:
                last_action = current_node._run(shared)

            next_action = last_action if isinstance(last_action, str) else None
            current_node = self.get_next_node(current_node, next_action)

        return last_action

    async def _run_async(self, shared: SharedData) -> PostResultT:
        """执行整个异步流程。"""
        prep_result = await self.prep_async(shared)
        orchestrate_result = await self._orchestrate_async(shared)
        return await self.post_async(shared, prep_result, orchestrate_result)

    async def post_async(
        self,
        shared: SharedData,
        prep_result: PrepResultT,
        exec_result: object,
    ) -> PostResultT:
        """默认返回流程编排结果。"""
        return cast(PostResultT, exec_result)


class AsyncBatchFlow(AsyncFlow[list[Params] | None, PostResultT]):
    """串行异步批量流程控制器。"""

    async def _run_async(self, shared: SharedData) -> PostResultT:
        """按批次参数逐个执行异步流程。"""
        batch_params = await self.prep_async(shared)
        if batch_params is None:
            batch_params = []

        for single_params in batch_params:
            merged_params = dict(self.params)
            merged_params.update(single_params)
            await self._orchestrate_async(shared, merged_params)

        return await self.post_async(shared, batch_params, None)


class AsyncParallelBatchFlow(AsyncFlow[list[Params] | None, PostResultT]):
    """并行异步批量流程控制器。"""

    async def _run_async(self, shared: SharedData) -> PostResultT:
        """按批次参数并发执行异步流程。"""
        batch_params = await self.prep_async(shared)
        if batch_params is None:
            batch_params = []

        coroutines = []

        for single_params in batch_params:
            merged_params = dict(self.params)
            merged_params.update(single_params)
            coroutines.append(self._orchestrate_async(shared, merged_params))

        await asyncio.gather(*coroutines)
        return await self.post_async(shared, batch_params, None)
