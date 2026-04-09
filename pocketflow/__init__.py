from __future__ import annotations

import asyncio
import warnings
from dataclasses import dataclass, field
from typing import Generic, TypeAlias, TypeVar

Params: TypeAlias = dict[str, object]

StateT = TypeVar("StateT")
InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")

__all__ = [
    "Params",
    "RetryPolicy",
    "FlowContext",
    "StepResult",
    "Node",
    "Flow",
]


@dataclass(slots=True)
class RetryPolicy:
    """节点重试策略。"""

    max_retries: int = 1
    wait: float = 0.0

    def __post_init__(self) -> None:
        """校验重试配置。"""
        if self.max_retries < 1:
            raise ValueError("`max_retries` 至少应为 1。")

        if self.wait < 0:
            raise ValueError("`wait` 不能为负数。")


@dataclass(slots=True)
class FlowContext(Generic[StateT]):
    """流程运行时上下文。"""

    state: StateT
    params: Params = field(default_factory=dict)


@dataclass(slots=True)
class StepResult(Generic[OutputT]):
    """单个节点执行后的结构化结果。"""

    output: OutputT | None = None
    action: str = ""


class Node(Generic[StateT, InputT, OutputT]):
    """异步节点基类。"""

    successors: dict[str, Node[object, object, object]]
    next_node: Node[object, object, object] | None
    retry: RetryPolicy

    def __init__(self, retry: RetryPolicy | None = None) -> None:
        """初始化节点。

        Args:
            retry: 当前节点的重试策略。
        """
        self.successors = {}
        self.next_node = None
        self.retry = retry if retry is not None else RetryPolicy()

    def _connect(
        self,
        node: Node[object, object, object],
        action: str,
    ) -> Node[object, object, object]:
        """连接后继节点。

        Args:
            node: 后继节点。
            action: 当前节点返回的动作名。

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

    def then(self, node: Node[object, object, object]) -> Node[object, object, object]:
        """连接默认顺序后继节点。"""
        if self.next_node is not None:
            warnings.warn(
                "默认后继节点将被覆盖。",
                stacklevel=2,
            )

        self.next_node = node
        return node

    def connect_on(
        self,
        action: str,
        node: Node[object, object, object],
    ) -> Node[object, object, object]:
        """为指定动作连接后继节点。"""
        return self._connect(node=node, action=action)

    async def exec_fallback(
        self,
        ctx: FlowContext[StateT],
        exc: Exception,
    ) -> StepResult[OutputT]:
        """在最后一次重试仍失败时执行兜底逻辑。"""
        raise exc

    async def run_step(
        self,
        ctx: FlowContext[StateT],
    ) -> StepResult[OutputT]:
        """执行单个节点的核心逻辑。"""
        raise NotImplementedError

    async def run(self, ctx: FlowContext[StateT]) -> StepResult[OutputT]:
        """带重试地执行单个节点。"""
        for retry_index in range(self.retry.max_retries):
            try:
                return await self.run_step(ctx)
            except Exception as exc:
                is_last_retry = retry_index == self.retry.max_retries - 1
                if is_last_retry:
                    return await self.exec_fallback(ctx, exc)

                if self.retry.wait > 0:
                    await asyncio.sleep(self.retry.wait)

        raise RuntimeError("节点执行未产生结果，请检查重试配置。")


class Flow(Generic[StateT]):
    """异步流程控制器。

    流程会根据节点返回的 `StepResult.action` 选择后继节点。
    空字符串表示流程结束。
    """

    start_node: Node[object, object, object] | None

    def __init__(self, start: Node[object, object, object] | None = None) -> None:
        """初始化流程。"""
        self.start_node = start

    def set_start(
        self,
        node: Node[object, object, object],
    ) -> Node[object, object, object]:
        """设置流程起始节点。"""
        self.start_node = node
        return node

    def _get_next_node(
        self,
        current_node: Node[object, object, object],
        action: str,
    ) -> Node[object, object, object] | None:
        """根据动作名获取后继节点。"""
        if action == "":
            return current_node.next_node

        next_node = current_node.successors.get(action)

        if next_node is None and current_node.successors:
            warnings.warn(
                f"流程结束：未找到动作 '{action}' 对应的后继节点，"
                f"可选动作为 {list(current_node.successors)}。",
                stacklevel=2,
            )

        return next_node

    async def _finalize(
        self,
        ctx: FlowContext[StateT],
        result: object | None,
    ) -> object | None:
        """在流程结束后生成最终返回值。"""
        return result

    async def run(
        self,
        state: StateT,
        params: Params | None = None,
    ) -> object | None:
        """从起始节点开始驱动整个流程。"""
        ctx = FlowContext(state=state, params=dict(params or {}))
        current_node = self.start_node
        result: object | None = None

        while current_node is not None:
            step = await current_node.run(ctx)
            result = step.output
            current_node = self._get_next_node(current_node, step.action)

        return await self._finalize(ctx, result)
