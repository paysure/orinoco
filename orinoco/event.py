import asyncio
from abc import ABC
from typing import Iterable, Callable, Optional

from orinoco.action import (
    Action,
    record_action,
    verbose_action_exception,
    ActionSet,
    async_record_action,
    async_verbose_action_exception,
)
from orinoco.exceptions import ActionNotProperlyInherited
from orinoco.tags import SystemActionTag
from orinoco.types import ActionDataT


class Event(Action, ABC):
    """
    Base class for event based actions which run a side-effect
    """

    def __init__(self, description: Optional[str] = None, async_blocking: bool = False, name: Optional[str] = None):
        """
        :param async_blocking: Controls whether to wait for the result when running asynchronously
        """
        self.async_blocking = async_blocking
        super().__init__(description=description, name=name)

    @record_action
    @verbose_action_exception
    def run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        self.run_side_effect(action_data)
        return action_data

    @async_record_action
    @async_verbose_action_exception
    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        coroutine = self.async_run_side_effect(action_data)
        if self.async_blocking:
            await coroutine
            return action_data
        return action_data.with_future(asyncio.create_task(coroutine))

    def run_side_effect(self, action_data: ActionDataT) -> None:
        raise ActionNotProperlyInherited(
            "`run_side_effect` method needs to implemented for sync execution or `async_run_side_effect` "
            "for async execution"
        )

    async def async_run_side_effect(self, action_data: ActionDataT) -> None:
        self.run_side_effect(action_data)


class GenericEvent(Event):
    """
    Utility action which allows running side-effects "on the fly" (usually by lambda functions)
    """

    def __init__(
        self,
        method: Callable[[ActionDataT], None],
        description: Optional[str] = None,
        async_blocking: bool = False,
        name: Optional[str] = None,
    ):
        super().__init__(description, async_blocking=async_blocking, name=name)
        self.method = method

    def run_side_effect(self, action_data: ActionDataT) -> None:
        self.method(action_data)


class EventSet(Event, SystemActionTag):
    """
    Isolated ActionSet - none of the ActionData modification is propagated further
    """

    def __init__(
        self,
        actions: Iterable[Action],
        description: Optional[str] = None,
        async_blocking: bool = False,
        name: Optional[str] = None,
    ):
        super().__init__(description=description, async_blocking=async_blocking, name=name)
        self.actions_set = ActionSet(actions=actions, name=name)

    def run_side_effect(self, action_data: ActionDataT) -> None:
        self.actions_set.run(action_data)
