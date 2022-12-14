from __future__ import annotations

import abc
import time
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable

from pydantic import BaseModel, Field
from returns.io import IOResult
from returns.pipeline import is_successful
from returns.unsafe import unsafe_perform_io

from orinoco.action import record_action, verbose_action_exception, Action
from orinoco.exceptions import RetryError
from orinoco.types import ActionT, ActionDataT


class RetryStatus(Enum):
    STARTED = auto()
    SUCCESSFUL = auto()
    FAILED = auto()


class RetryInfo(BaseModel):
    max_retries: int
    retry_delay: int
    retry_count: int = 0
    status: RetryStatus = RetryStatus.STARTED

    exceptions: list[Exception] = Field(default_factory=list)
    started: datetime = Field(default_factory=datetime.now)
    finished: datetime | None = None

    class Config:
        arbitrary_types_allowed = False
        allow_mutation = False


class AbstractRetry(Action, abc.ABC):
    def __init__(self, action: ActionT, key: str, max_retries: int = 10, retry_delay: int = 10):
        super().__init__(name=f"<{self.__class__.__name__}: {action.name} every {retry_delay}s (max {max_retries})>")
        self.action = action
        self.key = key
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    @record_action
    @verbose_action_exception
    def run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        retry_info = RetryInfo(max_retries=self.max_retries, retry_delay=self.retry_delay)
        for retry_counter in range(self.max_retries):
            result_action_data = self.action.run(action_data)
            retry_info_result = self._get_new_retry_info(result_action_data.get(self.key), retry_counter, retry_info)
            if is_successful(retry_info_result):
                return result_action_data.evolve(retry_info=retry_info_result.unwrap())
            retry_info = unsafe_perform_io(retry_info_result.failure())

            time.sleep(self.retry_delay)
        raise RetryError("{} failed after {} retries. Info: {}".format(self.name, self.max_retries, retry_info))

    def _get_new_retry_info(
        self, value: Any, retry_counter: int, previous_retry_info: RetryInfo
    ) -> IOResult[RetryInfo, RetryInfo]:
        retry_status = self._get_retry_status(value, retry_counter)

        retry_info = previous_retry_info.copy(update={"status": retry_status})
        if retry_status is RetryStatus.SUCCESSFUL:
            return IOResult.from_value(retry_info.copy(update={"finished": datetime.utcnow()}))

        return IOResult.from_failure(retry_info)

    def _get_retry_status(self, value: Any, retry_counter: int) -> RetryStatus:
        if self._is_finished(value):
            return RetryStatus.SUCCESSFUL
        elif retry_counter > self.max_retries:
            return RetryStatus.FAILED
        return RetryStatus.STARTED

    @abc.abstractmethod
    def _is_finished(self, value: Any) -> bool:
        pass


class WaitUntilTrue(AbstractRetry):
    def _is_finished(self, value: Any) -> bool:
        return value is True


class WaitUntilEqualsTo(AbstractRetry):
    def __init__(self, action: ActionT, key: str, value: Any, max_retries: int = 10, retry_delay: int = 10):
        super().__init__(action=action, key=key, max_retries=max_retries, retry_delay=retry_delay)
        self.value = value

    def _is_finished(self, value: Any) -> bool:
        return value == self.value


class WaitUntilContains(AbstractRetry):
    def __init__(self, action: ActionT, key: str, value: Any, max_retries: int = 10, retry_delay: int = 10):
        super().__init__(action=action, key=key, max_retries=max_retries, retry_delay=retry_delay)
        self.value = value

    def _is_finished(self, value: Any) -> bool:
        return value in self.value


class WaitForGeneric(AbstractRetry):
    def __init__(
        self,
        action: ActionT,
        key: str,
        is_finished_method: Callable[[Any], bool],
        max_retries: int = 10,
        retry_delay: int = 10,
    ):
        super().__init__(action=action, key=key, max_retries=max_retries, retry_delay=retry_delay)
        self.is_finished_method = is_finished_method

    def _is_finished(self, value: Any) -> bool:
        return self.is_finished_method(value)
