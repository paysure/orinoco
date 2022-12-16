from __future__ import annotations

import abc
import time
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Type, Generic, Optional, cast

from pydantic import BaseModel, Field
from returns import pipeline

from returns.result import Failure, Result, Success

from orinoco.action import record_action, verbose_action_exception, Action
from orinoco.exceptions import RetryError, ConditionNotMet, BaseActionException
from orinoco.types import ActionT, ActionDataT, ErrorT


class RetryStatus(Enum):
    STARTED = auto()
    SUCCESSFUL = auto()
    FAILED = auto()


class RetryInfo(BaseModel):
    max_retries: int
    retry_delay: float
    retry_count: int = 0
    status: RetryStatus = RetryStatus.STARTED

    started: datetime = Field(default_factory=datetime.now)
    finished: Optional[datetime] = None

    class Config:
        arbitrary_types_allowed = False
        allow_mutation = False


class AbstractRetry(Generic[ErrorT], Action, abc.ABC):
    def __init__(self, action: ActionT, max_retries: int = 10, retry_delay: float = 10):
        super().__init__(name=f"<{self.__class__.__name__}: {action.name} every {retry_delay}s (max {max_retries})>")
        self.action = action
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    @record_action
    @verbose_action_exception
    def run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        retry_info = RetryInfo(max_retries=self.max_retries, retry_delay=self.retry_delay)
        for i in range(self.max_retries):
            result_action_data_result = self._run_action(action_data)

            is_successful = self._is_successful(result_action_data_result)
            retry_info = self._get_new_retry_info(is_successful, i + 1, retry_info)
            if is_successful:
                result_action_data = result_action_data_result.unwrap()
                return result_action_data.evolve(
                    retry_infos=result_action_data.get("retry_infos", []) + [(self.action.name, retry_info)]
                )

            time.sleep(self.retry_delay)
        raise RetryError("{} failed after {} retries. Info: {}".format(self.name, self.max_retries, retry_info))

    def _get_new_retry_info(self, is_successful: bool, retry_counter: int, previous_retry_info: RetryInfo) -> RetryInfo:
        retry_status = self._get_retry_status(is_successful, retry_counter)
        retry_info = previous_retry_info.copy(update={"status": retry_status, "retry_count": retry_counter})
        if retry_status is RetryStatus.SUCCESSFUL:
            return retry_info.copy(update={"finished": datetime.utcnow()})

        return retry_info

    def _get_retry_status(self, is_successful: bool, retry_counter: int) -> RetryStatus:
        if is_successful:
            return RetryStatus.SUCCESSFUL
        elif retry_counter > self.max_retries:
            return RetryStatus.FAILED
        return RetryStatus.STARTED

    def _run_action(self, action_data: ActionDataT) -> Result[ActionDataT, ErrorT]:
        try:
            return Success(self.action.run(action_data))
        except ConditionNotMet as err:
            return Failure(cast(ErrorT, err))
        except BaseActionException:
            raise
        except BaseException as err:
            return Failure(cast(ErrorT, err))

    @abc.abstractmethod
    def _is_successful(self, value: Result[ActionDataT, ErrorT]) -> bool:
        pass


class AbstractNotFailingRetryWithKey(Generic[ErrorT], AbstractRetry[ErrorT], abc.ABC):
    def __init__(self, action: ActionT, key: str, max_retries: int = 10, retry_delay: float = 10):
        super().__init__(action=action, max_retries=max_retries, retry_delay=retry_delay)
        self.key = key

    def _is_successful(self, value: Result[ActionDataT, ErrorT]) -> bool:
        return self._check_condition(value.unwrap().get(self.key))

    @abc.abstractmethod
    def _check_condition(self, value: Any) -> bool:
        pass


class AbstractFailingRetry(Generic[ErrorT], AbstractRetry[ErrorT], abc.ABC):
    def _is_successful(self, action_data_result: Result[ActionDataT, ErrorT]) -> bool:
        if pipeline.is_successful(action_data_result):
            return True
        if self._check_exception(action_data_result.failure()):
            return False
        raise action_data_result.failure()

    @abc.abstractmethod
    def _check_exception(self, exception: BaseException) -> bool:
        pass


class WaitUntilTrue(AbstractFailingRetry[ConditionNotMet]):
    def _check_exception(self, value: BaseException) -> bool:
        return isinstance(value, ConditionNotMet)


class WaitUntilEqualsTo(AbstractNotFailingRetryWithKey):
    def __init__(self, action: ActionT, key: str, value: Any, max_retries: int = 10, retry_delay: float = 10):
        super().__init__(action=action, key=key, max_retries=max_retries, retry_delay=retry_delay)
        self.value = value

    def _check_condition(self, value: Any) -> bool:
        return value == self.value


class WaitUntilContains(AbstractNotFailingRetryWithKey):
    def __init__(self, action: ActionT, key: str, value: Any, max_retries: int = 10, retry_delay: float = 10):
        super().__init__(action=action, key=key, max_retries=max_retries, retry_delay=retry_delay)
        self.value = value

    def _check_condition(self, value: Any) -> bool:
        return self.value in value


class WaitUntilNotFail(Generic[ErrorT], AbstractFailingRetry[ErrorT]):
    def __init__(
        self,
        action: ActionT,
        exception_cls_to_catch: Optional[Type[ErrorT]] = None,
        max_retries: int = 10,
        retry_delay: float = 10,
    ):
        super().__init__(action=action, max_retries=max_retries, retry_delay=retry_delay)
        self.exception_cls_to_catch = exception_cls_to_catch

    def _check_exception(self, exception: BaseException) -> bool:
        return isinstance(exception, self.exception_cls_to_catch) if self.exception_cls_to_catch else True


class WaitForGeneric(AbstractNotFailingRetryWithKey):
    def __init__(
        self,
        action: ActionT,
        key: str,
        check_condition_function: Callable[[Any], bool],
        max_retries: int = 10,
        retry_delay: int = 10,
    ):
        super().__init__(action=action, key=key, max_retries=max_retries, retry_delay=retry_delay)
        self.check_condition_function = check_condition_function

    def _check_condition(self, value: Any) -> bool:
        return self.check_condition_function(value)
