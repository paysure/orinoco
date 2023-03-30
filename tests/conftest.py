from typing import Any, Type

import pytest
from typing_extensions import Annotated

from orinoco import config
from orinoco.condition import Condition
from orinoco.typed_action import TypedAction, TypedCondition
from orinoco.types import ActionDataT


@pytest.fixture
def with_strict_mode():
    current_mode = config.IMPLICIT_TYPE_STRICT_MODE_ENABLED
    config.IMPLICIT_TYPE_STRICT_MODE_ENABLED = True
    yield
    config.IMPLICIT_TYPE_STRICT_MODE_ENABLED = current_mode


@pytest.fixture
def is_positive_typed_action() -> TypedCondition:
    class IsPositive(TypedCondition):
        def __call__(self, x: int) -> Annotated[bool, "is_positive"]:
            return x > 0

    return IsPositive()


@pytest.fixture
def success_after_attempts_typed_action() -> Type[TypedCondition]:
    class SuccessAfterAttempts(TypedCondition):
        def __init__(self, attempts: int):
            super().__init__()
            self.attempts = attempts
            self._counter = 0

        def __call__(self, x: int) -> Annotated[bool, "is_positive"]:
            self._counter += 1
            return self._counter >= self.attempts

    return SuccessAfterAttempts


@pytest.fixture
def double_typed_action(double_typed_action_cls) -> TypedAction:
    return double_typed_action_cls()


@pytest.fixture
def double_typed_action_cls() -> Type[TypedAction]:
    class Double(TypedAction):
        def __call__(self, x: int) -> Annotated[int, "double"]:
            return x * 2

    return Double


@pytest.fixture
def check_action_data_fields_action_cls() -> Type[Condition]:
    class CheckFieldsInActionData(Condition):
        def __init__(self, *fields: str):
            super().__init__()
            self.fields = fields

        def _is_valid(self, action_data: ActionDataT) -> bool:
            return set(self.fields) == {signature.key for signature in action_data.signatures}

    return CheckFieldsInActionData


@pytest.fixture
def incremented_call_typed_action() -> TypedAction:
    class IncrementCounter(TypedAction):
        def __init__(self):
            super().__init__()
            self._counter = 0

        def __call__(self, x: int) -> Annotated[int, "call_counter"]:
            self._counter += 1
            return self._counter

    return IncrementCounter()


@pytest.fixture
def appended_incremental_call_typed_action() -> TypedAction:
    class AppendAttempts(TypedAction):
        def __init__(self):
            super().__init__()
            self._counter = 0
            self._buffer = ""

        def __call__(self, x: int) -> Annotated[str, "call_counter"]:
            self._counter += 1
            self._buffer += str(self._counter)
            return self._buffer

    return AppendAttempts()


@pytest.fixture
def fail_n_times_typed_action() -> Type[TypedAction]:
    class FailNTimes(TypedAction):
        def __init__(self, n: int, exception: Type[BaseException]):
            super().__init__()
            self._counter = 0
            self.n = n
            self.exception = exception

        def __call__(self, y: int) -> Annotated[int, "fail_counter"]:
            self._counter += 1
            if self._counter < self.n:
                raise self.exception
            return self._counter

    return FailNTimes
