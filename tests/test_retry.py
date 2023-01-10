from typing import Type

import pytest
from typing_extensions import Annotated

from orinoco.exceptions import RetryError
from orinoco.retry import RetryStatus
from orinoco.typed_action import TypedAction, TypedCondition


def test_retry_condition(is_positive_typed_action: TypedCondition, double_typed_action: TypedAction):
    with pytest.raises(RetryError):
        (is_positive_typed_action.retry_until(retry_delay=0.001) >> double_typed_action).run_with_data(x=-1, y=2)


def test_success_after_attempts_condition(
    success_after_attempts_typed_action: Type[TypedCondition], double_typed_action: TypedAction
):
    action = success_after_attempts_typed_action(15)  # type: ignore
    result = (action.retry_until(retry_delay=0.001, max_retries=20) >> double_typed_action).run_with_data(x=3)

    assert result.get("double") == 6
    assert len(result.get("retry_infos")) == 1

    retry_action_name, retry_info = result.get("retry_infos")[0]
    assert retry_action_name == "SuccessAfterAttempts"
    assert retry_info.retry_count == 15
    assert retry_info.status == RetryStatus.SUCCESSFUL
    assert retry_info.finished


def test_fail_before_attempts_condition(
    success_after_attempts_typed_action: Type[TypedCondition], double_typed_action: TypedAction
):
    with pytest.raises(RetryError):
        action = success_after_attempts_typed_action(15)  # type: ignore
        (action.retry_until(retry_delay=0.001, max_retries=5) >> double_typed_action).run_with_data(x=3)


def test_success_until_equals(incremented_call_typed_action: TypedAction, double_typed_action: TypedAction):
    result = (
        incremented_call_typed_action.retry_until_equals(5, retry_delay=0.001, max_retries=5) >> double_typed_action
    ).run_with_data(x=5)

    assert result.get("double") == 10
    assert len(result.get("retry_infos")) == 1

    retry_action_name, retry_info = result.get("retry_infos")[0]
    assert retry_action_name == "IncrementCounter"
    assert retry_info.retry_count == 5
    assert retry_info.status == RetryStatus.SUCCESSFUL
    assert retry_info.finished


def test_fail_until_equals(incremented_call_typed_action: TypedAction, double_typed_action: TypedAction):
    with pytest.raises(RetryError):
        (
            incremented_call_typed_action.retry_until_equals(6, retry_delay=0.001, max_retries=5) >> double_typed_action
        ).run_with_data(x=5)


def test_success_until_contains(appended_incremental_call_typed_action: TypedAction, double_typed_action: TypedAction):
    result = (
        appended_incremental_call_typed_action.retry_until_contains("910", retry_delay=0.001, max_retries=10)
        >> double_typed_action
    ).run_with_data(x=4)

    assert result.get("double") == 8
    assert len(result.get("retry_infos")) == 1

    retry_action_name, retry_info = result.get("retry_infos")[0]
    assert retry_action_name == "AppendAttempts"
    assert retry_info.retry_count == 10
    assert retry_info.status == RetryStatus.SUCCESSFUL
    assert retry_info.finished


def test_failed_until_contains(appended_incremental_call_typed_action: TypedAction, double_typed_action: TypedAction):
    with pytest.raises(RetryError):
        (
            appended_incremental_call_typed_action.retry_until_contains("910", retry_delay=0.001, max_retries=9)
            >> double_typed_action
        ).run_with_data(x=4)


def test_success_until_not_fails(fail_n_times_typed_action: Type[TypedAction], double_typed_action: TypedAction):
    action = fail_n_times_typed_action(3, ValueError)  # type: ignore
    result = (
        action.output_as("x").retry_until_not_fails(retry_delay=0.001, max_retries=5) >> double_typed_action
    ).run_with_data(y=0)

    assert result.get("x") == 3
    assert result.get("double") == 6
    assert len(result.get("retry_infos")) == 1

    retry_action_name, retry_info = result.get("retry_infos")[0]
    assert retry_action_name == "FailNTimes"
    assert retry_info.retry_count == 3
    assert retry_info.status == RetryStatus.SUCCESSFUL
    assert retry_info.finished


def test_failed_until_not_fails_with_different_exception(
    fail_n_times_typed_action: Type[TypedAction], double_typed_action: TypedAction
):
    with pytest.raises(RuntimeError):
        action = fail_n_times_typed_action(3, RuntimeError)  # type: ignore
        (
            action.output_as("x").retry_until_not_fails(
                exception_cls_to_catch=ValueError, retry_delay=0.001, max_retries=5
            )
            >> double_typed_action
        ).run_with_data(y=0)


def test_failed_until_not_fails(fail_n_times_typed_action: Type[TypedAction], double_typed_action: TypedAction):
    with pytest.raises(RetryError):
        action = fail_n_times_typed_action(10, RuntimeError)  # type: ignore
        (
            action.output_as("x").retry_until_not_fails(retry_delay=0.001, max_retries=5) >> double_typed_action
        ).run_with_data(y=0)


def test_multiple_retry_actions(
    appended_incremental_call_typed_action: TypedAction,
    incremented_call_typed_action: TypedAction,
    double_typed_action: TypedAction,
):
    result = (
        appended_incremental_call_typed_action.retry_until_contains("910", retry_delay=0.001, max_retries=10)
        >> double_typed_action
        >> incremented_call_typed_action.retry_until_equals(5, retry_delay=0.001, max_retries=5)
    ).run_with_data(x=7)

    assert result.get("double") == 14
    assert len(result.get("retry_infos")) == 2

    retry_action_name, retry_info = result.get("retry_infos")[0]
    assert retry_action_name == "AppendAttempts"
    assert retry_info.retry_count == 10
    assert retry_info.status == RetryStatus.SUCCESSFUL
    assert retry_info.finished

    retry_action_name, retry_info = result.get("retry_infos")[1]
    assert retry_action_name == "IncrementCounter"
    assert retry_info.retry_count == 5
    assert retry_info.status == RetryStatus.SUCCESSFUL
    assert retry_info.finished
