import asyncio
import os
import tempfile
import time
from contextlib import contextmanager, asynccontextmanager
from dataclasses import dataclass
from typing import Any
from unittest.mock import Mock, call

import pytest

from orinoco import config
from orinoco.action import (
    ActionSet,
    Then,
    HandledExceptions,
    AtomicActionSet,
    AsyncAtomicActionSet,
    Action,
    verbose_action_exception,
    Return,
)
from orinoco.condition import (
    GenericCondition,
    Switch,
    PropertyCondition,
    If,
    AnyCondition,
    AllCondition,
    NonNoneDataValues,
    AlwaysTrue,
)
from orinoco.data_source import GenericDataSource, DataSource, AddActionValue, AddActionValues, AddVirtualKeyShortcut
from orinoco.entities import Signature, ActionData
from orinoco.event import GenericEvent, Event, EventSet
from orinoco.exceptions import (
    ConditionNotMet,
    NoneOfActionsCanBeExecuted,
    ActionNotProperlyInherited,
)
from orinoco.loop import ForSideEffects, For, AsyncFor, AsyncForSideEffects
from orinoco.observers import ExecutionTimeObserver, ActionsLog
from orinoco.transformation import GenericTransformation, RenameActionField, WithoutFields, Transformation
from orinoco.types import ActionDataT


def increase_counter(action_data: ActionDataT) -> ActionDataT:
    return action_data.evolve(counter=action_data.get("counter") + 1)


def decrease_counter(action_data: ActionDataT) -> ActionDataT:
    return action_data.evolve(counter=action_data.get("counter") - 1)


def test_transformation() -> None:
    action = GenericTransformation(increase_counter)

    input_data = ActionData.create(counter=5)
    output_data = action.run(input_data)
    assert output_data.get("counter") == 6


def test_pipeline_with_event() -> None:
    event_handler = Mock()
    action = ActionSet(
        [GenericTransformation(increase_counter), GenericEvent(lambda action_data: event_handler.report() and None)]
    )

    input_data = ActionData.create(counter=5)
    output_data = action.run(input_data)
    assert output_data.get("counter") == 6
    assert event_handler.report_called


def test_generic_condition_pass() -> None:

    data = ActionData.create(counter=0, user_name="Alfred")
    action = GenericCondition(lambda action_data: action_data.get("user_name") is "Alfred").then(
        GenericTransformation(increase_counter)
    )

    result = action.run(data)
    assert result.get("counter") == 1


def test_generic_condition_fail() -> None:
    with pytest.raises(ConditionNotMet):
        data = ActionData.create(counter=0, user_name="Johan")
        action = GenericCondition(lambda action_data: action_data.get("user_name") == "Alfred").then(
            GenericTransformation(increase_counter)
        )

        action.run(data)


def test_fail_with_formatted_message() -> None:
    action = GenericCondition(
        lambda action_data: action_data.get("user_name") == "Alfred",
        fail_message="Failed for {user_name} after {counter} attempts",
    )
    with pytest.raises(ConditionNotMet) as exc_info:
        action.run_with_data(counter=3, user_name="Johan")
    assert exc_info.value.args[0] == "GenericCondition failed: Failed for Johan after 3 attempts"


def test_fail_with_formatted_message_missing_data() -> None:
    action = GenericCondition(
        lambda action_data: action_data.get("user_name") == "Alfred",
        fail_message="Failed for {user_name} after {counter} attempts",
    )
    with pytest.raises(ConditionNotMet) as exc_info:
        action.run_with_data(user_name="Johan")
    assert exc_info.value.args[0] == "GenericCondition failed: Failed for Johan after <NOT-PROVIDED> attempts"


def test_crossroad_action() -> None:
    class Claim:
        status = "PENDING"

    event_handler = Mock()
    action = (
        Switch()
        .if_then(
            PropertyCondition("claim", "status", "CANCELED"),
            GenericEvent(lambda action_data: event_handler.report("GOT CANCELED") and None),
        )
        .if_then(
            PropertyCondition("claim", "status", "PENDING"),
            GenericEvent(lambda action_data: event_handler.report("GOT PENDING") and None),
        )
    )

    action.run(ActionData.create(claim=Claim()))

    assert event_handler.report.call_args[0][0] == "GOT PENDING"


def test_crossroad_action_nothing_pass() -> None:
    class Claim:
        status = "DENIED"

    event_handler = Mock()
    action = (
        Switch()
        .if_then(
            PropertyCondition("claim", "status", "CANCELED"),
            GenericEvent(lambda action_data: event_handler.report("GOT CANCELED") and None),
        )
        .if_then(
            PropertyCondition("claim", "status", "PENDING"),
            GenericEvent(lambda action_data: event_handler.report("GOT PENDING") and None),
        )
    )

    with pytest.raises(NoneOfActionsCanBeExecuted):
        action.run(ActionData.create(claim=Claim()))


def test_negation_operator() -> None:

    always_false = GenericCondition(lambda ad: False, fail_message="Always false")
    always_true = GenericCondition(lambda ad: True, fail_message="Always true")

    always_true.run_with_data()
    (~always_false).run_with_data()
    (~(~always_true)).run_with_data()

    with pytest.raises(ConditionNotMet):
        (~always_true).run_with_data()

    with pytest.raises(ConditionNotMet):
        (~(~always_false)).run_with_data()


def test_and_operator_pass() -> None:

    cond1 = GenericCondition(lambda ad: bool(ad.get("value") > 100))
    cond2 = GenericCondition(lambda ad: bool(ad.get("value") < 200))

    action = cond1 & cond2

    action.run(ActionData.create(value=120))


def test_and_operator_fail() -> None:

    cond1 = GenericCondition(lambda ad: bool(ad.get("value") > 100))
    cond2 = GenericCondition(lambda ad: bool(ad.get("value") < 200), fail_message="Too big")

    action = cond1 & cond2

    with pytest.raises(ConditionNotMet) as err:
        action.run(ActionData.create(value=220))


def test_or_operator_one_match_pass() -> None:

    cond1 = GenericCondition(lambda ad: bool(ad.get("value") < 100))
    cond2 = GenericCondition(lambda ad: bool(ad.get("value") > 200))

    action = cond1 | cond2

    action.run(ActionData.create(value=30))
    action.run(ActionData.create(value=430))


def test_or_operator_both_match_pass() -> None:

    cond1 = GenericCondition(lambda ad: bool(ad.get("value") > 100))
    cond2 = GenericCondition(lambda ad: bool(ad.get("value") < 200))

    action = cond1 | cond2

    action.run(ActionData.create(value=130))


def test_or_operator_with_negation() -> None:

    always_false = GenericCondition(lambda ad: False)
    always_true = GenericCondition(lambda ad: True)

    negative_or_of_falses = ~(always_false | always_false)
    assert negative_or_of_falses.is_inverted
    assert negative_or_of_falses.validate(ActionData.create())
    negative_or_of_falses.run_with_data()

    (~always_false | always_false).run_with_data()
    (always_false | ~always_false).run_with_data()
    (~(always_false | always_false)).run_with_data()
    (~always_false | always_false | always_false).run_with_data()

    with pytest.raises(ConditionNotMet):
        (always_false | always_false).run_with_data()

    with pytest.raises(ConditionNotMet):
        (~(always_true | always_false)).run_with_data()


def test_and_operator_with_negation() -> None:

    always_false = GenericCondition(lambda ad: False, fail_message="Always false")
    always_true = GenericCondition(lambda ad: True, fail_message="Always true")

    (~always_false & always_true).run_with_data()
    (~(always_false & always_false)).run_with_data()
    (~(~(always_true & always_true))).run_with_data()
    (~(always_false & always_true)).run_with_data()


def test_or_operator_fail() -> None:

    cond1 = GenericCondition(lambda ad: bool(ad.get("value") > 100))
    cond2 = GenericCondition(lambda ad: bool(ad.get("value") < 200))

    action = cond1 | cond2

    action.run(ActionData.create(value=130))


def test_combined_operators_pass() -> None:

    cond1 = GenericCondition(lambda ad: bool(ad.get("x") > 100), "c1")
    cond2 = GenericCondition(lambda ad: bool(ad.get("x") < 200), "c2")
    cond3 = GenericCondition(lambda ad: bool(ad.get("y") == 1), "c3")
    cond4 = GenericCondition(lambda ad: bool(ad.get("y") == 2), "c4")

    action1 = cond1 & cond2 & cond3
    action2 = cond1 & (cond4 | cond3)

    action1.run(ActionData.create(x=130, y=1))
    action2.run(ActionData.create(x=130, y=1))


def test_combined_operators_fail() -> None:

    cond1 = GenericCondition(lambda ad: bool(ad.get("x") > 100), "c1")
    cond2 = GenericCondition(lambda ad: bool(ad.get("x") < 200), "c2")
    cond3 = GenericCondition(lambda ad: bool(ad.get("y") == 1), "c3")
    cond4 = GenericCondition(lambda ad: bool(ad.get("y") == 2), "c4")

    action1 = cond1 & cond2 & cond3 & cond4
    action2 = cond1 & (cond4 | cond3) & cond4

    with pytest.raises(ConditionNotMet):
        action1.run(ActionData.create(x=130, y=1))

    with pytest.raises(ConditionNotMet):
        action2.run(ActionData.create(x=130, y=1))


def test_inverted_pass() -> None:

    cond = GenericCondition(lambda ad: bool(ad.get("x") > 200), "c1")

    (~cond).run(ActionData.create(x=100))


def test_case_block_operators() -> None:

    action = (
        Switch()
        .case(If(GenericCondition(lambda ad: bool(ad.get("x") == 1))), Then(GenericTransformation(increase_counter)))
        .case(
            If(GenericCondition(lambda ad: bool(ad.get("x") == 2))),
            Then(GenericTransformation(increase_counter).then(GenericTransformation(increase_counter))),
        )
        .otherwise(GenericTransformation(decrease_counter))
    )

    assert action.run_with_data(x=1, counter=0).get("counter") == 1
    assert action.run_with_data(x=2, counter=0).get("counter") == 2
    assert action.run_with_data(x=33, counter=0).get("counter") == -1

    with pytest.raises(NoneOfActionsCanBeExecuted):
        (
            Switch()
            .case(
                If(GenericCondition(lambda ad: bool(ad.get("x") == 1))), Then(GenericTransformation(increase_counter))
            )
            .case(
                If(GenericCondition(lambda ad: bool(ad.get("x") == 2))),
                Then(GenericTransformation(increase_counter).then(GenericTransformation(increase_counter))),
            )
        ).run_with_data(x=-1, counter=0)


def test_loop_event_generic() -> None:
    log = Mock()
    action = ForSideEffects("x", lambda ad: ad.get("values")).do(
        GenericEvent(lambda action_data: log(action_data.get("x")))
    )
    action.run_with_data(values=[10, 40, 60])

    log.assert_has_calls([call(10), call(40), call(60)])


def test_loop() -> None:
    class DoubleValue(Transformation):
        def transform(self, action_data: ActionDataT) -> ActionDataT:
            return action_data.evolve(doubled=action_data.get("x") * 2)

    assert For(
        "x", lambda ad: ad.get("values"), aggregated_field="doubled", aggregated_field_new_name="doubled_list"
    ).do(DoubleValue()).run_with_data(values=[10, 40, 60]).get("doubled_list") == [20, 80, 120]

    assert (
        For(
            "x",
            lambda ad: ad.get("values"),
            aggregated_field="doubled",
        )
        .do(DoubleValue())
        .run_with_data(values=[10, 40, 60])
        .get("doubled")
        == [20, 80, 120]
    )


def test_loop_with_none_skip() -> None:
    class DoubleOddValue(Transformation):
        def transform(self, action_data: ActionDataT) -> ActionDataT:
            if action_data.get("x") % 2 == 0:
                return action_data.evolve(doubled=action_data.get("x") * 2)
            return action_data.evolve(doubled=None)

    assert (
        For(
            "x",
            lambda ad: ad.get("values"),
            aggregated_field="doubled",
            aggregated_field_new_name="doubled_list",
            skip_none_for_aggregated_field=True,
        )
        .do(DoubleOddValue())
        .run_with_data(values=[1, 2, 3, 4])
        .get("doubled_list")
        == [4, 8]
    )


def test_async() -> None:
    tmp_file, fiorinocome = tempfile.mkstemp()

    async def weather_api(day):
        return day * 2

    class GetWeather(DataSource):
        PROVIDES = "next_week_weather"

        async def async_get_data(self, action_data: ActionDataT) -> Any:
            return await weather_api(action_data.get("next_week"))

    class SaveDate(Event):
        async def async_run_side_effect(self, action_data: ActionDataT):
            with os.fdopen(tmp_file, "w") as tmp:
                tmp.write("Today is {}".format(action_data.get("today")))

    pipeline = (
        GenericDataSource(provides="next_week", method=lambda ad: 7 + ad.get("today"))
        >> SaveDate()
        >> GetWeather()
        >> GenericCondition(lambda ad: ad.get("next_week") > 5).if_then(
            GenericTransformation(lambda ad: ad.evolve(next_week_weather=int(ad.get("next_week_weather") / 10)))
        )
    )
    result = asyncio.run(pipeline.async_run_with_data(today=3))
    assert result.get("next_week_weather") == 2

    assert len(result.futures) == 1
    assert result.futures[0].done()
    assert result.futures[0].exception() is None

    with open(fiorinocome) as fp:
        assert fp.read() == "Today is 3"

    # Can't run sync with async tasks
    with pytest.raises(ActionNotProperlyInherited):
        pipeline.run_with_data(today=3)


def test_measurements() -> None:
    result = (
        GenericEvent(method=lambda ad: time.sleep(0.3))
        >> GenericEvent(method=lambda ad: time.sleep(0.5))
        >> GenericCondition(lambda ad: True)
    ).run_with_data()
    measurements = result.get_observer(ExecutionTimeObserver).measurements

    assert [m[0] for m in measurements] == ["action.GenericEvent", "action.GenericEvent", "action.GenericCondition"]
    assert round(measurements[0][1], 1) == 0.3
    assert round(measurements[1][1], 1) == 0.5


def test_measurements_async() -> None:

    async_result = asyncio.run(
        (
            GenericEvent(method=lambda ad: time.sleep(0.3), async_blocking=True)
            >> GenericEvent(method=lambda ad: time.sleep(0.5), async_blocking=False)
            >> GenericCondition(lambda ad: True)
        ).async_run_with_data()
    )
    measurements = async_result.get_observer(ExecutionTimeObserver).measurements
    assert [m[0] for m in measurements] == ["action.GenericEvent", "action.GenericEvent", "action.GenericCondition"]
    assert round(measurements[0][1], 1) == 0.3
    assert round(measurements[1][1], 1) == 0.0


def test_actions_done() -> None:
    def nothing(ad: ActionData) -> None:
        pass

    action_data = (
        GenericEvent(method=nothing, name="Action1") >> GenericEvent(method=nothing, name="Action2")
    ).run_with_data()

    assert [
        "ActionSet_start",
        "Action1_start",
        "Action1_end",
        "Action2_start",
        "Action2_end",
        "ActionSet_end",
    ] == action_data.get_observer(ActionsLog).actions_log


def test_on_subfield() -> None:
    class GetNameWithCounter(DataSource):
        PROVIDES = "name_with_counter"

        def get_data(self, action_data: ActionDataT) -> "str":
            return "{} - {}".format(action_data.get("key"), action_data.get("counter"))

    action_data = (
        (GenericTransformation(increase_counter) >> GetNameWithCounter()).on_subfield("request_data.user")
        >> GenericTransformation(method=lambda ad: ad.evolve(duplicated_request_data=ad.get("request_data")))
    ).run_with_data(request_data={"user": {"key": "Majka", "counter": 123}})

    assert action_data.get("request_data.user.counter") == 124
    assert action_data.get("request_data.user.name_with_counter") == "Majka - 124"
    assert action_data.get("duplicated_request_data") == action_data.get("request_data")

    assert not action_data.is_in("key")
    assert not action_data.is_in("counter")
    assert not action_data.is_in("user")

    # Actions on nested subfield are recorded
    assert [
        "ActionSet_start",
        "ActionSet_start",
        "GenericTransformation_start",
        "GenericTransformation_end",
        "GetNameWithCounter_start",
        "GetNameWithCounter_end",
        "ActionSet_end",
        "GenericTransformation_start",
        "GenericTransformation_end",
        "ActionSet_end",
    ] == action_data.get_observer(ActionsLog).actions_log


def test_rename_field() -> None:
    action_data = RenameActionField(key="old", new_key="new").run_with_data(old=1)

    assert action_data.is_in("new")
    assert not action_data.is_in("old")
    assert 1 == action_data.get("new")


def test_without_fields() -> None:
    action_data = WithoutFields("a", "b", "c").run_with_data(a=1, b=2, c=3, g=4)
    assert [Signature(key="g", type_=int)] == action_data.signatures
    assert 4 == action_data.get_by_type(int)


def test_add_action_value() -> None:
    assert 1 == AddActionValue(key="a", value=lambda: 1).run_with_data().get("a")
    assert 1 == AddActionValue(key="a", value=1).run_with_data().get("a")

    action_data = AddActionValues(a=2, b=lambda: 3).run_with_data()

    assert 2 == action_data.get("a")
    assert 3 == action_data.get("b")


def test_virtual_key_shortcut() -> None:
    @dataclass
    class Mazda:
        pass

    @dataclass
    class Toyota:
        pass

    action_data = (
        AddVirtualKeyShortcut(key="c1", source_key="car1") >> AddVirtualKeyShortcut(key="c2", source_key="car2")
    ).run_with_data(car1=Toyota(), car2=Mazda())

    assert Toyota() == action_data.get("c1") == action_data.get("car1") == action_data.get_by_type(Toyota)
    assert Mazda() == action_data.get("c2") == action_data.get("car2") == action_data.get_by_type(Mazda)


def test_loop_conditions() -> None:
    all_action = AllCondition(
        iterable_key="numbers", as_key="number", condition=GenericCondition(lambda ad: ad.get("number") >= 0)
    )

    all_action.run_with_data(numbers=[1, 2, 10])
    with pytest.raises(ConditionNotMet):
        all_action.run_with_data(numbers=[1, 2, -10])

    any_action = AnyCondition(
        iterable_key="numbers", as_key="number", condition=GenericCondition(lambda ad: ad.get("number") >= 0)
    )

    any_action.run_with_data(numbers=[-1, -2, 10])
    with pytest.raises(ConditionNotMet):
        any_action.run_with_data(numbers=[-1, -2, -10])


def test_check_non_none_values() -> None:
    action = NonNoneDataValues("field1", "field2")

    action.run_with_data(field1=1, field2=0, field3=None)

    with pytest.raises(ConditionNotMet):
        action.run_with_data(field1=None, field2=0, field3=3)

    with pytest.raises(ConditionNotMet):
        action.run_with_data(field1=1, field3=3)


def test_isolated_actions_set() -> None:
    checkpoints = []

    add_to_checkpoint_action = GenericEvent(lambda ad: checkpoints.append(ad.get("counter")))
    increase_action = GenericTransformation(increase_counter)
    action_data = (
        increase_action
        >> EventSet(
            actions=[
                add_to_checkpoint_action,
                increase_action,
                increase_action,
                increase_action,
                add_to_checkpoint_action,
            ]
        )
        >> increase_action
        >> add_to_checkpoint_action
    ).run_with_data(counter=10)

    assert checkpoints == [11, 14, 12]
    assert 12 == action_data.get("counter")


def test_handled_exception() -> None:
    exceptions_log = []

    class FailingForNonAdmin(Event):
        def run_side_effect(self, action_data: ActionDataT) -> None:
            if action_data.get("user") != "admin":
                raise ValueError()

    not_failing_handled_action = (
        HandledExceptions(
            FailingForNonAdmin(),
            catch_exceptions=ValueError,
            handle_method=lambda error, action_data: exceptions_log.append((error.__class__, action_data.get("user"))),
            fail_on_error=False,
        )
        >> GenericEvent(lambda ad: ...)
    )

    assert not_failing_handled_action.run_with_data(user="admin").get_observer(ActionsLog).actions_log == [
        "ActionSet_start",
        "HandledExceptions_start",
        "FailingForNonAdmin_start",
        "FailingForNonAdmin_end",
        "HandledExceptions_end",
        "GenericEvent_start",
        "GenericEvent_end",
        "ActionSet_end",
    ]
    assert exceptions_log == []
    assert not_failing_handled_action.run_with_data(user="user1").get_observer(ActionsLog).actions_log == [
        "ActionSet_start",
        "HandledExceptions_start",
        "FailingForNonAdmin_start",
        "GenericEvent_start",
        "GenericEvent_end",
        "ActionSet_end",
    ]
    assert exceptions_log == [(ValueError, "user1")]

    class MyException(Exception):
        pass

    exceptions_log = []
    failing_handled_action = HandledExceptions(
        FailingForNonAdmin(),
        catch_exceptions=ValueError,
        handle_method=lambda error, action_data: exceptions_log.append((error.__class__, action_data.get("user")))
        or MyException("Transformed error"),
        fail_on_error=True,
    )

    with pytest.raises(MyException):
        failing_handled_action.run_with_data(user="user2")
    assert exceptions_log == [(ValueError, "user2")]


def test_atomic_context() -> None:
    _log = []

    @contextmanager
    def logger() -> None:
        _log.append("cm_start")
        yield
        _log.append("cm_end")

    AtomicActionSet(
        actions=[
            GenericEvent(lambda ad: _log.append("action1_executed"))
            >> GenericEvent(lambda ad: _log.append("action2_executed"))
        ],
        atomic_context_manager=logger,
    ).run_with_data()

    assert ["cm_start", "action1_executed", "action2_executed", "cm_end"] == _log


def test_async_atomic_context() -> None:
    _log = []

    @asynccontextmanager
    async def logger() -> None:
        _log.append("cm_start")
        yield
        _log.append("cm_end")

    asyncio.run(
        AsyncAtomicActionSet(
            actions=[
                GenericEvent(lambda ad: _log.append("action1_executed"))
                >> GenericEvent(lambda ad: _log.append("action2_executed"))
            ],
            atomic_context_manager=logger,
        ).async_run_with_data()
    )

    assert ["cm_start", "cm_end", "action1_executed", "action2_executed"] == _log


def test_verbose_exception() -> None:
    config.VERBOSE_ERRORS = True

    class TestError(Exception):
        pass

    class FailingAction(Action):
        DESCRIPTION = "This will fail, trust me"
        NAME = "Failing"

        @verbose_action_exception
        def run(self, action_data: ActionDataT) -> ActionDataT:
            raise TestError()

    with pytest.raises(TestError) as context:
        (GenericEvent(lambda ad: ..., name="Action1") >> FailingAction()).run_with_data(x=1, y="abc")

    assert "Action context" in str(context.value)


def test_return_action():
    do_nothing = lambda ad: ...
    assert (
        (
            GenericEvent(do_nothing, name="Action1")
            >> GenericEvent(do_nothing, name="Action2").finish()
            >> GenericEvent(lambda ad: 1 / 0, name="Action3")
        )
        .run_with_data()
        .skip_processing
    )

    log = []
    assert (
        AlwaysTrue()
        .if_then(GenericEvent(lambda ad: log.append("1")).finish() >> GenericEvent(lambda ad: log.append("2")))
        .run_with_data()
        .skip_processing
    )
    assert log == ["1"]

    log = []
    assert (
        AlwaysTrue()
        .if_then(GenericEvent(lambda ad: log.append("1")) >> Return() >> GenericEvent(lambda ad: log.append("2")))
        .run_with_data()
        .skip_processing
    )
    assert log == ["1"]


def test_async_loop_for_side_effects():
    class AsyncGen:
        async def __aiter__(self):
            yield 1
            await asyncio.sleep(0.01)
            yield 2

    class AddingEvent(Event):
        async def async_run_side_effect(self, action_data: ActionDataT) -> None:
            action_data.get("result")["total"] += action_data.get("number")

    async def run_pipeline():
        result = (
            await AsyncForSideEffects("number", lambda ad: AsyncGen())
            .do(AddingEvent())
            .async_run_with_data(result={"total": 0})
        )
        return result.get("result")

    assert 3 == asyncio.run(run_pipeline())["total"]


def test_async_loop_for():
    class AsyncGen:
        async def __aiter__(self):
            yield 1
            await asyncio.sleep(0.01)
            yield 2

    class EmptyEvent(Event):
        async def async_run_side_effect(self, action_data: ActionDataT) -> None:
            pass

    async def run_pipeline():
        result = (
            await AsyncFor("number", lambda ad: AsyncGen(), "number", "results").do(EmptyEvent()).async_run_with_data()
        )
        return result.get("results")

    assert [1, 2] == asyncio.run(run_pipeline())


def test_for_side_effects_loop_run_as_async():
    class Gen:
        def __iter__(self):
            yield 1
            yield 2

    class AddingEvent(Event):
        def run_side_effect(self, action_data: ActionDataT) -> None:
            action_data.get("result")["total"] += action_data.get("number")

        async def async_run_side_effect(self, action_data: ActionDataT) -> None:
            pass

    async def run_pipeline():
        result = (
            await ForSideEffects("number", lambda ad: Gen()).do(AddingEvent()).async_run_with_data(result={"total": 0})
        )
        return result.get("result")

    assert 3 == asyncio.run(run_pipeline())["total"]


def test_for_loop_run_as_async():
    class Gen:
        def __iter__(self):
            yield 1
            yield 2

    class EmptyEvent(Event):
        def run_side_effect(self, action_data: ActionDataT) -> None:
            pass

        async def async_run_side_effect(self, action_data: ActionDataT) -> None:
            pass

    async def run_pipeline():
        result = await For("number", lambda ad: Gen(), "number", "results").do(EmptyEvent()).async_run_with_data()
        return result.get("results")

    assert [1, 2] == asyncio.run(run_pipeline())


def test_guarded_action_set(double_typed_action_cls, check_action_data_fields_action_cls):

    action = (
        ActionSet(
            [
                check_action_data_fields_action_cls("x", "ignored_other_x", "other_x"),
                double_typed_action_cls(),
                double_typed_action_cls().input_as(x="other_x").output_as("other_double"),
                double_typed_action_cls().input_as(x="ignored_other_x").output_as("ignored_other_double"),
            ]
        )
        .as_guarded()
        .with_inputs("x", "ignored_other_x", other_x="different_other_x")
        .with_outputs("other_double", renamed_double="double")
    )

    assert action.run_with_data(x=1, different_other_x=3, ignored_other_x=7, ignored_other2_x=9).as_keyed_dict() == {
        "renamed_double": 2,
        "other_double": 6,
    }
