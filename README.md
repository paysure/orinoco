# orinoco

Functional composable pipelines allowing clean separation of the business logic and its implementation.

Features

* powerful chaining capabilities
* separation of business logic from the implementation
* functional approach
* async support
* PEP 561 compliant (checked with strict `mypy`)
* complete set of logic operations
* built-in execution measurement via observer pattern
* typed data container with powerful lookup capabilities
* easy to extend by user defined actions

Consider pipeline like this:

```
ParseUserName() >> GetUserDataFromDb() >> GetEmailTemplate() >> SendEmail()
```

Even without knowing how the implementation looks like or even what this library does, it's quite obvious what will 
happen when the pipeline is executed. Main idea of this library is to operate with simple composable blocks 
("actions") to build more complex pipelines.

`orinoco` provides many action bases to simply define new actions. An example implementation of the first action from 
the above example could look like this:

```
class ParseUserName(TypedAction):
    def __call__(self, payload: str) -> str
        return json.loads(payload)["user_name"]
```

The action above is using annotations to get the signature of the input and output data. However, we can be more 
explicit:

```

class ParseUserName(TypedAction):
    CONFIG = ActionConfig(
          INPUT=({"payload": Signature(key="payload")}), OUTPUT=Signature(key="user_name", type_=str)
        )
    def __call__(self, payload: str) -> str
        return json.loads(payload)["user_name"]
```

We don't have to limit ourselves to simple "straightforward" pipelines as the one above. The execution flow can be 
controlled or modified via several predefined actions. These actions allow to perform conditional execution, branching, 
loops, context managers, error handling etc. 


```
Switch()
    .case(
        If(ClaimInValidState("PAID")),
        Then(
            ~ClaimHasPaymentAuthorizationAssigned(
                fail_message="You cannot reset a claim {claim} that has payment authorizations assigned!"
            ),
            StateToAuthorize(),
            ResetEligibleAmount(),
        ),
    )
    .case(
        If(
            ClaimInValidState("DECLINED", "CANCELLED"),
            ClaimHasPreAuth(),
            ~ClaimHasValidNotification(),
            ~ClaimIsRetroactive(),
        ),
        Then(UserCanResetClaim(), StateToPendingAuthorization()),
    )
>> GetClaimChangedNotificationMessage()
>> NotifyUser()
```

See the [docs](https://orinoco.rtfd.io) for more info.

## Installation

Use pypi to install the package:

```
pip install orinoco
```

## Motivation

Python is a very powerful programming language allowing developers to quickly transform their ideas into the code.
As you can imagine, this could be a double-edged sword. On one hand, it renders Python easy to use, on the other hand,
larger projects can get messy if the team is not well-disciplined. Moreover, if the problem domain is complex enough,
even seasoned developers can struggle with producing maintainable and easily readable code.

`orinoco` aims to help developers to express complex business rules in a more readable, understandable
and maintainable fashion. Usual approach of implementing routines as a sequence of commands (e.g. querying a database,
communicating with an external API) is replaced with pipelines composed from individual actions.

### Example scenario

Let's imagine an application authorizing payments, for instance the ones send by a payment terminal in a shop. The 
authorisation logic is, understandably, based on various business rules. 

Suppose the card holder is also an insurance policy holder. Their card could be then used to cover
their insurance claims. Ideally, we would like to authorise payments based not only on the details of the current 
transaction, but also based on their insurance policy. An example implementation could look like this:

```
class Api:
    def __init__(self, parser, fraud_service, db_service, policies_matcher):
        self.parser = parser
        self.fraud_service = fraud_service
        self.db_service = db_service
        self.policies_matcher = policies_matcher

    def payment_auth_endpoint(self.request):
        payment_data = self.parser.parser(request)
        
        self.db_service.store_payment(payment_data)
        
        if self.fraud_service.is_it_fraud(payment_data):
            return Response(json={"authorization_decision": False, "reason": "Fraud payment"})
        
        policy = self.policies_matcher.get_policy_for_payment(card_data)
        
        if not policy:
            return Response(json={"authorization_decision": False, "reason": "Not matching policy"})
            
        funding_account = self.db_service.get_funding_account(policy["funding_account_id"])
        
        if funding_account.amount < payment_data["amount"]:
            return Response(json={"authorization_decision": False, "reason": "Not enough money"})
            
            
        self.db_service.update_policy(policy, payment_data)
        self.db_service.update_funding_account(funding_account, payment_data)
        self.db_service.link_policy_to_payment(policy, payment_data)
        
        return Response(json={"authorization_decision": True})
```

In this example we abstracted all we could into services and simple methods, we leveraged design patterns (such as 
explicit dependency injection), but it still feels there is a lot going on in this method. In order to 
understand the ins and outs of the method, it's rather necessary to go through the code line by line.

Let's look at an alternative version implemented using `orinoco`:

```
class Api:
    AUTH_ACTION = (
        ParseData()
        >> StorePayment()
        >> IsFraud().if_then(GetDeniedFraudResponse().finish())
        >> GetUserPolicy()
        >> GetFundingAccount()
        >> (~EnoughMoney()).if_then(GetNoMoneyResponse().finish())
        >> UpdatePolicy()
        >> UpdateFundingAccount()
        >> UpdatePolicy()
        >> GetAuthorizedResponse()
    )
    def payment_auth_endpoint(self, request):
        return self.AUTH_ACTION.run_with_data(request=request).get_by_type(Response)
```

We moved from the actual implementation of the process as a series of commands into an actual description of the 
business process. This makes it readable even for people without any programming knowledge. 
We can go even further and separate the pipeline into another file which will serve 
as a source of truth for our business processes.

## Building blocks

### Actions
Actions are main building blocks carrying the business logic and can be chained together. There are many predefined 
actions that can be used to build more complex pipelines such as actions for boolean logic and loops.

Actions can be created directly by inheriting from specialized bases (see subsections below). If there is no
suitable base for your use case, you can inherit from `orinoco.action.Action` too, but it's generally discouraged. In
the latter can you would proceed by overriding `run(action_data: ActionData) -> ActionData` method.

Pipelines can be then executed by providing `ActionData` container directly to `run` method or 
by `run_with_data(**kwargs: Any)` method which will basically create the `ActionData` and pass it to the `run` method. 
Find more info about `ActionData` below.


#### TypedAction

This is an enhanced `Action` that uses `ActionConfig` as the way to configure the input and the output.

Business logic is defined in the call method with "normal" parameters as the input, this means that no raw
`ActionData` is required. The propagation of the data from the `ActionData` to the method is done automatically
based on the `ActionConfig` which can be either passed directly to the initializer (see `config`) or as a class variable
(see `CONFIG`) or it could be implicitly derived from annotations of the `__call__` method.

The result of the method is propagated to the `ActionData` with a matching signature. Note that implicit
config will use only annotated return type for the signature, unless it's annotated by `typing.Annotated`, where 
the arguments are: type, key, *tags. For more control, please define the `ActionConfig` manually.

The implicit approach:

```
class SumValuesAndRound(TypedAction):
    def __call__(self, x: float, y: float) -> str:
        return int(x + y)

class SumValuesAndRoundAnnotated(TypedAction):
    def __call__(self, x: float, y: float) -> Annotated[str, "my_sum", "optional_tag1", "optional_tag2"]:
        return int(x + y)
        
assert 3 == SumValuesAndRound().run_with_data(x=1.2, y=1.8).get_by_type(int)
assert 3 == SumValuesAndRoundAnnotated().run_with_data(x=1.2, y=1.8).get("my_sum")
assert 3 == SumValuesAndRoundAnnotated().run_with_data(x=1.2, y=1.8).get_by_tag("optional_tag1")
assert 3 == SumValuesAndRoundAnnotated().run_with_data(x=1.2, y=1.8).get_by_tag("optional_tag1", "optional_tag1")
```

Explicit approach:

```
class SumValuesAndRound(TypedAction):
    CONFIG = ActionConfig(
          INPUT=({"x": Signature(key="x"), "y": Signature(key="y")}), OUTPUT=Signature(key="sum_result", type_=int)
        )
    def __call__(self, x: float, y: float) -> int:
        return int(x + y)
result: ActionData = SumValuesAndRound().run_with_data(x=1.2, y=1.8)
assert 3 == result.get_by_type(int) == result.get("sum_result")
```

Notice there are more possibilities how to retrieve the values from the `ActionData` since it's explicitly 
annotated. See the next section for more information about `ActionData` container. In this "mode" the type annotations 
are optional.

```
assert 5 == SumValuesAndRound()(x=1.9, y=3.1)
```

##### Default values

```
class SumValuesAndRound(TypedAction):
    def __call__(self, x: float, y: float = 1.0) -> str:
        return int(x + y)

assert 2.2 == SumValuesAndRound().run_with_data(x=1.2)
```

##### Retry
`TypedAction` and `TypedCondition` can be configured to retry the execution of the action if they fail.

```
my_typed_condition.retry_until_not_fails(max_retries=3, retry_delay=0.5) >> other_actions
my_typed_condition.retry_until_not_fails(exception_cls_to_catch=ValueError, max_retries=3, retry_delay=0.5) >> other_actions
my_typed_condition.retry_until(retry_delay=0.001, max_retries=20) >> other_actions

my_typed_action.retry_until_equals(5, retry_delay=0.001, max_retries=5)
my_typed_action.retry_until_contains("some_sub_string", retry_delay=0.001, max_retries=10)
my_typed_action.retry_until_contains("some_item_in_list", retry_delay=0.001, max_retries=10)
```

##### Changing output signature
Sometimes it's necessary to change the output signature of the actions "on the fly". This can be done by using 
`output_as` method implemented on `TypedAction` and `TypedCondition`.

```
my_typed_action.output_as(key="x_doubled", type_=MyNumber)
my_typed_action.output_as(key="different_key")
```

##### Changing input name

```
class MyAction(TypedAction):
    def __call__(self, x: int) -> int:
        ...
        
MyAction().input_as(x="different_input").run_with_data(different_input=3)
```

#### OnActionDataSubField
Utility action that executes an action on values of a nested item. Consider this example -- we got his nested data:

```
action_data = ActionData.create(user_info={"address": {"city": "London"}})
```
Action executed on the "city" field:

```
class GetPopulation(TypedAction):
    ...
    def __call__(self, city: str) -> int:
        ...
```

You might be wondering - how can we run this action with the data above, since we expect `city` to be present in the 
`ActionData` container (not hidden in the `user_info.address`)? We can either define a new action that would retrieve 
data manually, or we can use `OnActionDataSubField` to do the trick for us:

```
OnActionDataSubField(GetPopulation(), "user_info.address").run(action_data).get("user_info.address.population")
```

All actions have `on_subfield` method which is a shortcut for the `OnActionDataSubField`:

```
GetPopulation().on_subfield("user_info.address").run(action_data).get("user_info.address.population")
```

#### Return

Action that flags the end of the execution path. In other words if the execution 
path reaches this action, none of the following actions will be executed. 

```
Condition1().if_then(Return(Action1())) >> Action2()
```

```
Condition1().if_then(Action1() >> Return()) >> Action2()
```

```
Condition1().if_then(Action1().finish()) >> Action2()
```
In all of these 3 examples the `Action2` is not executed.

#### HandledExceptions

It's not uncommon that during the execution various actions at various places might cause failures. 
`HandledExceptions` action brings control of these actions (that can fail) and allow us to specify how the failure 
would be handled.

```
class FailingForNonAdmin(Event):
    def run_side_effect(self, action_data: ActionData) -> None:
        if action_data.get("user") != "admin":
            raise ValueError()

action = HandledExceptions(
   FailingForNonAdmin(),
   catch_exceptions=ValueError,
   handle_method=lambda error, action_data: send_alert(action_data.get("user")),
   fail_on_error=False,
    )
```

#### AddActionValue

Action that adds static data into the `ActionData` container.

```
assert 1 == AddActionValue(key="my_number", value=lambda: 1).run_with_data().get("my_number")
```

`value` can also be callable `Callable[[], Any]`.


#### AddVirtualKeyShortcut

Sometimes it can be hard to plug more pipelines together, because they use different keys for same inputs. The 
recommended way how to handle this situation is to use `AddVirtualKeyShortcut` to basically create a "symbolic link" 
between a key and a data already registered in `ActionData`. This way you can access the same original data
with both old and new keys.

```
action_data = AddVirtualKeyShortcut(key="color", source_key="my_favorite_color").run_with_data(my_favorite_color="pink")
assert action_data.get("my_favorite_color") == action_data.get("color") == "pink
```

#### RenameActionField

This changes the key of an item in the `ActionData`.

```
action_data = RenameActionField(key="old", new_key="new").run_with_data(old=1)
assert action_data.is_in("new")
assert not action_data.is_in("old")
```

#### WithoutFields

This deletes items with matching keys.

```
action_data == WithoutFields("a", "b").run_with_data(a=1, c=3)
assert not action_data.is_in("a")
assert not action_data.is_in("b")
assert action_data.is_in("c")
```

#### ActionSet

Collection of actions. This action is used internally when actions are chained.

```
Action1() >> Action2() >> Action3()
```

is equal to

```
ActionSet([Action1(), Action2(), Action3()])
```

##### GuardedActionSet
It can be generated from the `ActionSet` by using `as_guarded` method. This is useful when you want to ensure that 
the action set will use only specified fields and "return" (=propagate further) only desired fields.

```
ActionSet([Action1(), Action2(), Action3()]).as_guarded()
    .with_inputs("a", "b", new_c_name="old_c_name"),
    .with_outputs("c", "d", new_e_name="old_e_name", new_f_name="old_f_name"),
).run_with_data(a=1, b=2, old_c_name=3, will_be_ignored=4)
```

Result action data will contain only "c", "d", "new_e_name" and "new_f_name" keys.


##### Input data validation

Action data for `ActionSet` can be validated to ensure that the action set will have all required inputs. This can be 
done by adding inner `Input` class to the action set or explicitly via `input_validation` argument.

```
class MyActionSet(ActionSet):
    ACTIONS = [...]

    class Input:
        x: int
        y: str
```

```
class MyActionSet(ActionSet):
    ACTIONS = [...]

class Input(BaseModel):
    x: int
    y: str

action = MyActionSet(input_validation=Input)
```


```
class MyActionSet(ActionSet):
    ACTIONS = [...]

def validate(x, y, **kwargs):
    # Can raise `ActionSetInputTypeValidationError` or `ActionSetInputValidationMissingValueError`
    ...

MyActionSet(input_validation=validate)
```

Can be pydantic `BaseModel` as well:
```
class MyActionSet(ActionSet):
    ACTIONS = [...]

    class Input(BaseModel):
        x: int
        y: str
        
        @validator("x")
        def validate_x(cls, value):
            ...
```

##### EventSet

Special type of isolated action set -- none of the `ActionData` modification is propagated further to the pipeline. 
The motivation of this class was adopting more functional approach to the parts of the execution chain.

```
action = IncrementNumber() >> EventSet([DoubleNumber() >> PrintNumber()] >> IncrementNumber())

assert 12 == action.run_with_data(number=10).get("number")
# But `22` (`11 * 2`) is printed by `PrintNumber`
```


#### AtomicActionSet and AsyncAtomicActionSet

Action set which run its actions in a context manager.

We could, for instance, wrap `ActionSet` within a DB transaction:

```
AtomicActionSet(actions=[GetModelData(), CreateModel()], atomic_context_manager=transaction.atomic)
```

#### Generic actions

Generic actions are used to dynamically define actions, usually via lambda functions. There 4 types of them:

##### GenericDataSource

This adds a value (`provides`) resolved by `method`.

```
 GenericDataSource(provides="next_week", method=lambda ad: 7 + ad.get("today"))
```

##### GenericTransformation

This creates a new version of `ActionData`.

```
GenericTransformation(lambda ad: ad.evolve(counter=ad.get("counter") + 1))
```

##### GenericEvent

This action runs a side effect while nothing is returned to the `ActionData`. 

```
GenericEvent(lambda ad: event_handler.report(ad.get("alert")))
```

##### GenericCondition

This is a condition (see `Condition` section below) that returns a boolean value.

```
GenericCondition(lambda ad: ad.get("user_name") == "Alfred")
```

### Conditions

This class allows us to check pre-conditions during the pipeline execution. Also, it can be used to support branching 
logic.

The result of the `Condition` action is not propagated further into the data container, hence it's used only for the 
validation purposes. Essentially, you should provide a validation routine (via `_is_valid()`) that returns a boolean 
value - if the value returned is `True`, the pipeline continues uninterrupted, whereas when the value is `False` 
an exception is raised.


This is how we would check an `x` in the `ActionData` container is non-negative:

```
class IsPositive(Condition):
    ERROR_CLS = ValueError
    
    def _is_valid(self, x: float) -> bool:
        return x >= 0
```

If the condition holds, nothing happens. If the condition is not fulfilled, an exception `ValueError` is raised.

As mentioned above, `Condition` can be used for branching logic too - 
see conditional actions such `Switch`, `If` or `ConditionalAction` below.

### Custom fail message
Fail messages can be customized  or by setting the `fail_message` attribute, specifying `FAIL_MESSAGE` class attribute 
or by overriding the `fail` method.

Fail messages support formatted keyword strings where the values are injected from the `ActionData` container.

```
class IsPositive(Condition):
    FAIL_MESSAGE = "{x} is not positive"
    
    def _is_valid(self, x: float) -> bool:
        return x >= 0
```


#### Operators

* You can use `|` as `OR` operator between two `Condition` instances.
* Analogically you can use `&` as `AND` operator.
* Negation could be achieved with the use of tilde (`~`).

```
~((always_true & always_true) | always_true)
```

#### Branching

Class `Switch` provides a branching support. It can be used in three ways:

1) Provide all `Condition` classes via the initializer (i.e. `__init__` method)
2) Use `if_then` convenience method: `Switch().if_then(cond1, action1).if_then(cond2, action2).otherwise(action3)`
3) Use `Switch.case` with `If` and `Then` actions:

```
 Switch()
    .case(
        If(ClaimInValidState("PAID")),
        Then(
            ~ClaimHasPaymentAuthorizationAssigned(
                fail_message="You cannot reset a claim that has payment authorizations assigned!"
            ),
            StateToAuthorize(),
            ResetEligibleAmount(),
        ),
    )
    .case(
        If(
            ClaimInValidState("DECLINED", "CANCELLED"),
            ClaimHasPreAuth(),
            ~ClaimHasValidNotification(),
            ~ClaimIsRetroactive(),
        ),
        Then(UserCanResetClaim(), StateToPendingAuthorization()),
    ).otherwise(SendNotificationAboutInvalidClaim())
```


#### Conditional actions

A convenience method `if_then` is also present on all `Condition` instances, therefore you can use it to chain 
conditions:

```
is_positive.if_then(DoSomething()) >> DoThisAlways()
```


#### TypedCondition

Typed conditions are defined in the same fashion as the `TypedAction`, but they should return `bool` value which is 
used for validation. Please note that you should implement `__call__` instead of `_is_valid` 
(as is case with `Condition`).

```
class IsPositive(TypedCondition):
    def __call__(self, value: float) -> bool:
        return value >= 0
```

Note that explicit definition can be used as well (see `TypedAction`).

#### Convenience classes

* Use `NonNoneDataValues` to check for the existence/non-null values within `ActionData`.
* Use `PropertyCondition` to check properties of objects.

### ActionData

This is the main data container passed between actions. It also holds metadata about the pipeline execution.

#### Pattern matching

`ActionData` can be pattern-matched by various ways to retrieve data.

Data in `ActionData` are described by the `Signature` which represents "metadata" of the data. Basically it says:
- `type_` - Type of the object
- `key` - Key of the object
- `tags` - Custom tags of the object

For example:

```
ActionData(
    [
        (Signature(type_=Card, tags={"new"}), card1),
        (Signature(type_=Card, tags={"old", "to-remove"}), card2),
        (Signature(key="my_value", type_str), "Hello"),
    ]
)
```

#### Get by name

Get data by name:

```
data = action_data.get("claim")
```

It's the same as `action_data.get_by_signature(Signature(name="claim"))`

##### Nested dictionary lookup

Values from dictionary data can be retrieved by "dot lookups":

```
assert "chrome" == ActionData.create(request={"payload": {"meta": {"browser": "chrome"}}}).get(
        "request.payload.meta.browser"
    )
```

#### Get by type

Get data by data type:

```
data = action_data.get_by_type(Claim)
```

It's the same as `action_data.get_by_signature(Signature(type_=Claim))`

#### Get by signature

Get data with an exact signature:

```
data = action_data.get_by_signature(Signature(type_=Card))
```

It raises `SearchError` if there is not exactly one matching entry. Moreover `get_with_signature` can 
be used to retrieve matched signature along with the data:

```
signature, data = action_data.get_with_signature(Signature(type_=Card))
```

#### Find by signature

Get all data which match the signature:

```
data_list = action_data.find(Signature(type_=Card))
```

Alternatively `find_one` can be used to retrieve single entry (otherwise `SearchError` will be raised). 
Difference from `get_by_signature` is that the signature matching is not exact. For example sets aren't compared, 
but rather subsets:

```
ActionData(
    [
        (Signature(type_=str, tags={"tag1", "tag2"}), "a"),
    ]
).get_by_signature(Signature(type_=str, tags={"tag1"}))
```

Expression above would raise `SearchError`, because the tag is not exactly the same. In order to find this particular 
entry the search signature would have to be `Signature(type_=str, tags={"tag1", "tag2"})`.

```
ActionData(
        [
            (Signature(type_=str, tags={"tag1", "tag2"}), "a"),
        ]
    ).find_one(Signature(type_=str, tags={"tag1"}))
```

This expression would find the data as expected.

#### Observers

There is a mechanism that allows developers to run a code every time an individual action is started and to run
a (possibly different) code when each action is finished. This follow the _Observer_ design pattern.

In order to implement such observers, inherit from `Observer` class. Such classes should be then passed to
the initializer of `ActionData` container. The container is then responsible for passing data to your observer
during the execution.

Note: If you don't provide any custom observers, two default observers are used instead.

Observers can be accessed directly by looking them up in `ActionData.observers` list or via 
`ActionData.get_observer(observer_cls: Type[ObserverT]) -> ObserverT`. Note that if more observers of on type exists 
this method will raise an `FoundMoreThanOne` exception.

##### ActionsLog observer

It records starts and ends of all actions into the log.

```
assert action_data.get_observer(ActionsLog).actions_log == [
        "ActionSet_start",
        "DoSomething_start",
        "DoSomethingElse_start",
        "DoSomethingElse_end",
        "DoSomething_end",
        "ActionSet_end",
    ]
```

##### ExecutionTimeObserver observer

It records the execution times.

```
assert action_data.get_observer(ExecutionTimeObserver).measurements == [
    ("DoSomething", 0.012)
    ("DoSomethingElse", 0.028)
   ]
```

##### Custom observers

New observers can be implemented by inheriting `orinoco.observers.Observer`. Simple logger for condition actions can look 
like this:

```
class ConditionsLogger(Observer):

    def __init__(self, logger: BoundLogger):
        self.logger = logger
        
    # This method decides whether an action should be recorded via `record_start` and `record_end`
    def should_record_action(self, action) -> bool:
        return isinstance(action, Condition)

    def record_start(self, action):
        self.logger.info("Condition evaluation started", action=action)

    def record_end(self, action):
        self.logger.info("Condition evaluation ended", action=action)
        
action_data = ActionData(observers=[ConditionsLogger()])
```

#### Exporting to a dict

`ActionData` can be exported to a dictionary `Dict[str, Any]` via `action_data.as_keyed_dict()`. Note that's done only 
for signatures which have `key` set, since they are used as dictionary keys.


#### Reusing action data

Result `ActionData` returned by the actions pipeline after the execution can be used again as an input to a pipeline. 
The only issues is that it also carries information about the execution from the previous run, so the new records 
would be appended to the old ones. However, this doesn't have to be desirable in all cases. In this case, use 
`ActionData.with_new_execution_meta` method to create a new version of action data without the history of previous 
executions.

#### Immutability

All methods that change action data always return the copy of such data instead of just modifying it. This is, 
for example, done after the execution of each action. One of the many implications of this fact is that the input of the
action won't be changed in any way.

### Chaining actions

All actions implement `then` method for chaining. You can also use `rshift` operator shortcut:

```
ParsePayload() >> ExtractUserEmail() >> SendEmail() 
```

The other way to build pipelines is to use `orinoco.action.ActionSet`:

```
ActionSet([ParsePayload(), ExtractUserEmail(), SendEmail()])
```

### Loops

#### For

If you need to loop over any `Iterable` within `ActionData` and you want to apply any `Actions` to every single element
of such iterable, use `For` class.


```
class DoubleValue(ActionType):
    def __call__(self, x: int) -> Annotated[int, "doubled"]:
        return x * 2

assert For(
    "x", lambda ad: ad.get("values"), aggregated_field="doubled", aggregated_field_new_name="doubled_list"
).do(DoubleValue()).run_with_data(values=[10, 40, 60]).get("doubled_list") == [20, 80, 120]
```

`For` parameters :
- iterating_key: Key which will be propagated into the `ActionData` with the new value
- method: Method which returns the iterable to iterate over
- aggregated_field: Name of the field which will be extracted from the `ActionData` and aggregated
        (appended to the list)
- aggregated_field_new_name: Name of the field which will be used for the aggregated field
- skip_none_for_aggregated_field: If `True` then `None` values won't be added to the aggregated field

#### LoopCondition

Implementation of any (`AnyCondition`) and all (`AllCondition`) conditions.

```
any_action = AnyCondition(
    iterable_key="numbers", as_key="number", condition=GenericCondition(lambda ad: ad.get("number") >= 0)
)
any_action.run_with_data(numbers=[-1, -2, 10])
```


### Async support

All `Action` pipelines can be executed synchronously or asynchronously by invoking dedicatted methods. Main async 
execution methods are `async_run(action_data: ActionData)` and `async_run_with_data(**kwargs: Any)`. 

```
class SendEmail(Action)
    sending_service = ...
    
    async def async_run(self, action_data: ActionData) -> ActionData:
        await self.sending_service.send(action_data.get("email")) 
        return action_data
        
coroutine = SendEmail().async_run_with_data(email=...)

# Or you can use any other async framework
asyncio.get_event_loop().run_until_complete(coroutine)
```

#### AsyncTypedAction

This is a specialized variation of `TypedAction` that uses `ActionConfig` for managing data in `ActionData`.

```
class AsyncIntSum(AsyncTypedAction):
    async def __call__(self, x: float, y: float) -> int:
        return int(x + y)
```

#### Combining sync and async actions

Executing sync actions asynchronously is supported by default. On the other hand, execution of async actions 
synchronously has to be implemented explicitly by implementing both `async_run` and `run` methods:

```
class SleepOneSecondAction(Action):
    async def async_run(self, action_data: ActionData) -> ActionData:
        await asyncio.sleep(1)
        return action_data
        
    def run(self, action_data: ActionData) -> ActionData:
        time.sleep(1)
        return action_data
```

`AsyncTypedAction` has `SYNC_ACTION: Optional[Type[TypedAction]]` attribute to provide sync version of the action:

```
class AsyncIntSum(AsyncTypedAction):
    SYNC_ACTION = SyncIntSum

    async def __call__(self, x: float, y: float) -> int:
        return int(x + y)
```

### Other action bases

`TypedAction` and `AsyncTypedAction` are recommended action bases which should do just fine for most cases. 
However, sometimes it's necessary to do more low-level operations and work at `ActionData -> ActionData` level. 
It's not generally recommended to inherit from `Action` directly (see the section below for more details), use one of
the following bases instead.

#### Transformation

This is an `Action` subtype. Interestingly, its functionality does not differ from its parent class, but it
serves the purpose of clearly describing the nature of the action (i.e. transforming data), thus the only difference 
is that the method to implement is called `transform`.

```
class RunTwoActions(Transformation):

    def __init__(self, action1: ActionT, action2: ActionT):
        super().__init__()
        self.action1 = action1
        self.action2 = action2

    def transform(self, action_data: ActionDataT) -> ActionDataT:
        resutl1 = self.action1.run(action_data)
        # do something meaningful
        ...
        
        result2 = self.action1.run(resutl1)
        
        # do something meaningful
        ...
        
        return result2
```

Values in `ActionData` should be "modified" (well it's an immutable container, so it returns a copy of itself with 
the new values) via `ActionData.evolve` to add a value without a full signature (using a key only) or 
`ActionData.register` to add a value with a full signature.

```
class IncrementByRandomNumbers(Transformation):
    def transform(self, action_data: ActionDataT) -> ActionDataT:
        my_number = action_data.get("my_number")
        return action_data.evolve(random_number1=my_number + random()).register(
            Signature(key="random_number2", type_=float, tags={"random", "zero-to-one"}),
            my_number + random(),
        )
        
result_action_data = AddRandomNumbers().run_with_data()
result_action_data.get("random_number1)
result_action_data.get("random_number2)
result_action_data.find_one(Signature(tags={"random"}))
```

In order to create an async version, just implement `async_transform` method.

#### DataSource

This is an action that adds a value to the `ActionData`. You should just return the plain value (no fiddling around
with the container). The action will then add the return value to the container automatically. Nevertheless,
we need to specify what should be the _key_ used when adding the value to the container. That's why this class
has to specify `PROVIDES: str` class attribute. 

```
class IncrementByRandomNumber(DataSource):
    PROVIDES = "random_number"
   
    def get_data(self, action_data: ActionDataT) -> Any:
        my_number = action_data.get("my_number")
        return my_number + random()
```

In order to create an async version, just implement `async_get_data` method.

#### Event

This is an action that does not return anything (nothing from this action is propagated to the action data). It's usually 
used to isolate side effects from the rest of the pipeline. Since nothing is returned back to the `ActionData`, events 
can be executed in parallel in a separate thread when running the async version of the pipeline (see "Async support" 
section above). This is done by default and can be turned off via `async_blocking` parameter of the `Event`.


```
class SendEmail(Event):
    
    def __init__(self, email_service: EmailService):
        super().__init__()
        self.email_service = email_service
        
    def run_side_effect(self, action_data: ActionDataT) -> None:
         self.email_service.send(recepient=action_data.get_by_type(User), message=action_data.get_by_type(EmailMessage))
        
```

In order to create an async version, just implement `async_run_side_effect` method.

#### Inheriting Action directly

This is not recommended and using other action bases should be just fine -- basically the same capabilities has 
`Transformation` class since it's also operating on `ActionData -> ActionData` level. However, if for some reason it's 
necessary to do that, there are a few things needed to keep in mind.

The most important one is to implement "skipping logic". Current implementation is using `ActionData.skip_processing` 
flag to determine whether the action data should be processed (see `Return` action). So don't forget to add 
something like this in your code (especially when using `Return` action in the pipeline):

```
@record_action
@verbose_action_exception
def run(self, action_data: ActionDataT) -> ActionDataT:
    if action_data.skip_processing:
        return action_data
    
    # normal implementation of the action
    ...
```

Second thing is to add `record_action` (for `run`) or `async_record_action` (for `async_run`) decorators for the run 
method. These decorators are important for recording the executed actions to observers. 

Lastly, `verbose_action_exception` (for `run`) or `async_verbose_action_exception` (for `async_run`) should be added 
as well to prettify errors.