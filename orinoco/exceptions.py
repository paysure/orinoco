from typing import Optional


class BaseActionException(Exception):
    def __init__(self, message: str = "", reason: Optional[str] = None, action: Optional[object] = None):
        super().__init__(message)
        self.reason = reason
        self.action = action
        self.message = message

    def __str__(self) -> str:
        if self.reason is not None and self.action is not None:
            return "{}: {}: {}: {}".format(
                self.__class__.__name__, self.action.__class__.__name__, self.reason, self.message
            )
        return "{}: {}".format(self.__class__.__name__, self.message)


class ConditionNotMet(BaseActionException):
    pass


class NoneOfActionsCanBeExecuted(BaseActionException):
    pass


class KeyNotInActionData(BaseActionException):
    pass


class ActionNotReturnedActionData(BaseActionException):
    pass


class ActionNotProperlyInherited(RuntimeError):
    pass


class ActionNotProperlyConfigured(RuntimeError):
    pass


##
class SearchError(BaseActionException):
    pass


class NothingFound(SearchError):
    pass


class FoundMoreThanOne(SearchError):
    pass


class AlreadyRegistered(BaseActionException):
    pass


class RunnableOnlyInAsyncContext(BaseActionException):
    pass


class RetryError(BaseActionException):
    pass
