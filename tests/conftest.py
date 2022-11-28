import pytest

from orinoco import settings


@pytest.fixture
def with_strict_mode():
    current_mode = settings.IMPLICIT_TYPE_STRICT_MODE_ENABLED
    settings.IMPLICIT_TYPE_STRICT_MODE_ENABLED = True
    yield
    settings.IMPLICIT_TYPE_STRICT_MODE_ENABLED = current_mode
