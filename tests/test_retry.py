from orinoco.retry import WaitForEqualTo


def test_retry_equal_to():
    WaitForEqualTo(action=None, key=None, value=None, max_retries=10, retry_delay=10)