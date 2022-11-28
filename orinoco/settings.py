import os

DEBUG = bool(int(os.getenv("DEBUG", 0)))
IMPLICIT_TYPE_STRICT_MODE_ENABLED = bool(int(os.getenv("IMPLICIT_TYPE_STRICT_MODE_ENABLED", 0)))
