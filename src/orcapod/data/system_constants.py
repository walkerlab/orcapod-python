# Constants used for source info keys
SYSTEM_COLUMN_PREFIX = "__"
SOURCE_INFO_PREFIX = "_source_"

DATA_CONTEXT_KEY = "_context_key"


class SystemConstant:
    def __init__(self, global_prefix: str = ""):
        self._global_prefix = global_prefix

    @property
    def META_PREFIX(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}"

    @property
    def SOURCE_PREFIX(self) -> str:
        return f"{self._global_prefix}{SOURCE_INFO_PREFIX}"

    @property
    def CONTEXT_KEY(self) -> str:
        return f"{self._global_prefix}{DATA_CONTEXT_KEY}"


orcapod_constants = SystemConstant()
