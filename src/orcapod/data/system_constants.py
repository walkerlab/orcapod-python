# Constants used for source info keys
SYSTEM_COLUMN_PREFIX = "__"
DATAGRAM_PREFIX = "_"
SOURCE_INFO_PREFIX = "source_"
POD_ID_PREFIX = "pod_id_"
DATA_CONTEXT_KEY = "context_key"
INPUT_PACKET_HASH = "input_packet_hash"
PACKET_RECORD_ID = "packet_id"
SYSTEM_TAG_PREFIX = "system_tag_"


class SystemConstant:
    def __init__(self, global_prefix: str = ""):
        self._global_prefix = global_prefix

    @property
    def META_PREFIX(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}"

    @property
    def DATAGRAM_PREFIX(self) -> str:
        return f"{self._global_prefix}{DATAGRAM_PREFIX}"

    @property
    def SOURCE_PREFIX(self) -> str:
        return f"{self._global_prefix}{DATAGRAM_PREFIX}{SOURCE_INFO_PREFIX}"

    @property
    def CONTEXT_KEY(self) -> str:
        return f"{self._global_prefix}{DATAGRAM_PREFIX}{DATA_CONTEXT_KEY}"

    @property
    def POD_ID_PREFIX(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{POD_ID_PREFIX}"

    @property
    def INPUT_PACKET_HASH(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{INPUT_PACKET_HASH}"

    @property
    def PACKET_RECORD_ID(self) -> str:
        return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{PACKET_RECORD_ID}"

    @property
    def SYSTEM_TAG_PREFIX(self) -> str:
        return f"{self._global_prefix}{DATAGRAM_PREFIX}{SYSTEM_TAG_PREFIX}"


constants = SystemConstant()
