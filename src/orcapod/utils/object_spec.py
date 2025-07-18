import importlib
from typing import Any


def parse_objectspec(obj_spec: Any) -> Any:
    if isinstance(obj_spec, dict):
        if "_class" in obj_spec:
            # if _class is specified, treat the dict as an object specification, looking for
            # _config key to extract configuration parameters
            module_name, class_name = obj_spec["_class"].rsplit(".", 1)
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            configs = parse_objectspec(obj_spec.get("_config", {}))
            return cls(**configs)
        else:
            # otherwise, parse through the dictionary recursively
            parsed_object = obj_spec
            for k, v in obj_spec.items():
                parsed_object[k] = parse_objectspec(v)
            return parsed_object
    elif isinstance(obj_spec, list):
        # if it's a list, parse each item in the list
        return [parse_objectspec(item) for item in obj_spec]
    elif isinstance(obj_spec, tuple):
        # if it's a tuple, parse each item in the tuple
        return tuple(parse_objectspec(item) for item in obj_spec)
    else:
        # if it's neither a dict nor a list, return it as is
        return obj_spec
