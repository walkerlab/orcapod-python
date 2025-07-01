import importlib

def parse_objectspec(obj_spec: dict) -> Any:
    if "_class" in obj_spec:
        # if _class is specified, treat the dict as an object specification
        module_name, class_name = obj_spec["_class"].rsplit(".", 1)
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        configs = parse_objectspec(obj_spec.get("config", {}))
        return cls(**configs)
    else:
        # otherwise, parse through the dictionary recursively
        parsed_object = obj_spec
        for k, v in obj_spec.items():
            if isinstance(v, dict):
                parsed_object[k] = parse_objectspec(v)
            else:
                parsed_object[k] = v
        return parsed_object