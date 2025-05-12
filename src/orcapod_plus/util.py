import yaml
import textwrap


def yaml_multiline_string_presenter(dumper, data):
    if len(data.splitlines()) > 1:
        data = "\n".join([line.rstrip() for line in textwrap.dedent(data).strip().splitlines()])
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data.strip())


yaml.add_representer(str, yaml_multiline_string_presenter)

dump_original = yaml.dump


def dump(*args, **kwargs):
    return dump_original(*args, **dict(kwargs, sort_keys=False))


yaml.dump = dump
