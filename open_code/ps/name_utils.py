import re

def is_valid_qualified_name(name):
    pattern = r'^[A-Za-z_.\-][A-Za-z0-9_.\-]*$'
    match = re.match(pattern, name)
    return match is not None
