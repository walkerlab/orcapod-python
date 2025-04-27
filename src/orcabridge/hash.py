
# a function to hash a dictionary of key value pairs into uuid
import hashlib
import uuid
from uuid import UUID
from typing import Dict, Union

# arbitrary depth of nested dictionaries
T = Dict[str, Union[str, "T"]]

# TODO: implement proper recursive hashing
def hash_dict(d: T) -> UUID:
    # Convert the dictionary to a string representation
    dict_str = str(sorted(d.items()))
    
    # Create a hash of the string representation
    hash_object = hashlib.md5(dict_str.encode())
    
    # Convert the hash to a UUID
    hash_uuid = uuid.UUID(hash_object.hexdigest())
    
    return hash_uuid


