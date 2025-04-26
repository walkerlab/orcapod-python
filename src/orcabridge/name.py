
"""
Utility functions for handling names
"""
import re

def pascal_to_snake(name):
    # Convert PascalCase to snake_case
    # if already in snake_case, return as is
    # TODO: replace this crude check with a more robust one
    if '_' in name:
        return name
    # Replace uppercase letters with underscore followed by lowercase letter
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def snake_to_pascal(name):
    # Convert snake_case to PascalCase
    # if already in PascalCase, return as is
    if '_' not in name:
        return name
    # Split the string by underscores and capitalize each component
    components = name.split('_')
    return ''.join(x.title() for x in components)