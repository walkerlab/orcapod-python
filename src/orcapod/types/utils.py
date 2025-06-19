# TODO: move these functions to util
def escape_with_postfix(field: str, postfix=None, separator="_") -> str:
    """
    Escape the field string by doubling separators and optionally append a postfix.
    This function takes a field string and escapes any occurrences of the separator
    by doubling them, then optionally appends a postfix with a separator prefix.

    Args:
        field (str): The input string containing to be escaped.
        postfix (str, optional): An optional postfix to append to the escaped string.
                               If None, no postfix is added. Defaults to None.
        separator (str, optional): The separator character to escape and use for
                                 prefixing the postfix. Defaults to "_".
    Returns:
        str: The escaped string with optional postfix. Returns empty string if
             fields is provided but postfix is None.
    Examples:
        >>> escape_with_postfix("field1_field2", "suffix")
        'field1__field2_suffix'
        >>> escape_with_postfix("name_age_city", "backup", "_")
        'name__age__city_backup'
        >>> escape_with_postfix("data-info", "temp", "-")
        'data--info-temp'
        >>> escape_with_postfix("simple", None)
        'simple'
        >>> escape_with_postfix("no_separators", "end")
        'no__separators_end'
    """

    return field.replace(separator, separator * 2) + (f"_{postfix}" if postfix else "")


def unescape_with_postfix(field: str, separator="_") -> tuple[str, str | None]:
    """
    Unescape a string by converting double separators back to single separators and extract postfix metadata.
    This function reverses the escaping process where single separators were doubled to avoid
    conflicts with metadata delimiters. It splits the input on double separators, then extracts
    any postfix metadata from the last part.

    Args:
        field (str): The escaped string containing doubled separators and optional postfix metadata
        separator (str, optional): The separator character used for escaping. Defaults to "_"
    Returns:
        tuple[str, str | None]: A tuple containing:
            - The unescaped string with single separators restored
            - The postfix metadata if present, None otherwise
    Examples:
        >>> unescape_with_postfix("field1__field2__field3")
        ('field1_field2_field3', None)
        >>> unescape_with_postfix("field1__field2_metadata")
        ('field1_field2', 'metadata')
        >>> unescape_with_postfix("simple")
        ('simple', None)
        >>> unescape_with_postfix("field1--field2", separator="-")
        ('field1-field2', None)
        >>> unescape_with_postfix("field1--field2-meta", separator="-")
        ('field1-field2', 'meta')
    """

    parts = field.split(separator * 2)
    parts[-1], *meta = parts[-1].split("_", 1)
    return separator.join(parts), meta[0] if meta else None
