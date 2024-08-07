

def kwds_similarity(*kwds: list[str]) -> float:
    """
    Calculates the similarity score between multiple keyword lists.

    Args:
        *kwds (list[str]): Variable number of keyword lists.

    Returns:
        float: The similarity score between the keyword lists.

    Raises:
        None

    Examples:
        >>> kwds_similarity(['apple', 'banana', 'orange'], ['banana', 'orange', 'grape'])
        0.6666666666666666

    Notes:
        - The similarity score is calculated by taking the intersection of all keyword lists
          and dividing it by the minimum length of the input lists.
        - If any of the input lists are empty, the similarity score is 0.
    """
    if not kwds:
        return 0.0
    
    # Convert the first list to a set
    x = set(kwds[0])
    
    # Compute the intersection with all other lists
    for kw in kwds[1:]:
        y = set(kw)
        x = x.intersection(y)
    
    # Compute the similarity score
    # Return 0 if all lists are empty
    if len(kwds) == 0 or min(len(kw) for kw in kwds) == 0:
        return 0.0
    
    # Compute the minimum length of the input lists
    min_len = min(len(kw) for kw in kwds)
    
    return len(x) / min_len

