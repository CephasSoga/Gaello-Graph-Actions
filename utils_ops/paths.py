from pathlib import Path

from typing import Union

def constructPath(basePath: Path, *pathsParts: Union[str, Path]) -> Path:
    """
    Constructs a path by appending path_parts to the base_path.
    """
    return basePath.joinpath(*pathsParts)