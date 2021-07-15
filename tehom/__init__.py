import sys
import argparse

from typing import Union
from pathlib import Path

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions


def save_user_token(token: Union[Path, str], force: bool = False) -> None:
    """Save the provided ONC token so future functions that require an
    ONC token will use this value by default.

    Arguments:
        token: the token string or location of the file.  If a file, the
            first line must contain the token and nothing else.
        force: whether to overwrite saved token if it exists
    """
    save_location = Path(__file__).parent / "token"
    if save_location.exists() and not force:
        raise OSError(
            "A saved token already exists.  If you want to"
            + "overwrite, pass `force=True`."
        )
    if Path(token).exists():
        with open(Path(token), "r") as fh:
            token = fh.readline()
    with open(save_location, "w") as fh:
        fh.write(token)


save_token_parser = argparse.ArgumentParser(
    description=save_user_token.__doc__
)
save_token_parser.add_argument("token")
save_token_parser.add_argument("-f", "--force", type=bool, default=False)


def __main__():
    subcommand = sys.argv[1]
    if subcommand == "save-token":
        parser = save_token_parser
        args = parser.parse_args(sys.argv[2:])
        subcommand = save_user_token
    else:
        raise ValueError(f"No subcommand named '{subcommand}'")
    subcommand(**vars(args))
