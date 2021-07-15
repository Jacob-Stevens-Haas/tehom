import sys
import argparse

from ._version import get_versions
from ._persistence import save_user_token

__version__ = get_versions()["version"]
del get_versions


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
