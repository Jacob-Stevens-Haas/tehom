import sys

from . import save_user_token, save_token_parser


def __main__():
    subcommand = sys.argv[1]
    if subcommand == "save_token":
        parser = save_token_parser
        args = parser.parse_args(sys.argv[2:])
        save_user_token(**vars(args))
