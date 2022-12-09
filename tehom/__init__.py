import sys
import argparse

from ._version import get_versions
from ._persistence import save_user_token
from .downloads import download_ships, download_acoustics, certify_audio_availability

__version__ = get_versions()["version"]
del get_versions


save_token_parser = argparse.ArgumentParser(description=save_user_token.__doc__)
save_token_parser.add_argument("token")
save_token_parser.add_argument("-f", "--force", type=bool, default=False)


download_ships_parser = argparse.ArgumentParser(
    description="Download ship tracking data or hydrophone acoustics"
)
download_ships_parser.add_argument("year", type=int)
download_ships_parser.add_argument("month", type=int)
download_ships_parser.add_argument("zone", type=int)


download_acoustics_parser = argparse.ArgumentParser(
    description="Download ship tracking data or hydrophone acoustics"
)


def _list_or_str(h_phones):
    return h_phones.split(",")


download_acoustics_parser.add_argument("hydrophones", type=_list_or_str)
download_acoustics_parser.add_argument("begin")
download_acoustics_parser.add_argument("end")
download_acoustics_parser.add_argument("extension")


def __main__():
    subcommand = sys.argv[1]
    if subcommand == "save-token":
        parser = save_token_parser
        subcommand = save_user_token
    elif subcommand == "ships":
        parser = download_ships_parser
        subcommand = download_ships
    elif subcommand == "sound":
        parser = download_acoustics_parser
        subcommand = download_acoustics
    elif subcommand == "cert":
        parser = argparse.ArgumentParser(
            description="Certify hydrophone availability (no arguments)"
        )
        subcommand = certify_audio_availability
    else:
        raise ValueError(f"No subcommand named '{subcommand}'")
    args = parser.parse_args(sys.argv[2:])
    subcommand(**vars(args))
