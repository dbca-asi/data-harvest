import sys

import utils

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("Command missing.")

    command = utils.load_module(sys.argv[1])
    command.run()
