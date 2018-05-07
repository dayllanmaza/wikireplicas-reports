import pkgutil
from generators import *
import sys

def main():

    args = sys.argv[1:]

    if args:
        for generator in args:
            getattr(eval(generator), 'generate_data')()
    else:
        # run everything
        generators = [name for _, name, _ in pkgutil.iter_modules(['generators'])]
        for generator in generators:
            # super dirty, whateves. Maybe use importlib ?
            getattr(eval(generator), 'generate_data')()


if __name__ == '__main__':
    main()
