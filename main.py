import pkgutil
from generators import *

def main():
    generators = [name for _, name, _ in pkgutil.iter_modules(['generators'])]
    for generator in generators:
        # super dirty, whateves. Maybe user importlib ?
        getattr(eval(generator), 'generate_data')()

if __name__ == '__main__':
    main()
