import argparse
import sys
from src.logger import logger
from src.helpers import Helpers
from src.nexus import Nexus



def main():
    parser = argparse.ArgumentParser(description='Load and print YAML configuration.')
    parser.add_argument('config_file', metavar='config_file.yml', type=str,
                        help='path to the YAML configuration file')
    args = parser.parse_args()

    # Parse Config
    config = Helpers.read_yaml_file(args.config_file)

    if not config:
        logger.error(f'Error loading config from {args.config}. Are you sure you put in the right location?')
        sys.exit(1)

    nexus = Nexus(config)
    nexus.run()


if __name__ == '__main__':
    main()
