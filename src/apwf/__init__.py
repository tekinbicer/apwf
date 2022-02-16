import logging
from importlib.metadata import version, PackageNotFoundError

logging.getLogger(__name__).addHandler(logging.NullHandler())

try:
    __version__ = version('apwf')
except PackageNotFoundError:
    logging.warning("Unable to find version (importlib.metadata) package.")