from dotenv import load_dotenv
load_dotenv()

from .meteoblue import _MeteoblueIngestor, _MeteoblueRetriever
import importlib.util
if importlib.util.find_spec('pygeoapi') is not None:
    from .meteoblue import MeteoblueIngestorProcessor, MeteoblueRetrieverProcessor

from .main import run_meteoblue_ingestor
from .utils.strings import parse_event
