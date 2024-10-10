import json
from json import JSONDecodeError
from pathlib import Path
from typing import List, Dict

from pipeline_logger import logger


class AttributeLoader:
    def __init__(self, root_dir: Path = None):
        self.root_dir = root_dir or Path(__file__).parent

    def _load_json(self, filename: str) -> List[Dict]:
        path = self.root_dir / "qradar" / "input" / filename
        try:
            with path.open("r") as f:
                return json.load(f)
        except (FileNotFoundError, JSONDecodeError) as e:
            if isinstance(e, JSONDecodeError):
                # Read the first few lines to identify the problematic content
                with path.open("r") as f:
                    lines = [next(f) for _ in range(3)]  # Read up to 3 lines
                    error_context = "".join(lines)
                    msg = (
                        f"Error loading {filename}: Invalid JSON format. "
                        f"Expecting property name enclosed in double quotes. "
                        f"Error near:\n{error_context}"
                    )
            else:
                msg = f"Error loading {filename}: {e}"

            logger.error(msg)
            raise SystemExit(msg)  # Exit gracefully with error message

    def load_queries(self) -> List[Dict]:
        return self._load_json("queries.json")

    def load_ep_client_list(self) -> List[Dict]:
        return self._load_json("ep_clients.json")

    def load_duration(self) -> List[Dict]:
        return self._load_json("duration.json")


def load_attributes():
    loader = AttributeLoader()
    try:
        return {
            "ep_client_list": loader.load_ep_client_list(),
            "queries": loader.load_queries(),
            "duration": loader.load_duration()
        }
    except SystemExit:
        # Don't catch SystemExit here, let it propagate to stop execution
        raise
