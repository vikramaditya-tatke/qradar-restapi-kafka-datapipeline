import json
from pathlib import Path
from typing import List, Dict


class AttributeLoader:
    def __init__(self, root_dir: Path = None):
        self.root_dir = root_dir or Path(__file__).parent
        # Makes root directory configurable for testing

    def _load_json(self, filename: str) -> List[Dict]:
        # Combine common loading logic into one private helper
        path = self.root_dir / "qradar" / "input" / filename
        with path.open("r") as f:
            return json.load(f)

    def load_queries(self) -> List[Dict]:
        return self._load_json("queries.json")

    def load_ep_client_list(self) -> List[Dict]:
        # Type hint updated based on your AttributeConfig definition
        return self._load_json("ep_clients.json")


def load_attributes():
    loader = AttributeLoader()
    try:
        return {
            "ep_client_list": loader.load_ep_client_list(),
            "queries": loader.load_queries(),
        }
    except json.decoder.JSONDecodeError as e:
        raise print("Exception occured")
    except FileNotFoundError as fnf:
        raise FileNotFoundError(str(fnf))
