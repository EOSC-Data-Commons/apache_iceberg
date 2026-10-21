import json
from jsonschema.validators import validate
from jsonschema.exceptions import ValidationError
from pathlib import Path
import jq

_REPO_ROOT = Path(__file__).resolve().parents[2]
_RESOURCES_FOLDER = _REPO_ROOT / "resources"

zenodo_datacite_jq_path = _RESOURCES_FOLDER / "zenodo_to_datacite.jq"
zenodo_datacite_transformer = jq.compile(zenodo_datacite_jq_path.read_text())


def transform_json_zenodo_to_datacite(zenodo_json: dict) -> str:
    """
    Transform Zenodo raw JSON to DataCite schema JSON.
    The result is stringified JSON, needs to go thru json.loads before validating against Datacite schema.
    """
    result = zenodo_datacite_transformer.input_value(zenodo_json)
    return result.text()
