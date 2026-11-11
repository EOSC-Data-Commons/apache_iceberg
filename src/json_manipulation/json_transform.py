import json
from jsonschema.validators import validate
from jsonschema.exceptions import ValidationError
from pathlib import Path
import jq

_REPO_ROOT = Path(__file__).resolve().parents[2]
_RESOURCES_FOLDER = _REPO_ROOT / "resources"

zenodo_datacite_jq_path = _RESOURCES_FOLDER / "zenodo_to_datacite.jq"
zenodo_datacite_transformer = jq.compile(zenodo_datacite_jq_path.read_text())

with open(_RESOURCES_FOLDER / "datacite_schema.json") as f:
    datacite_schema = json.load(f)

zenodo_files_jq_path = _RESOURCES_FOLDER / "zenodo_dump_to_files_endpoint.jq"
zenodo_files_transformer = jq.compile(zenodo_files_jq_path.read_text())

with open(_RESOURCES_FOLDER / "zenodo_files_schema.json") as f:
    files_schema = json.load(f)

def transform_json_zenodo_to_datacite(zenodo_json: dict,
                                      validate_schema: bool = True) -> str:
    """
    Transform Zenodo raw JSON to DataCite schema JSON.
    The result is stringified JSON.

    Args:
        zenodo_json: dict, a JSON object, raw input from the Zenodo dump
        validate_schema: bool, whether to go through the validation of the result. Raises exception if validation failed.
    """
    result = zenodo_datacite_transformer.input_value(zenodo_json)

    if validate_schema:
        validate(result.first(), datacite_schema) 

    return result.text()

def transform_json_zenodo_to_files_endpoint(zenodo_json: dict,
                                      validate_schema: bool = True) -> str:
    """
    Transform Zenodo raw JSON to the JSON returned by the /files endpoint.  
    This is needed for "additional_metadata" in the warehouse, so it needs to be in the same schema.
    The result is stringified JSON.

    Args:
        zenodo_json: dict, a JSON object, raw input from the Zenodo dump
        validate_schema: bool, whether to go through the validation of the result. Raises exception if validation failed.
    """
    result = zenodo_files_transformer.input_value(zenodo_json)

    if validate_schema:
        validate(result.first(), files_schema) 

    return result.text()