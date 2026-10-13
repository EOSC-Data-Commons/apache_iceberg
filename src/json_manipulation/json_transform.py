import json
from jsonschema.validators import validate
from jsonschema.exceptions import ValidationError
from pathlib import Path
import jq

zenodo_datacite_transformer = jq.compile(Path("../resources/zenodo_to_datacite.jq").read_text())

def transform_json_zenodo_to_datacite(zenodo_json:dict) -> str:
    result = zenodo_datacite_transformer.input_value(zenodo_json)
    return result.text()
