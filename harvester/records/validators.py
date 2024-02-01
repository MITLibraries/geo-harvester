import json
import logging
import os
import re

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from harvester.records import MITAardvark

import shapely

from ast import literal_eval
from attrs import define, field
from jsonschema import FormatChecker
from jsonschema.validators import Draft202012Validator
from referencing import Registry, Resource

from harvester.records.exceptions import DataValidationWarning, JSONSchemaValidationError

logger = logging.getLogger(__name__)


@define
class MITAardvarkDataValidator:

    normalized_record: "MITAardvark"

    def _valid_geodata_string(self, geodata_string: str):
        if envelope := re.compile(r"^ENVELOPE\s?(.*)").match(geodata_string):
            try:
                minLon, maxLon, maxLat, minLat = literal_eval(envelope.group(1))
                geoshape_object = shapely.box(
                    xmin=minLon, ymin=minLat, xmax=maxLon, ymax=maxLat
                )
            except Exception:
                return f"unable to parse geodata string: '{geodata_string}'"
            if not geoshape_object.is_valid:
                return (
                    f"invalid geoshape object for geodata string: '{geodata_string}' "
                    f"reason: {shapely.is_valid_reason(geoshape_object)}"
                )
        else:
            try:
                geoshape_object = shapely.from_wkt(geodata_string)
            except Exception:
                return f"unable to parse geodata string: '{geodata_string}'"
            if not geoshape_object.is_valid:
                return (
                    f"invalid geoshape object for geodata string: '{geodata_string}' "
                    f"reason: {shapely.is_valid_reason(geoshape_object)}"
                )

    def dcat_bbox(self):
        if dcat_bbox_value := self.normalized_record.dcat_bbox:
            if self._valid_geodata_string(dcat_bbox_value):
                error_message = (
                    f"field: dcat_bbox, {self._valid_geodata_string(dcat_bbox_value)}"
                )
                dcat_bbox_value = None
                return error_message

    def locn_geometry(self):
        if locn_geometry_value := self.normalized_record.locn_geometry:
            if self._valid_geodata_string(locn_geometry_value):
                error_message = f"field: locn_geometry, {self._valid_geodata_string(locn_geometry_value)}"
                locn_geometry_value = None
                return error_message

    def get_validation_warnings(self):
        data_warnings = []
        if dcat_bbox_error := self.dcat_bbox():
            data_warnings.append(dcat_bbox_error)
        if locn_geometry_error := self.locn_geometry():
            data_warnings.append(locn_geometry_error)

        if data_warnings:
            return DataValidationWarning(data_warnings)

    def get_validation_errors(self):
        return


@define
class MITAardvarkStructureValidator:

    normalized_record: "MITAardvark"

    @property
    def jsonschemas(self) -> dict:
        """Load JSON schemas for validating MITAardvark records.

        To validate MITAardvark records, the validator relies on two schemas:
           * MITAardvark schema;
           * OpenGeoMetadata's (OGM) Geoblacklight Aardvark schema.

        The MITAardvark schema will comprise of majority of OGM's Aardvark schema,
        with several updates for MIT's purposes. The schemas are read from
        harvester/records/schemas directory and later added to a referencing.Registry.
        Once in the registry, the validator can use the schemas to validate data.

        Returns:
            dict: JSON schemas for validating MITAardvark records.
        """
        schemas = {}
        schema_dir = os.path.join(os.path.dirname(__file__), "schemas")
        with open(schema_dir + "/mit-schema-aardvark.json") as f:
            schemas["mit-schema-aardvark"] = json.loads(f.read())
        with open(schema_dir + "/geoblacklight-schema-aardvark.json") as f:
            schemas["geoblacklight-schema-aardvark"] = json.loads(f.read())
        return schemas

    @property
    def jsonschema_validator(self) -> Draft202012Validator:
        """Create a validator with JSON schemas for evaluating MITAardvark records.

        An instance referencing.Registry is created with the required schema added as
        resources. When the validator is created, the registry is included as an argument.
        This enables the validator to use the schemas for validation.

        Note: For more information on
            * registries: https://python-jsonschema.readthedocs.io/en/stable/referencing
            * validators: https://python-jsonschema.readthedocs.io/en/stable/validate/#the-validator-protocol

        Returns:
            Draft202012Validator: JSON schema validator with MITAardvark and OGM Aardvark
                schemas.
        """
        registry: Registry = Registry().with_resources(
            [
                (
                    "mit-schema-aardvark",
                    Resource.from_contents(self.jsonschemas["mit-schema-aardvark"]),
                ),
                (
                    "geoblacklight-schema-aardvark",
                    Resource.from_contents(
                        self.jsonschemas["geoblacklight-schema-aardvark"]
                    ),
                ),
            ]
        )
        return Draft202012Validator(
            schema=self.jsonschemas["mit-schema-aardvark"],
            registry=registry,
            format_checker=FormatChecker(),
        )

    def get_validation_errors(self) -> JSONSchemaValidationError | None:
        """Validate that Aardvark is compliant for MIT purposes.

        The validator is retrieved in order to use .iter_errors() to iterate through
        each of the validation errors in the normalized record. If there are any errors,
        they are compiled into a single error message that appears in a
        JSONSchemaValidationError exception.
        """
        if schema_validation_errors := sorted(
            self.jsonschema_validator.iter_errors(self.normalized_record.to_dict()),
            key=str,
        ):
            validation_error = JSONSchemaValidationError(schema_validation_errors)
            logger.error(validation_error)
            return validation_error
