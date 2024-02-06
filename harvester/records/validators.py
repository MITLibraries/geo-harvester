import json
import logging
import os
import re
from ast import literal_eval
from collections.abc import Callable
from functools import partial, update_wrapper

import shapely
from attrs import define
from jsonschema import FormatChecker
from jsonschema.validators import Draft202012Validator
from referencing import Registry, Resource

from harvester.records.exceptions import (
    FieldValueInvalidWarning,
    JSONSchemaValidationError,
)

logger = logging.getLogger(__name__)


###################
# Data Validators
###################
class ValidateGeoshapeWKT:
    """Decorator class for validating geoshape WKT values.

    The validator should be applied to any field methods that retrieve geoshape
    WKT values. The validator logs a warning if the WKT value cannot be parsed
    using the shapely module. If the WKT value cannot be parsed, the validator
    resets the value to None.
    """

    invalid_wkt_warning_message: str = (
        "field: {field_name}, shapely was unable to parse WKT: '{value}'; "
        "setting value to None"
    )

    def __init__(self, field_method: Callable):
        update_wrapper(self, field_method)
        self.field_method = field_method

    def __call__(self, obj: object) -> str | None:
        field_name = self.field_method.__name__.removeprefix("_")
        value = self.field_method(obj)

        try:
            self.create_geoshape(value)
        except Exception:  # noqa: BLE001
            logger.warning(
                FieldValueInvalidWarning(
                    self.invalid_wkt_warning_message.format(
                        field_name=field_name, value=value
                    )
                )
            )
            return None
        return value

    def __get__(self, obj: object, _: type) -> partial[str | None]:
        """Required by decorator to access SourceRecord instance"""
        return partial(self.__call__, obj)

    @staticmethod
    def create_geoshape(wkt: str) -> shapely.Geometry:
        """Run shapely to determine whether geoshape WKT value is valid.

        If the geoshape WKT value is valid, shapely can successfully create a
        geometry object.

        Note: shapely does not currently support WKT values for bounding box regions or
        envelopes. The method uses regex to retrieving the vertices for a geoshape WKT
        value with the format: "ENVELOPE(<vertices>)". The regex retrieves the vertices
        inside the parentheses and passes the vertices as arguments to shapely.box().
        """
        if geoshape_string := re.compile(r"^ENVELOPE\s?(.*)").match(wkt):
            xmin, xmax, ymax, ymin = literal_eval(geoshape_string.group(1))
            return shapely.box(xmin, ymin, xmax, ymax)
        return shapely.from_wkt(wkt)


#####################
# Format Validators
#####################
@define
class MITAardvarkFormatValidator:
    """MITAardvark format validator class."""

    normalized_record: dict

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

    def validate(self) -> None:
        """Validate format of MITAardvark record using JSON schema.

        The validator is retrieved in order to use .iter_errors() to iterate through
        each of the validation errors in the normalized record. If there are any errors,
        they are compiled into a single error message that appears in a
        JSONSchemaValidationError exception.
        """
        if schema_validation_errors := sorted(
            self.jsonschema_validator.iter_errors(self.normalized_record),
            key=str,
        ):
            validation_error = JSONSchemaValidationError(schema_validation_errors)
            logger.error(validation_error)
            raise validation_error
        logger.debug("The normalized MITAardvark record is valid")
