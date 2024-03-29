import re
import traceback

from jsonschema.exceptions import ValidationError


class FieldValueInvalidWarning(Warning):
    """Warning to log when a validator determines field method returns an invalid value"""


class FieldMethodError(Exception):
    """Exception to raise from normalize() method"""

    def __init__(self, original_exception: Exception, message: str):
        self.original_exception = original_exception
        self.message = message
        super().__init__(self.message)

    def get_formatted_traceback(self) -> str:
        """Get string representation of the original exception traceback."""
        return "".join(traceback.format_tb(self.original_exception.__traceback__))


class JSONSchemaValidationError(ValidationError):
    """Exception to raise from MITAardvark.validate() method"""

    def __init__(self, validation_errors: list):
        self.validation_errors = validation_errors
        super().__init__(message=self.get_formatted_message())

    def get_formatted_message(self) -> str:
        """Get string representation of the compiled errors from JSON schema validation.

        The default error messages from jsonschema.exceptions.ValidationError does not
        include the field name. The field name can be extracted from the the .json_path
        attribute. Below are some examples of the validation errors in their original
        format:

            1. A validation error for a field expecting a 'date-time' format:
               - ValidationError.json_path: $.gbl_mdModified_dt
               - ValidationError.message: 2023-12-13' is not a 'date-time'
            2. A validation error for a field that is required:
               - ValidationError.json_path: $
               - ValidationError.message: 'dct_accessRights_s' is a required property

        This method ensures that the error messages adhere to the following format:
            "field: <field_name>, <validation_error_message",

        Returns:
            str: Compiled validation error messages from JSON schema validation.
        """
        error_messages = []
        for error in self.validation_errors:
            if "is a required property" in error.message:
                field = error.message.split()[0].replace("'", "")
            else:
                field = re.sub(r"\$\.*", "", error.json_path)
            error_messages.append(f"field: {field}, {error.message}")

        return "\n".join(
            ["The normalized MITAardvark record is invalid:", *error_messages]
        )


class NoExternalUrlError(Exception):
    """Exception to raise when external URL cannot be determined from OGM record."""

    def __init__(
        self, message: str = "Could not determine external URL from source metadata"
    ) -> None:
        super().__init__(message)
