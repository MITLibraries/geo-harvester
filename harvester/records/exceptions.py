import re
import traceback

from jsonschema.exceptions import ValidationError


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
