import traceback


class FieldMethodError(Exception):
    """Exception to raise from normalize() method"""

    def __init__(self, original_exception: Exception, message: str):
        self.original_exception = original_exception
        self.message = message
        super().__init__(self.message)

    def get_formatted_traceback(self) -> str:
        """Get string representation of the original exception traceback."""
        return "".join(traceback.format_tb(self.original_exception.__traceback__))
