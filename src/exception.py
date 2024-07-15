import sys

from src.logger import logging

def error_message_detail(error, error_detail: sys):
    """
    Generates a detailed error message including file name and line number of the exception.

    Args:
        error (Exception): The error object.
        error_detail (sys): System information related to the error.

    Returns:
        str: Detailed error message formatted as "[File Name] [Line Number]: [Error Message]".
    """
    _, _, ex_tb = error_detail.exc_info()
    file_name = ex_tb.tb_frame.f_code.co_filename
    error_message = f"Error occurred in Python script '{file_name}' at line {ex_tb.tb_lineno}: {str(error)}"
    return error_message

class CustomException(Exception):
    """
    Custom exception class that captures and formats detailed error messages.

    Attributes:
        error_message (str): Detailed error message including file name and line number.
    """
    def __init__(self, error_message, error_detail):
        """
        Initialize the CustomException with an error message.

        Args:
            error_message (str): The error message to be logged.
            error_detail (sys): System information related to the error.
        """
        super().__init__(error_message)
        self.error_message = error_message_detail(error_message, error_detail=error_detail)

    def __str__(self):
        """
        String representation of the CustomException.

        Returns:
            str: The detailed error message.
        """
        return self.error_message
