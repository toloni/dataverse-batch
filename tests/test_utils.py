import pytest
import logging
import tempfile
import os
from dataverse_batch.utils import setup_logging, validate_data


class TestUtils:

    def test_setup_logging_console_only(self):
        """Test logging setup with console only"""
        logger = setup_logging("DEBUG")

        assert isinstance(logger, logging.Logger)
        assert logger.level == logging.DEBUG
        assert len(logger.handlers) >= 1  # At least console handler

    def test_setup_logging_with_file(self):
        """Test logging setup with file handler"""
        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as temp_file:
            temp_path = temp_file.name

        logger = None
        root_logger = None
        try:
            # Clear any existing handlers to avoid duplicates
            root_logger = logging.getLogger("dataverse_batch")
            root_logger.handlers.clear()

            logger = setup_logging("INFO", log_file=temp_path)

            assert isinstance(logger, logging.Logger)
            assert logger.level == logging.INFO

            print(f"Logger handlers: {logger.handlers}")
            print(f"Logger name: {logger.name}")

            # Test that file handler was added (check both FileHandler and its subclasses)
            file_handlers = [
                h
                for h in logger.handlers
                if isinstance(
                    h, (logging.FileHandler, logging.handlers.RotatingFileHandler)
                )
            ]

            # If no file handler in logger, check root logger
            if not file_handlers and root_logger is not None:
                file_handlers = [
                    h
                    for h in root_logger.handlers
                    if isinstance(
                        h, (logging.FileHandler, logging.handlers.RotatingFileHandler)
                    )
                ]

            assert len(file_handlers) == 1
            assert file_handlers[0].baseFilename == temp_path

        finally:
            # Clean up handlers only if the logger/root_logger were created
            if logger:
                for handler in list(logger.handlers):
                    handler.close()
                    logger.removeHandler(handler)
            if root_logger:
                for handler in list(root_logger.handlers):
                    handler.close()
                    root_logger.removeHandler(handler)

            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_setup_logging_no_duplicates(self):
        """Test that logging doesn't create duplicate handlers"""
        logger1 = setup_logging("INFO")
        handler_count_first = len(logger1.handlers)

        logger2 = setup_logging("INFO")
        handler_count_second = len(logger2.handlers)

        assert handler_count_first == handler_count_second
        assert logger1 is logger2

    def test_validate_data_success(self, sample_account_data):
        """Test successful data validation"""
        result = validate_data(sample_account_data, "accounts")
        assert result is True

    def test_validate_data_not_list(self):
        """Test validation with non-list data"""
        with pytest.raises(ValueError, match="The 'data' parameter must be a list"):
            validate_data("not a list", "accounts")

    def test_validate_data_not_dict_elements(self):
        """Test validation with non-dictionary elements"""
        invalid_data = [{"valid": "dict"}, "not a dict", {"another": "valid"}]

        with pytest.raises(
            ValueError, match="All elements in the 'data' list must be dictionaries"
        ):
            validate_data(invalid_data, "accounts")

    def test_validate_data_invalid_table_name(self, sample_account_data):
        """Test validation with invalid table name"""
        with pytest.raises(
            ValueError, match="The 'table_name' parameter must be a non-empty string"
        ):
            validate_data(sample_account_data, "")

        with pytest.raises(
            ValueError, match="The 'table_name' parameter must be a non-empty string"
        ):
            validate_data(sample_account_data, None)
