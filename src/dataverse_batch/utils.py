import logging
import sys
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime

def setup_logging(log_level: str = "INFO", log_file: str = None):
    """Configure logging system"""
    
    # Create logger
    logger = logging.getLogger('dataverse_batch')
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Avoid duplicate logs
    if logger.handlers:
        return logger
    
    # Log format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def validate_data(data, table_name: str) -> bool:
    """Validate input data"""
    if not isinstance(data, list):
        raise ValueError("The 'data' parameter must be a list")
    
    if not all(isinstance(item, dict) for item in data):
        raise ValueError("All elements in the 'data' list must be dictionaries")
    
    if not table_name or not isinstance(table_name, str):
        raise ValueError("The 'table_name' parameter must be a non-empty string")
    
    return True