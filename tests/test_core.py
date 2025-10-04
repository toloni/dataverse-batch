import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from dataverse_batch.core import DataverseBatch
from dataverse_batch.utils import validate_data


class TestDataverseBatch:
    
    @patch('dataverse_batch.core.DataverseClient')
    @patch('dataverse_batch.core.setup_logging')
    def test_initialization(self, mock_setup_logging, mock_client_class, sample_config):
        """Test DataverseBatch initialization"""
        mock_logger = Mock()
        mock_setup_logging.return_value = mock_logger
        
        mock_client_instance = Mock()
        mock_client_instance.test_connection.return_value = True
        mock_client_class.return_value = mock_client_instance
        
        dataverse_batch = DataverseBatch(**sample_config)
        
        # Verify initialization
        mock_setup_logging.assert_called_once_with('INFO', None)
        mock_client_class.assert_called_once_with(**sample_config)
        assert dataverse_batch.logger == mock_logger
        assert dataverse_batch.client == mock_client_instance
    
    @patch('dataverse_batch.core.DataverseClient')
    @patch('dataverse_batch.core.setup_logging')
    def test_initialization_with_custom_logging(self, mock_setup_logging, mock_client_class, sample_config):
        """Test initialization with custom logging settings"""
        mock_logger = Mock()
        mock_setup_logging.return_value = mock_logger
        
        mock_client_instance = Mock()
        mock_client_instance.test_connection.return_value = True
        mock_client_class.return_value = mock_client_instance
        
        dataverse_batch = DataverseBatch(
            **sample_config,
            log_level='DEBUG',
            log_file='test.log'
        )
        
        mock_setup_logging.assert_called_once_with('DEBUG', 'test.log')
    
    @patch('dataverse_batch.core.DataverseClient')
    @patch('dataverse_batch.core.setup_logging')
    @patch('dataverse_batch.core.BatchProcessor')
    def test_create_multiple_sequential(self, mock_processor_class, mock_setup_logging, mock_client_class, sample_config, sample_account_data):
        """Test sequential record creation"""
        mock_logger = Mock()
        mock_setup_logging.return_value = mock_logger
        
        mock_client_instance = Mock()
        mock_client_instance.test_connection.return_value = True
        mock_client_class.return_value = mock_client_instance
        
        mock_processor_instance = Mock()
        mock_processor_instance.process_sequential.return_value = pd.DataFrame([
            {'record_index': 0, 'status': 'success', 'id': '111'},
            {'record_index': 1, 'status': 'success', 'id': '222'}
        ])
        mock_processor_class.return_value = mock_processor_instance
        
        dataverse_batch = DataverseBatch(**sample_config)
        
        result = dataverse_batch.create_multiple(
            data=sample_account_data[:2],
            table='accounts',
            batch_size=100,
            parallel=False
        )
        
        # Verify calls
        mock_processor_class.assert_called_once_with(mock_client_instance, 'accounts', 100)
        mock_processor_instance.process_sequential.assert_called_once_with(sample_account_data[:2])
        
        # Verify logging
        assert mock_logger.info.call_count >= 3  # Initialization + processing messages
    
    @patch('dataverse_batch.core.DataverseClient')
    @patch('dataverse_batch.core.setup_logging')
    @patch('dataverse_batch.core.BatchProcessor')
    def test_create_multiple_parallel(self, mock_processor_class, mock_setup_logging, mock_client_class, sample_config, sample_account_data):
        """Test parallel record creation"""
        mock_logger = Mock()
        mock_setup_logging.return_value = mock_logger
        
        mock_client_instance = Mock()
        mock_client_instance.test_connection.return_value = True
        mock_client_class.return_value = mock_client_instance
        
        mock_processor_instance = Mock()
        mock_processor_instance.process_parallel.return_value = pd.DataFrame([
            {'record_index': 0, 'status': 'success', 'id': '111'},
            {'record_index': 1, 'status': 'success', 'id': '222'}
        ])
        mock_processor_class.return_value = mock_processor_instance
        
        dataverse_batch = DataverseBatch(**sample_config)
        
        result = dataverse_batch.create_multiple(
            data=sample_account_data[:2],
            table='accounts',
            batch_size=50,
            parallel=True,
            workers=5
        )
        
        # Verify calls
        mock_processor_class.assert_called_once_with(mock_client_instance, 'accounts', 50)
        mock_processor_instance.process_parallel.assert_called_once_with(sample_account_data[:2], 5)
    
    @patch('dataverse_batch.core.DataverseClient')
    @patch('dataverse_batch.core.setup_logging')
    def test_create_multiple_validation_error(self, mock_setup_logging, mock_client_class, sample_config):
        """Test validation errors in create_multiple"""
        mock_logger = Mock()
        mock_setup_logging.return_value = mock_logger
        
        mock_client_instance = Mock()
        mock_client_instance.test_connection.return_value = True
        mock_client_class.return_value = mock_client_instance
        
        dataverse_batch = DataverseBatch(**sample_config)
        
        # Test with invalid data (not a list)
        with pytest.raises(ValueError, match="The 'data' parameter must be a list"):
            dataverse_batch.create_multiple(
                data="not a list",  # Invalid data
                table='accounts'
            )
        
        # Test with invalid table name
        with pytest.raises(ValueError, match="The 'table_name' parameter must be a non-empty string"):
            dataverse_batch.create_multiple(
                data=[{'name': 'Test'}],
                table=''  # Invalid table name
            )
    
    @patch('dataverse_batch.core.DataverseClient')
    @patch('dataverse_batch.core.setup_logging')
    def test_get_batch_recommendations(self, mock_setup_logging, mock_client_class, sample_config):
        """Test batch recommendations"""
        mock_logger = Mock()
        mock_setup_logging.return_value = mock_logger
        
        mock_client_instance = Mock()
        mock_client_instance.test_connection.return_value = True
        mock_client_class.return_value = mock_client_instance
        
        dataverse_batch = DataverseBatch(**sample_config)
        
        # Test with small number of records
        recommendations = dataverse_batch.get_batch_recommendations(100)
        
        assert recommendations['batch_size'] == 100
        assert recommendations['parallel'] is False
        assert recommendations['workers'] == 2
        
        # Test with large number of records
        recommendations = dataverse_batch.get_batch_recommendations(5000)
        
        assert recommendations['batch_size'] == 1000
        assert recommendations['parallel'] is True
        assert recommendations['workers'] == 10
        
        # Test with medium number of records
        recommendations = dataverse_batch.get_batch_recommendations(1500)
        
        assert 100 <= recommendations['batch_size'] <= 1000
        assert recommendations['parallel'] is True
        assert 2 <= recommendations['workers'] <= 10