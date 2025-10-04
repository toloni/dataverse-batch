import pytest
import pandas as pd
from unittest.mock import patch, Mock
import responses


class TestIntegration:
    """Integration tests that combine multiple components"""
    
    @patch('dataverse_batch.core.DataverseClient')
    @patch('dataverse_batch.core.setup_logging')
    @responses.activate
    def test_end_to_end_success(self, mock_setup_logging, mock_client_class, sample_config, sample_account_data):
        """Test complete successful workflow"""
        # Setup mocks
        mock_logger = Mock()
        mock_setup_logging.return_value = mock_logger
        
        mock_client_instance = Mock()
        mock_client_instance.test_connection.return_value = True
        
        # Mock successful batch creation
        mock_client_instance.create_records_batch.return_value = [
            {
                'record_index': 0, 
                'status': 'success', 
                'id': '11111111-1111-1111-1111-111111111111',
                'data': sample_account_data[0]
            },
            {
                'record_index': 1,
                'status': 'success',
                'id': '22222222-2222-2222-2222-222222222222', 
                'data': sample_account_data[1]
            }
        ]
        mock_client_class.return_value = mock_client_instance
        
        from dataverse_batch import DataverseBatch
        
        # Initialize and process
        dataverse_batch = DataverseBatch(**sample_config)
        result = dataverse_batch.create_multiple(
            data=sample_account_data[:2],
            table='accounts',
            batch_size=2,
            parallel=False
        )
        
        # Verify results
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert all(result['status'] == 'success')
        assert mock_client_instance.create_records_batch.call_count == 1
    
    @patch('dataverse_batch.core.DataverseClient') 
    @patch('dataverse_batch.core.setup_logging')
    def test_end_to_end_with_recommendations(self, mock_setup_logging, mock_client_class, sample_config, sample_account_data):
        """Test workflow with automatic recommendations"""
        mock_logger = Mock()
        mock_setup_logging.return_value = mock_logger
        
        mock_client_instance = Mock()
        mock_client_instance.test_connection.return_value = True
        mock_client_instance.create_records_batch.return_value = [
            {'record_index': i, 'status': 'success', 'id': f'test-id-{i}'}
            for i in range(10)
        ]
        mock_client_class.return_value = mock_client_instance
        
        from dataverse_batch import DataverseBatch
        
        dataverse_batch = DataverseBatch(**sample_config)
        
        # Get recommendations for large dataset
        recommendations = dataverse_batch.get_batch_recommendations(5000)
        
        assert recommendations['parallel'] is True
        assert recommendations['batch_size'] == 1000
        assert recommendations['workers'] == 10
        
        # Use recommendations
        large_data = [{'name': f'Company {i}'} for i in range(5000)]
        
        with patch('dataverse_batch.batch_processor.BatchProcessor.process_parallel') as mock_process:
            mock_process.return_value = pd.DataFrame([
                {'record_index': i, 'status': 'success'} for i in range(5000)
            ])
            
            result = dataverse_batch.create_multiple(
                data=large_data,
                table='accounts',
                batch_size=recommendations['batch_size'],
                parallel=recommendations['parallel'],
                workers=recommendations['workers']
            )
            
            # Verify parallel processing was used
            mock_process.assert_called_once()