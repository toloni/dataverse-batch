import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from dataverse_batch.batch_processor import BatchProcessor


class TestBatchProcessor:

    @pytest.fixture
    def mock_client(self):
        client = Mock()
        client.create_records_batch = Mock()
        return client

    @pytest.fixture
    def processor(self, mock_client):
        return BatchProcessor(mock_client, "accounts", 2)

    def test_initialization(self, mock_client):
        """Test processor initialization"""
        processor = BatchProcessor(mock_client, "accounts", 100)

        assert processor.client == mock_client
        assert processor.table_name == "accounts"
        assert processor.batch_size == 100

    def test_chunk_data(self, processor, sample_account_data):
        """Test data chunking"""
        chunks = processor._chunk_data(sample_account_data)

        assert len(chunks) == 2  # 3 records with batch_size=2
        assert len(chunks[0]) == 2
        assert len(chunks[1]) == 1
        assert chunks[0][0]["name"] == "Test Company 1"
        assert chunks[0][1]["name"] == "Test Company 2"
        assert chunks[1][0]["name"] == "Test Company 3"

    def test_chunk_data_exact_batch_size(self, mock_client, sample_account_data):
        """Test chunking with exact batch size"""
        processor = BatchProcessor(mock_client, "accounts", 3)
        chunks = processor._chunk_data(sample_account_data)

        assert len(chunks) == 1
        assert len(chunks[0]) == 3

    def test_chunk_data_empty(self, processor):
        """Test chunking empty data"""
        chunks = processor._chunk_data([])

        assert chunks == []

    @patch("dataverse_batch.batch_processor.tqdm")
    @patch("dataverse_batch.batch_processor.time.sleep")
    def test_process_sequential_success(
        self, mock_sleep, mock_tqdm, processor, sample_account_data
    ):
        """Test successful sequential processing"""
        # Mock client responses
        processor.client.create_records_batch.side_effect = [
            [
                {
                    "record_index": 0,
                    "status": "success",
                    "id": "111",
                    "data": sample_account_data[0],
                },
                {
                    "record_index": 1,
                    "status": "success",
                    "id": "222",
                    "data": sample_account_data[1],
                },
            ],
            [
                {
                    "record_index": 2,
                    "status": "success",
                    "id": "333",
                    "data": sample_account_data[2],
                }
            ],
        ]

        # Mock tqdm
        mock_progress = Mock()
        mock_tqdm.return_value.__enter__.return_value = mock_progress

        result_df = processor.process_sequential(sample_account_data)

        # Verify results
        assert len(result_df) == 3
        assert all(result_df["status"] == "success")
        assert processor.client.create_records_batch.call_count == 2

        # Verify progress was updated
        assert mock_progress.update.call_count == 2
        mock_progress.update.assert_any_call(2)
        mock_progress.update.assert_any_call(1)

    @patch("dataverse_batch.batch_processor.tqdm")
    @patch("dataverse_batch.batch_processor.time.sleep")
    def test_process_sequential_with_errors(
        self, mock_sleep, mock_tqdm, processor, sample_account_data
    ):
        """Test sequential processing with errors"""
        # Mock client responses with errors
        processor.client.create_records_batch.side_effect = [
            [
                {
                    "record_index": 0,
                    "status": "success",
                    "id": "111",
                    "data": sample_account_data[0],
                },
                {
                    "record_index": 1,
                    "status": "error",
                    "error": "Name required",
                    "data": sample_account_data[1],
                },
            ],
            [
                {
                    "record_index": 2,
                    "status": "success",
                    "id": "333",
                    "data": sample_account_data[2],
                }
            ],
        ]

        mock_progress = Mock()
        mock_tqdm.return_value.__enter__.return_value = mock_progress

        result_df = processor.process_sequential(sample_account_data)

        # Verify results
        assert len(result_df) == 3
        success_count = len(result_df[result_df["status"] == "success"])
        error_count = len(result_df[result_df["status"] == "error"])

        assert success_count == 2
        assert error_count == 1

    @patch("dataverse_batch.batch_processor.as_completed")
    @patch("dataverse_batch.batch_processor.ThreadPoolExecutor")
    @patch("dataverse_batch.batch_processor.tqdm")
    def test_process_parallel_success(
        self,
        mock_tqdm,
        mock_executor,
        mock_as_completed,
        processor,
        sample_account_data,
    ):
        """Test successful parallel processing"""
        # Mock executor and futures
        mock_future1 = Mock()
        mock_future2 = Mock()

        mock_future1.result.return_value = [
            {
                "record_index": 0,
                "status": "success",
                "id": "111",
                "data": sample_account_data[0],
            },
            {
                "record_index": 1,
                "status": "success",
                "id": "222",
                "data": sample_account_data[1],
            },
        ]

        mock_future2.result.return_value = [
            {
                "record_index": 2,
                "status": "success",
                "id": "333",
                "data": sample_account_data[2],
            }
        ]

        mock_executor.return_value.__enter__.return_value.submit.side_effect = [
            mock_future1,
            mock_future2,
        ]

        mock_executor.return_value.__enter__.return_value.__enter__.return_value.as_completed.return_value = [
            mock_future1,
            mock_future2,
        ]

        # Mock executor submit
        mock_executor_instance = MagicMock()
        mock_executor.return_value.__enter__.return_value = mock_executor_instance
        mock_executor_instance.submit.side_effect = [mock_future1, mock_future2]

        # Mock as_completed to return our futures
        mock_as_completed.return_value = [mock_future1, mock_future2]

        # Mock tqdm
        mock_progress = Mock()
        mock_tqdm.return_value.__enter__.return_value = mock_progress

        result_df = processor.process_parallel(sample_account_data, workers=2)

        # Verify results
        assert len(result_df) == 3
        assert all(result_df["status"] == "success")

        # Verify progress was updated
        assert mock_progress.update.call_count == 2
        mock_progress.update.assert_any_call(2)
        mock_progress.update.assert_any_call(1)

    @patch("dataverse_batch.batch_processor.as_completed")
    @patch("dataverse_batch.batch_processor.ThreadPoolExecutor")
    @patch("dataverse_batch.batch_processor.tqdm")
    def test_process_parallel_with_exceptions(
        self,
        mock_tqdm,
        mock_executor,
        mock_as_completed,
        processor,
        sample_account_data,
    ):
        """Test parallel processing with exceptions"""
        # Mock executor with one failing future
        mock_future1 = Mock()
        mock_future2 = Mock()

        mock_future1.result.side_effect = Exception("Network error")
        mock_future2.result.return_value = [
            {
                "record_index": 2,
                "status": "success",
                "id": "333",
                "data": sample_account_data[2],
            }
        ]

        # Mock executor submit
        mock_executor_instance = MagicMock()
        mock_executor.return_value.__enter__.return_value = mock_executor_instance
        mock_executor_instance.submit.side_effect = [mock_future1, mock_future2]

        mock_as_completed.return_value = [mock_future1, mock_future2]

        # Mock tdqm
        mock_progress = Mock()
        mock_tqdm.return_value.__enter__.return_value = mock_progress

        result_df = processor.process_parallel(sample_account_data, workers=2)

        # Verify results include errors
        assert len(result_df) == 3

        # Should have 1 success and 2 errors (from the failed batch)
        success_count = len(result_df[result_df["status"] == "success"])
        error_count = len(result_df[result_df["status"] == "error"])

        assert success_count == 1
        assert error_count == 2

    def test_create_results_dataframe(self, processor, sample_account_data):
        """Test DataFrame creation from results"""
        results = [
            {
                "record_index": 0,
                "status": "success",
                "id": "111",
                "data": sample_account_data[0],
            },
            {
                "record_index": 1,
                "status": "error",
                "error": "Validation failed",
                "data": sample_account_data[1],
            },
            {
                "record_index": 2,
                "status": "success",
                "id": "333",
                "data": sample_account_data[2],
            },
        ]

        result_df = processor._create_results_dataframe(results, sample_account_data)

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 3

        # Check columns exist (order doesn't matter)
        expected_columns = {"record_index", "status", "id", "error", "data"}
        assert expected_columns.issubset(result_df.columns)

        # Verify data
        assert result_df.iloc[0]["status"] == "success"
        assert result_df.iloc[0]["id"] == "111"
        assert result_df.iloc[1]["status"] == "error"
        assert result_df.iloc[1]["error"] == "Validation failed"
        assert result_df.iloc[2]["status"] == "success"
        assert result_df.iloc[2]["id"] == "333"
