import pytest
import httpx
import json
from unittest.mock import Mock, patch, MagicMock
from dataverse_batch.client import DataverseClient
import responses, respx


class TestDataverseClient:

    def test_initialization(self, sample_config):
        """Test client initialization"""
        with patch("dataverse_batch.client.DataverseClient._authenticate"):
            client = DataverseClient(**sample_config)

            assert client.dynamics_url == sample_config["dynamics_url"]
            assert client.client_id == sample_config["client_id"]
            assert client.client_secret == sample_config["client_secret"]
            assert client.tenant_id == sample_config["tenant_id"]

    @respx.mock
    def test_successful_authentication(
        self, sample_config, mock_authentication_response
    ):
        """Test successful authentication"""
        # Mock authentication endpoint
        auth_url = f"https://login.microsoftonline.com/{sample_config['tenant_id']}/oauth2/v2.0/token"
        respx.post(auth_url).mock(
            return_value=httpx.Response(200, json=mock_authentication_response)
        )

        client = DataverseClient(**sample_config)

        assert client.access_token == mock_authentication_response["access_token"]

    @responses.activate
    def test_authentication_failure(self, sample_config):
        """Test authentication failure"""
        auth_url = f"https://login.microsoftonline.com/{sample_config['tenant_id']}/oauth2/v2.0/token"
        responses.add(
            responses.POST, auth_url, json={"error": "invalid_client"}, status=400
        )

        with pytest.raises(Exception):
            DataverseClient(**sample_config)

    def test_get_headers(self, sample_config):
        """Test headers generation"""
        with patch("dataverse_batch.client.DataverseClient._authenticate"):
            client = DataverseClient(**sample_config)
            client.access_token = "test-token"

            headers = client._get_headers()

            assert headers["Authorization"] == "Bearer test-token"
            assert headers["Content-Type"] == "application/json"
            assert headers["OData-MaxVersion"] == "4.0"
            assert headers["OData-Version"] == "4.0"

    def test_create_batch_payload(self, sample_config, sample_account_data):
        """Test batch payload creation"""
        with patch("dataverse_batch.client.DataverseClient._authenticate"):
            client = DataverseClient(**sample_config)

            payload, headers = client._create_batch_payload(
                "accounts", sample_account_data[:2]
            )

            assert "multipart/mixed" in headers["Content-Type"]
            assert "batch_" in headers["Content-Type"]
            assert "Authorization" in headers

            # Verify payload structure
            assert (
                "POST https://test.crm.dynamics.com/api/data/v9.2/accounts HTTP/1.1"
                in payload
            )
            assert "Test Company 1" in payload
            assert "Test Company 2" in payload
            assert "Content-ID: 1" in payload
            assert "Content-ID: 2" in payload

    @respx.mock
    def test_create_records_batch_success(
        self, sample_config, sample_account_data, sample_batch_response_success
    ):
        """Test successful batch creation"""
        with patch("dataverse_batch.client.DataverseClient._authenticate"):
            client = DataverseClient(**sample_config)

            # Mock batch endpoint
            batch_url = f"{sample_config['dynamics_url']}/api/data/v9.2/$batch"
            respx.post(batch_url).mock(
                return_value=httpx.Response(200, text=sample_batch_response_success)
            )

            results = client.create_records_batch("accounts", sample_account_data[:2])

            assert len(results) == 2
            assert all(result["status"] == "success" for result in results)
            assert "id" in results[0]
            assert "id" in results[1]

    @respx.mock
    def test_create_records_batch_with_errors(
        self, sample_config, sample_account_data, sample_batch_response_with_errors
    ):
        """Test batch creation with errors"""
        with patch("dataverse_batch.client.DataverseClient._authenticate"):
            client = DataverseClient(**sample_config)

            # Mock batch endpoint with error response
            batch_url = f"{sample_config['dynamics_url']}/api/data/v9.2/$batch"
            respx.post(batch_url).mock(
                return_value=httpx.Response(200, text=sample_batch_response_with_errors)
            )

            results = client.create_records_batch("accounts", sample_account_data[:2])

            assert len(results) == 2
            assert results[0]["status"] == "success"
            assert results[1]["status"] == "error"
            assert "name" in results[1]["error"]

    @respx.mock
    def test_create_records_batch_http_error(self, sample_config, sample_account_data):
        """Test batch creation with HTTP error"""
        with patch("dataverse_batch.client.DataverseClient._authenticate"):
            client = DataverseClient(**sample_config)

            # Mock batch endpoint with HTTP error
            batch_url = f"{sample_config['dynamics_url']}/api/data/v9.2/$batch"
            respx.post(batch_url).mock(
                return_value=httpx.Response(500, text="Internal Server Error")
            )

            results = client.create_records_batch("accounts", sample_account_data[:2])

            assert len(results) == 2
            assert all(result["status"] == "error" for result in results)
            assert "HTTP 500" in results[0]["error"]

    def test_create_records_batch_empty_data(self, sample_config):
        """Test batch creation with empty data"""
        with patch("dataverse_batch.client.DataverseClient._authenticate"):
            client = DataverseClient(**sample_config)

            results = client.create_records_batch("accounts", [])

            assert results == []

    @respx.mock
    def test_test_connection_success(self, sample_config):
        """Test successful connection test"""
        with patch("dataverse_batch.client.DataverseClient._authenticate"):
            client = DataverseClient(**sample_config)
            client.access_token = "test-token"

            # Mock test endpoint
            test_url = f"{sample_config['dynamics_url']}/api/data/v9.2"
            respx.get(test_url).mock(return_value=httpx.Response(200))

            result = client.test_connection()

            assert result is True

    @responses.activate
    def test_test_connection_failure(self, sample_config):
        """Test failed connection test"""
        with patch("dataverse_batch.client.DataverseClient._authenticate"):
            client = DataverseClient(**sample_config)
            client.access_token = "test-token"

            # Mock test endpoint with error
            test_url = f"{sample_config['dynamics_url']}/api/data/v9.2"
            responses.add(responses.GET, test_url, status=401)

            result = client.test_connection()

            assert result is False

    def test_parse_batch_response_complex(self, sample_config, sample_account_data):
        """Test complex batch response parsing"""
        with patch("dataverse_batch.client.DataverseClient._authenticate"):
            client = DataverseClient(**sample_config)

            # Create a mock response with mixed results
            mixed_response = """--batch_response
Content-Type: multipart/mixed; boundary=changeset_response

--changeset_response
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: 1

HTTP/1.1 204 No Content
OData-EntityId: https://test.crm.dynamics.com/api/data/v9.2/accounts(11111111-1111-1111-1111-111111111111)

--changeset_response
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: 2

HTTP/1.1 400 Bad Request
Content-Type: application/json

{
    "error": {
        "code": "0x8006088a", 
        "message": "Invalid email format"
    }
}

--changeset_response
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: 3

HTTP/1.1 204 No Content
OData-EntityId: https://test.crm.dynamics.com/api/data/v9.2/accounts(33333333-3333-3333-3333-333333333333)

--changeset_response--
--batch_response--"""

            mock_response = Mock()
            mock_response.text = mixed_response

            results = client._parse_batch_response(
                mock_response, sample_account_data[:3]
            )

            assert len(results) == 3
            assert results[0]["status"] == "success"
            assert results[0]["id"] == "11111111-1111-1111-1111-111111111111"
            assert results[1]["status"] == "error"
            assert "Invalid email format" in results[1]["error"]
            assert results[2]["status"] == "success"
            assert results[2]["id"] == "33333333-3333-3333-3333-333333333333"
