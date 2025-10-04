import pytest
import pandas as pd
from typing import List, Dict, Any
import os

@pytest.fixture
def sample_config():
    return {
        'dynamics_url': 'https://test.crm.dynamics.com',
        'client_id': 'test-client-id',
        'client_secret': 'test-client-secret', 
        'tenant_id': 'test-tenant-id'
    }

@pytest.fixture
def sample_account_data() -> List[Dict[str, Any]]:
    return [
        {
            'name': 'Test Company 1',
            'emailaddress1': 'test1@company.com',
            'telephone1': '+1-555-0001',
            'revenue': 1000000.00
        },
        {
            'name': 'Test Company 2', 
            'emailaddress1': 'test2@company.com',
            'telephone1': '+1-555-0002',
            'revenue': 2000000.00
        },
        {
            'name': 'Test Company 3',
            'emailaddress1': 'test3@company.com', 
            'telephone1': '+1-555-0003',
            'revenue': 3000000.00
        }
    ]

@pytest.fixture
def sample_batch_response_success():
    return """--batch_response
Content-Type: multipart/mixed; boundary=changeset_response

--changeset_response
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: 1

HTTP/1.1 204 No Content
OData-EntityId: https://test.crm.dynamics.com/api/data/v9.2/accounts(12345678-1234-1234-1234-123456789012)
OData-Version: 4.0

--changeset_response
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: 2

HTTP/1.1 204 No Content
OData-EntityId: https://test.crm.dynamics.com/api/data/v9.2/accounts(22345678-1234-1234-1234-123456789012)
OData-Version: 4.0

--changeset_response--
--batch_response--"""

@pytest.fixture
def sample_batch_response_with_errors():
    return """--batch_response
Content-Type: multipart/mixed; boundary=changeset_response

--changeset_response
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: 1

HTTP/1.1 204 No Content
OData-EntityId: https://test.crm.dynamics.com/api/data/v9.2/accounts(12345678-1234-1234-1234-123456789012)
OData-Version: 4.0

--changeset_response
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: 2

HTTP/1.1 400 Bad Request
Content-Type: application/json

{
    "error": {
        "code": "0x8006088a",
        "message": "The 'name' attribute is required and cannot be empty"
    }
}

--changeset_response--
--batch_response--"""

@pytest.fixture
def mock_authentication_response():
    return {
        "token_type": "Bearer",
        "expires_in": 3600,
        "ext_expires_in": 3600,
        "access_token": "mock-access-token-12345"
    }