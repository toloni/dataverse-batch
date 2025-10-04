import logging
import httpx
import uuid
import time
from typing import Dict, Any, List, Optional, Tuple
import json

logger = logging.getLogger(__name__)

class DataverseClient:
    def __init__(self, dynamics_url: str, client_id: str, client_secret: str, tenant_id: str):
        self.dynamics_url = dynamics_url.rstrip('/')
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.access_token = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with Dataverse using OAuth"""
        try:
            auth_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            
            auth_data = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'scope': f'{self.dynamics_url}/.default',
                'grant_type': 'client_credentials'
            }
            
            logger.info("Starting authentication with Dataverse...")
            
            # Using httpx with verify=False
            with httpx.Client(verify=False) as client:
                response = client.post(auth_url, data=auth_data)
                response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data['access_token']
            logger.info("Authentication successful!")
            
        except httpx.RequestError as e:
            logger.error(f"Connection error during authentication: {str(e)}")
            raise
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error during authentication: {e.response.status_code}")
            raise
        except KeyError:
            logger.error("Access token not found in response")
            raise
    
    def _get_headers(self) -> Dict[str, str]:
        """Return headers for API requests"""
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'OData-MaxVersion': '4.0',
            'OData-Version': '4.0',
            'Prefer': 'odata.continue-on-error'
        }
    
    def _create_batch_payload(self, table_name: str, records: List[Dict[str, Any]]) -> Tuple[str, Dict[str, str]]:
        """Create payload for $batch request"""
        batch_id = f"batch_{uuid.uuid4().hex}"
        changeset_id = f"changeset_{uuid.uuid4().hex}"
        
        batch_content = []
        batch_content.append(f"--{batch_id}")
        batch_content.append(f"Content-Type: multipart/mixed; boundary={changeset_id}")
        batch_content.append("")
        
        # Add each operation to the changeset
        for i, record in enumerate(records):
            operation_id = f"{changeset_id}_{i+1}"
            batch_content.append(f"--{changeset_id}")
            batch_content.append("Content-Type: application/http")
            batch_content.append("Content-Transfer-Encoding: binary")
            batch_content.append("Content-ID: " + str(i + 1))
            batch_content.append("")
            
            # Individual request body
            url = f"{self.dynamics_url}/api/data/v9.2/{table_name}"
            batch_content.append(f"POST {url} HTTP/1.1")
            batch_content.append("Content-Type: application/json;type=entry")
            batch_content.append("")
            batch_content.append(json.dumps(record))
            batch_content.append("")
        
        batch_content.append(f"--{changeset_id}--")
        batch_content.append(f"--{batch_id}--")
        
        payload = "\r\n".join(batch_content)
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': f'multipart/mixed; boundary={batch_id}',
            'OData-Version': '4.0',
            'Accept': 'application/json'
        }
        
        return payload, headers
    
    def create_records_batch(self, table_name: str, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create multiple records using $batch API"""
        if not records:
            return []
        
        try:
            logger.info(f"Sending batch with {len(records)} records to table {table_name} via $batch API")
            
            # Create batch payload
            payload, headers = self._create_batch_payload(table_name, records)
            
            # Batch API URL
            batch_url = f"{self.dynamics_url}/api/data/v9.2/$batch"
            
            # Using httpx with verify=False
            with httpx.Client(verify=False, timeout=30.0) as client:
                response = client.post(batch_url, content=payload, headers=headers)
                response.raise_for_status()
            
            # Process batch response
            results = self._parse_batch_response(response, records)
            
            success_count = len([r for r in results if r.get('status') == 'success'])
            logger.info(f"Batch processed: {success_count}/{len(records)} successes")
            
            return results
            
        except httpx.RequestError as e:
            logger.error(f"Connection error in batch: {str(e)}")
            return [{'error': str(e), 'status': 'error'} for _ in records]
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error in batch: {e.response.status_code} - {e.response.text}")
            return [{'error': f"HTTP {e.response.status_code}", 'status': 'error'} for _ in records]
        except Exception as e:
            logger.error(f"Unexpected error in batch: {str(e)}")
            return [{'error': str(e), 'status': 'error'} for _ in records]
    
    def _parse_batch_response(self, response: httpx.Response, original_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse $batch API response"""
        results = []
        response_text = response.text
        
        # Split response into parts
        parts = response_text.split('--changeset_')
        
        for i, record in enumerate(original_records):
            result = {
                'record_index': i,
                'status': 'unknown',
                'data': record
            }
            
            # Try to find the part corresponding to this record
            part_found = False
            for part in parts:
                if f"Content-ID: {i + 1}" in part:
                    part_found = True
                    
                    # Check HTTP status
                    if "HTTP/1.1 204 No Content" in part or "HTTP/1.1 201 Created" in part:
                        result['status'] = 'success'
                        
                        # Extract created record ID
                        if "OData-EntityId:" in part:
                            entity_id_line = [line for line in part.split('\r\n') if 'OData-EntityId:' in line]
                            if entity_id_line:
                                entity_id = entity_id_line[0].split('(')[-1].split(')')[0]
                                result['id'] = entity_id
                    
                    elif "HTTP/1.1 4" in part or "HTTP/1.1 5" in part:
                        result['status'] = 'error'
                        # Extract error message if available
                        if "{" in part and "}" in part:
                            try:
                                json_start = part.find('{')
                                json_end = part.rfind('}') + 1
                                error_json = part[json_start:json_end]
                                error_data = json.loads(error_json)
                                result['error'] = error_data.get('error', {}).get('message', 'Unknown error')
                            except:
                                result['error'] = 'Error creating record'
                    
                    break
            
            if not part_found:
                result['status'] = 'error'
                result['error'] = 'Response not found in batch'
            
            results.append(result)
        
        return results
    
    def test_connection(self) -> bool:
        """Test connection to Dataverse"""
        try:
            test_url = f"{self.dynamics_url}/api/data/v9.2"
            headers = self._get_headers()
            
            with httpx.Client(verify=False) as client:
                response = client.get(test_url, headers=headers)
                response.raise_for_status()
            
            logger.info("Dataverse connection test successful!")
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False