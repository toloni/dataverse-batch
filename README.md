# dataverse-batch
Python library for batch processing in Microsoft Dataverse.


## Usage Example

```python
from dataverse_batch import DataverseBatch

# Configuration
dataverse_batch = DataverseBatch(
    dynamics_url='https://my-org.crm.dynamics.com',
    client_id='12345678-1234-1234-1234-123456789012',
    client_secret='my-client-secret',
    tenant_id='12345678-1234-1234-1234-123456789012',
    log_level='INFO'
)

# Generate sample data
sample_data = [
    {'name': f'Account {i}', 'emailaddress1': f'account{i}@example.com'} 
    for i in range(1000)
]

# Get recommendations
recommendations = dataverse_batch.get_batch_recommendations(len(sample_data))

# Execute processing
result = dataverse_batch.create_multiple(
    data=sample_data,
    table='accounts',
    batch_size=recommendations['batch_size'],
    parallel=recommendations['parallel'],
    workers=recommendations['workers']
)

# Analyze results
print(f"Total processed: {len(result)}")
print(f"Successes: {len(result[result['status'] == 'success'])}")
print(f"Errors: {len(result[result['status'] == 'error'])}")

# Save results
result.to_csv('processing_results.csv', index=False)
