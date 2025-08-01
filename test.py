import boto3

# Create an S3 client
s3 = boto3.client('s3')

try:
    # Call the list_buckets method to get a list of all buckets
    response = s3.list_buckets()

    # Iterate through the 'Buckets' key in the response dictionary
    print("Existing S3 buckets:")
    for bucket in response['Buckets']:
        print(f"  - {bucket['Name']} (Created on: {bucket['CreationDate']})")

except Exception as e:
    print(f"Error listing S3 buckets: {e}")