import boto3

S3_BUCKET = 'imad-pyspark-test'
S3_KEY = 'scripts/main.py'
S3_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_KEY)
INSTANCE_TYPE = 'm4.large'
INSTANCE_COUNT = 3

# Upload pyspark script to S3 bucket
s3 = boto3.resource('s3')
s3.meta.client.upload_file("main.py", S3_BUCKET, S3_KEY)

# Run job flow
client = boto3.client('emr', region_name='us-east-2')
response = client.run_job_flow(
    Name="PySpark",
    ReleaseLabel='emr-6.3.0',
    LogUri = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key='emr-logs'),
    # CustomAmiId  =  'string', # https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-custom-ami.html
    Instances={
        'MasterInstanceType': INSTANCE_TYPE,
        'SlaveInstanceType': INSTANCE_TYPE,
        'InstanceCount': INSTANCE_COUNT,
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    Applications=[
        {
            'Name': 'Spark'
        }
    ],
    Steps=[
    {
        'Name': 'Copy Files',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', S3_URI, '/home/hadoop/']
        }
    },
    {
        'Name': 'Run Spark',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/main.py']
        }
    }
    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole'
)

print(response)
