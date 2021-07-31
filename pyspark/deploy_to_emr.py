import boto3

S3_BUCKET = 'imad-pyspark-test'
S3_KEY = 'scripts/main.py'
S3_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_KEY)
INSTANCE_TYPE = 'm4.large'
INSTANCE_COUNT = 3

# Upload pyspark and bootstrap script to S3 bucket
s3 = boto3.resource('s3')
s3.meta.client.upload_file("main.py", S3_BUCKET, S3_KEY)
s3.meta.client.upload_file("pip_install.sh", S3_BUCKET, 'scripts/bootstrap/pip_install.sh')

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
    BootstrapActions=[
        {
            'Name': 'Python Dependencies',
            'ScriptBootstrapAction' : {
                'Path': 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key='scripts/bootstrap/pip_install.sh')
            }
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
