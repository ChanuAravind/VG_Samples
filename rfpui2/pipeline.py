import sagemaker
from sagemaker.workflow.pipeline import Pipeline

from sagemaker.workflow.steps import ProcessingStep
from sagemaker.processing import ProcessingInput, ProcessingOutput, ScriptProcessor
from sagemaker.workflow.parameters import ParameterString
import os

sagemaker_session = sagemaker.session.Session()
role = sagemaker.get_execution_role()

role

import boto3

s3 = boto3.client('s3')

os.getcwd()

import boto3
from botocore.exceptions import ClientError

bucket_name = 'aravind-demo-rfpui2'
region = 'ap-south-1'

# Create an S3 client
s3 = boto3.client('s3', region_name=region)

# Attempt to create the bucket
try:
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={'LocationConstraint': region}
    )
    print(f"✅ Bucket '{bucket_name}' created successfully in region '{region}'.")
except ClientError as e:
    error_code = e.response['Error']['Code']
    if error_code == 'BucketAlreadyOwnedByYou':
        print(f"ℹ️ Bucket '{bucket_name}' already exists and is owned by you.")
    elif error_code == 'BucketAlreadyExists':
        print(f"❌ Bucket name '{bucket_name}' is already taken globally. Try another name.")
    else:
        print(f"❌ Failed to create bucket: {e}")


# s3://aravind-demo-rfpui2/dataset2.csv

dataset1_uri = ParameterString(name="Dataset1Uri", default_value="s3://aravind-demo-rfpui2/dataset1.csv")
dataset2_uri = ParameterString(name="Dataset2Uri", default_value="s3://aravind-demo-rfpui2/dataset2.csv")

script_processor = ScriptProcessor(
    image_uri=sagemaker.image_uris.retrieve(framework='sklearn', region=sagemaker_session.boto_region_name, version='1.0-1'),
    command=["python3"],  # 👈 Add this line
    role=role,
    instance_count=1,
    instance_type='ml.m5.xlarge'
)


step1 = ProcessingStep(
    name="Top10Dataset1",
    processor=script_processor,
    inputs=[ProcessingInput(source=dataset1_uri, destination="/opt/ml/processing/input")],
    outputs=[ProcessingOutput(output_name="dataset1_top10", source="/opt/ml/processing/output")],
    code="scripts/top10_dataset1.py",
)

step2 = ProcessingStep(
    name="Top10Dataset2",
    processor=script_processor,
    inputs=[ProcessingInput(source=dataset2_uri, destination="/opt/ml/processing/input")],
    outputs=[ProcessingOutput(output_name="dataset2_top10", source="/opt/ml/processing/output")],
    code="scripts/top10_dataset2.py",
)

step3 = ProcessingStep(
    name="CombineDatasets",
    processor=script_processor,
    inputs=[
        ProcessingInput(source=step1.properties.ProcessingOutputConfig.Outputs["dataset1_top10"].S3Output.S3Uri,
                        destination="/opt/ml/processing/input1"),
        ProcessingInput(source=step2.properties.ProcessingOutputConfig.Outputs["dataset2_top10"].S3Output.S3Uri,
                        destination="/opt/ml/processing/input2")
    ],
    outputs=[ProcessingOutput(output_name="combined", source="/opt/ml/processing/output")],
    code="scripts/combine_datasets.py",
)


pipeline = Pipeline(
    name="Top10AndCombinePipeline",
    parameters=[dataset1_uri, dataset2_uri],
    steps=[step1, step2, step3],  # Ensure all stepX are valid ProcessingStep objects
    sagemaker_session=sagemaker_session
)



# Upsert and run the pipeline
if __name__ == "__main__":
    pipeline.upsert(role_arn=role)
    execution = pipeline.start()
    print("Pipeline started:", execution.arn)

