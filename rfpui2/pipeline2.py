import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.processing import ProcessingInput, ProcessingOutput, ScriptProcessor
from sagemaker.workflow.parameters import ParameterString

# Setup
sagemaker_session = sagemaker.session.Session()
role = sagemaker.get_execution_role()

# Parameters for input datasets and combined output location
dataset1_uri = ParameterString(
    name="Dataset1Uri",
    default_value="s3://aravind-demo-rfpui2/dataset1.csv"
)

dataset2_uri = ParameterString(
    name="Dataset2Uri",
    default_value="s3://aravind-demo-rfpui2/dataset2.csv"
)

combined_output_uri = ParameterString(
    name="CombinedOutputUri",
    default_value="s3://aravind-demo-rfpui2/combined-output/"
)

# ScriptProcessor (sklearn image)
script_processor = ScriptProcessor(
    image_uri=sagemaker.image_uris.retrieve(
        framework='sklearn',
        region=sagemaker_session.boto_region_name,
        version='1.0-1'
    ),
    command=["python3"],
    role=role,
    instance_count=1,
    instance_type='ml.m5.xlarge',
    sagemaker_session=sagemaker_session,
)

# Step 1 - top 10 rows from dataset1
step1 = ProcessingStep(
    name="Top10Dataset1",
    processor=script_processor,
    inputs=[ProcessingInput(source=dataset1_uri, destination="/opt/ml/processing/input")],
    outputs=[ProcessingOutput(output_name="dataset1_top10", source="/opt/ml/processing/output",destination='s3://aravind-demo-rfpui2/intermittent/dataset1.csv')],
    code="scripts/top10_dataset1.py",
)

# Step 2 - top 10 rows from dataset2
step2 = ProcessingStep(
    name="Top10Dataset2",
    processor=script_processor,
    inputs=[ProcessingInput(source=dataset2_uri, destination="/opt/ml/processing/input")],
    outputs=[ProcessingOutput(output_name="dataset2_top10", source="/opt/ml/processing/output")],
    code="scripts/top10_dataset2.py",
)

# Step 3 - combine outputs from step1 and step2, save to specified S3 location
step3 = ProcessingStep(
    name="CombineDatasets",
    processor=script_processor,
    inputs=[
        ProcessingInput(
            source=step1.properties.ProcessingOutputConfig.Outputs["dataset1_top10"].S3Output.S3Uri,
            destination="/opt/ml/processing/input1"
        ),
        ProcessingInput(
            source=step2.properties.ProcessingOutputConfig.Outputs["dataset2_top10"].S3Output.S3Uri,
            destination="/opt/ml/processing/input2"
        )
    ],
    outputs=[
        ProcessingOutput(
            output_name="combined",
            source="/opt/ml/processing/output",
            destination=combined_output_uri  # save combined output here
        )
    ],
    code="scripts/combine_datasets.py",
    depends_on=[step1, step2]
)

# Define pipeline
pipeline = Pipeline(
    name="Top10AndCombinePipeline",
    parameters=[dataset1_uri, dataset2_uri, combined_output_uri],
    # steps=[step1, step2, step3],
    steps=[step3],
    sagemaker_session=sagemaker_session,
)

if __name__ == "__main__":
    pipeline.upsert(role_arn=role)
    execution = pipeline.start()
    print(f"Pipeline started with execution ARN: {execution.arn}")
