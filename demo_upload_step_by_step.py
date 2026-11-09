from datacollective import (
    DatasetSubmission,
    License,
    Task,
    Visibility,
    submit_submission,
    delete_submission,
    create_submission_draft,
    update_submission,
    upload_dataset_file,
    upload_sample_file,
)

# Step 1: Create a draft, placeholder submission with only the name for now.
init_draft = DatasetSubmission(name="Dataset Name")
draft = create_submission_draft(init_draft)

submission_id = draft["submission"]["id"]
print(f"Created draft submission: {submission_id}")

# Step 2: At a second stage, we can update each field.
update_fields = DatasetSubmission(
    longDescription="A detailed description of the dataset.",
    shortDescription="A brief description of the dataset.",
    locale="en-US",
    task=Task.ASR,
    format="TSV",
    licenseAbbreviation=License.CC_BY_4_0,
    other="This text should provide a detailed description of the dataset, "
    "including its contents, structure, and any relevant information "
    "that would help users understand what the dataset is about "
    "and how it can be used.",
    restrictions="Any restrictions you want to impose on the dataset",
    forbiddenUsage="Use cases that are not allowed with this dataset",
    additionalConditions="Any additional conditions for using the dataset",
    pointOfContactFullName="Jane Doe",
    pointOfContactEmail="jane@example.com",
    fundedByFullName="Funder Name",
    fundedByEmail="funder@example.com",
    legalContactFullName="Legal Name",
    legalContactEmail="legal@example.com",
    createdByFullName="Creator Name",
    createdByEmail="creator@example.com",
    intendedUsage="Describe the intended usage of the dataset, including "
    "potential applications and use cases.",
    ethicalReviewProcess="Describe the ethical review process that was "
    "followed for this dataset, including any approvals "
    "or considerations related to data collection and usage.",
    showContactInfo=False,  # Whether to publicly display the contact information above
    visibility=Visibility.PUBLIC,  # public | private | restricted
    isPaid=False,  # If True then compensated dataset and requires basePriceCents, If False (default) = free dataset
    # basePriceCents=100_000,  # Required if isPaid=True. Price in USD cents (in this example: $1,000.00).
    exclusivityOptOut=True,  # True = dataset is not exclusive to Data Collective (can be found elsewhere),
    # False = dataset is exclusively shared in Mozilla Data Collective
    agreeToSubmit=True,  # True = You confirm that you have the right to submit this dataset and that all information provided in the datasheet is accurate. Required to be true to complete the submission
)
response = update_submission(
    submission=update_fields,
    submission_id=submission_id,
)

print(f"Metadata updated: {response}")

# Step 3: We can now upload the file we want to our submission

upload_state = upload_dataset_file(
    file_path="example_dataset.tar.gz",
    submission_id=submission_id,
)

print(f"Upload complete! File Upload ID: {upload_state.fileUploadId}")

# (Optionally) Upload a sample file
sample_state = upload_sample_file(
    file_path="example_dataset.tar.gz",
    submission_id=submission_id,
)

print(f"Sample upload complete! File Upload ID: {sample_state.fileUploadId}")

# Step 4: Once all the fields are filled and the file(s) uploaded, the submission is ready
# to be submitted for review!

response = submit_submission(
    submission_id=submission_id,
    submission=DatasetSubmission(agreeToSubmit=True),
)

print(f"Submission status: {response['submission']['status']}")

# You can also completely remove your submission with the command below

delete_submission(submission_id, enable_logging=True)
