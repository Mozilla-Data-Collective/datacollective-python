from datacollective import (
    DatasetSubmission,
    License,
    Task,
    create_submission_with_upload,
)

submission = DatasetSubmission(
    name="My Dataset Name",
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
    exclusivityOptOut=True,  # True = dataset is not exclusive to Data Collective (can be found elsewhere),
    # False = dataset is exclusively shared in Mozilla Data Collective
    agreeToSubmit=True,  # True = You confirm that you have the right to submit this dataset and that all information provided in the datasheet is accurate. Required to be true to complete the submission
)

response = create_submission_with_upload(
    file_path="example_dataset.tar.gz",
    submission=submission,
)

print(response)
