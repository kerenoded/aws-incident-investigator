import os
import aws_cdk as cdk
from infra.incident_investigator_stack import IncidentInvestigatorStack

app = cdk.App()

IncidentInvestigatorStack(
    app,
    "IncidentInvestigatorStack",
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION"),
    ),
)

app.synth()
