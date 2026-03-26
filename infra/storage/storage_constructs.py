"""
Storage constructs for the incident investigator.

Provisions:
  - DynamoDB table: Investigations
      PK = INCIDENT#<incidentId>  (String)
      SK = META | REPORT          (String)
      Billing: PAY_PER_REQUEST
      TTL attribute: ttl (optional cleanup)

  - S3 bucket: incident-investigator-artifacts
      Stores large payloads (scope, worker evidence, hypotheses, final report).
      See docs/STORAGE_DESIGN.md for the key convention.

Removal policy:
  DESTROY when CDK context key "dev" == "true" (default for local/CI use).
  RETAIN otherwise — production deployments must not silently delete audit artifacts.
  Deploy with: cdk deploy -c dev=false   (production)
               cdk deploy -c dev=true    (dev / CI)
"""

import aws_cdk as cdk
from aws_cdk import (
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    RemovalPolicy,
)
from constructs import Construct


class StorageConstructs(Construct):
    def __init__(self, scope: Construct, id: str) -> None:
        super().__init__(scope, id)

        # Use DESTROY only in dev/CI environments. Production keeps artifacts.
        _is_dev = self.node.try_get_context("dev") == "true"
        _removal = RemovalPolicy.DESTROY if _is_dev else RemovalPolicy.RETAIN

        # Table names are hardcoded for PoC simplicity.
        # This limits the stack to one deployment per account/region.
        self.table = dynamodb.Table(
            self,
            "InvestigationsTable",
            table_name="Investigations",
            partition_key=dynamodb.Attribute(
                name="PK", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="SK", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=_removal,
            time_to_live_attribute="ttl",
        )

        self.contexts_table = dynamodb.Table(
            self,
            "IncidentContextsTable",
            table_name="IncidentContexts",
            partition_key=dynamodb.Attribute(
                name="PK", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="SK", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=_removal,
        )

        self.contexts_table.add_global_secondary_index(
            index_name="OwnerUpdatedAtIndex",
            partition_key=dynamodb.Attribute(
                name="ownerSub", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="updatedAt", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # Bucket name is auto-generated to avoid global naming conflicts.
        # The name is exported as an output from the stack.
        self.bucket = s3.Bucket(
            self,
            "ArtifactsBucket",
            removal_policy=_removal,
            auto_delete_objects=_is_dev,  # only safe when removal_policy is DESTROY
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
        )

        # Outputs — useful for manual testing and CI
        cdk.CfnOutput(
            self,
            "InvestigationsTableName",
            value=self.table.table_name,
            description="DynamoDB table for incident metadata and status",
        )
        cdk.CfnOutput(
            self,
            "ArtifactsBucketName",
            value=self.bucket.bucket_name,
            description="S3 bucket for investigation artifacts (scope, evidence, reports)",
        )
        cdk.CfnOutput(
            self,
            "IncidentContextsTableName",
            value=self.contexts_table.table_name,
            description="DynamoDB table for incident context metadata",
        )
