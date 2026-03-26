"""
API Gateway and trigger Lambda CDK constructs.

Provisions:
  - Trigger Lambda: backend/orchestration/trigger/handler.py
      Env vars: TABLE_NAME, SFN_ARN
      IAM: DynamoDB PutItem on the Investigations table + sfn:StartExecution

  - API Lambda: backend/orchestration/api/handler.py
      Env vars: TABLE_NAME, S3_BUCKET
      IAM: DynamoDB GetItem on the Investigations table + S3 GetObject on the artifacts bucket

  - REST API:
      POST /investigations                    → trigger Lambda
      GET  /investigations/{incidentId}       → api Lambda
      GET  /investigations/{incidentId}/report → api Lambda
      CORS enabled for frontend origin.

Alarm-driven EventBridge rule is not yet implemented.
The trigger Lambda handler.py has a placeholder comment where that path goes.
"""

import os
import aws_cdk as cdk
from aws_cdk import (
    aws_lambda as lambda_,
    aws_apigateway as apigw,
    aws_cognito as cognito,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_iam as iam,
    Duration,
)
from constructs import Construct

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
_TRIGGER_DIR = os.path.join(
    _REPO_ROOT, "backend", "orchestration", "trigger"
)
_API_DIR = os.path.join(
    _REPO_ROOT, "backend", "orchestration", "api"
)
_CONTEXTS_API_DIR = os.path.join(
    _REPO_ROOT, "backend"
)
_DISCOVERY_API_DIR = os.path.join(
    _REPO_ROOT, "backend"
)

# Exclude test artifacts from Lambda deployment bundles.
_ASSET_EXCLUDES = ["tests", "__pycache__", "*.pyc"]


class ApiConstructs(Construct):
    """CDK constructs for the trigger and read-only investigation API."""

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        table: dynamodb.ITable,
        contexts_table: dynamodb.ITable,
        bucket: s3.IBucket,
        state_machine: sfn.IStateMachine,
    ) -> None:
        super().__init__(scope, id)

        # Configurable CORS origin. Set via CDK context:
        #   cdk deploy -c allowed_cors_origin=https://your-frontend-url
        # Defaults to http://localhost:5173 for local Vite dev server.
        _allowed_origin: str = (
            self.node.try_get_context("allowed_cors_origin") or "http://localhost:5173"
        )
        _callback_urls_raw: str | None = self.node.try_get_context("cognito_callback_urls")
        _logout_urls_raw: str | None = self.node.try_get_context("cognito_logout_urls")
        _callback_urls = (
            [u.strip() for u in _callback_urls_raw.split(",") if u.strip()]
            if _callback_urls_raw
            else [_allowed_origin]
        )
        _logout_urls = (
            [u.strip() for u in _logout_urls_raw.split(",") if u.strip()]
            if _logout_urls_raw
            else [_allowed_origin]
        )

        # ----------------------------------------------------------------
        # Cognito User Pool auth for browser-facing API
        # ----------------------------------------------------------------
        # self_sign_up_enabled=True is intentional for a PoC demo — any visitor can
        # register an account. For a restricted environment, set this to False and
        # provision users manually or via an IdP federation.
        self.user_pool = cognito.UserPool(
            self,
            "IncidentInvestigatorUserPool",
            user_pool_name="incident-investigator-users",
            self_sign_up_enabled=True,
            sign_in_aliases=cognito.SignInAliases(email=True),
            auto_verify=cognito.AutoVerifiedAttrs(email=True),
            password_policy=cognito.PasswordPolicy(
                min_length=12,
                require_digits=True,
                require_lowercase=True,
                require_uppercase=True,
                require_symbols=False,
            ),
            account_recovery=cognito.AccountRecovery.EMAIL_ONLY,
            standard_attributes=cognito.StandardAttributes(
                email=cognito.StandardAttribute(required=True, mutable=False)
            ),
        )

        self.user_pool_client = self.user_pool.add_client(
            "IncidentInvestigatorWebClient",
            user_pool_client_name="incident-investigator-web-client",
            generate_secret=False,
            prevent_user_existence_errors=True,
            auth_flows=cognito.AuthFlow(
                user_srp=True,
                user_password=False,
                admin_user_password=False,
                custom=False,
            ),
            o_auth=cognito.OAuthSettings(
                callback_urls=_callback_urls,
                logout_urls=_logout_urls,
                flows=cognito.OAuthFlows(authorization_code_grant=True),
                scopes=[
                    cognito.OAuthScope.OPENID,
                    cognito.OAuthScope.EMAIL,
                    cognito.OAuthScope.PROFILE,
                ],
            ),
            supported_identity_providers=[
                cognito.UserPoolClientIdentityProvider.COGNITO
            ],
        )

        self.user_pool_domain = self.user_pool.add_domain(
            "IncidentInvestigatorDomain",
            cognito_domain=cognito.CognitoDomainOptions(
                domain_prefix=(
                    self.node.try_get_context("cognito_domain_prefix")
                    or f"incident-investigator-{self.node.addr[-8:].lower()}"
                )
            ),
        )

        self.cognito_authorizer = apigw.CognitoUserPoolsAuthorizer(
            self,
            "ApiCognitoAuthorizer",
            cognito_user_pools=[self.user_pool],
        )

        # ----------------------------------------------------------------
        # Trigger Lambda
        # ----------------------------------------------------------------
        self.trigger_lambda = lambda_.Function(
            self,
            "TriggerLambda",
            function_name="incident-investigator-trigger",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="orchestration.trigger.handler.lambda_handler",
            code=lambda_.Code.from_asset(_CONTEXTS_API_DIR, exclude=_ASSET_EXCLUDES),
            timeout=Duration.seconds(30),
            environment={
                "TABLE_NAME": table.table_name,
                "CONTEXTS_TABLE_NAME": contexts_table.table_name,
                "SFN_ARN": state_machine.state_machine_arn,
                "ALLOWED_ORIGIN": _allowed_origin,
                "TTL_RETENTION_DAYS": str(self.node.try_get_context("ttl_retention_days") or 30),
                "PAYLOAD_WARNING_THRESHOLD_BYTES": str(
                    self.node.try_get_context("payload_warning_threshold_bytes") or (180 * 1024)
                ),
            },
        )

        # Grant only the DynamoDB actions the trigger handler actually uses:
        # put_item (create RUNNING record) and update_item (_mark_investigation_failed).
        table.grant(
            self.trigger_lambda,
            "dynamodb:PutItem",
            "dynamodb:UpdateItem",
            "dynamodb:GetItem",
        )
        contexts_table.grant(self.trigger_lambda, "dynamodb:GetItem")

        # Grant Step Functions start execution
        state_machine.grant_start_execution(self.trigger_lambda)

        # ----------------------------------------------------------------
        # API Lambda (read-only — status + report endpoints)
        # ----------------------------------------------------------------
        self.api_lambda = lambda_.Function(
            self,
            "ApiLambda",
            function_name="incident-investigator-api",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(_API_DIR, exclude=_ASSET_EXCLUDES),
            timeout=Duration.seconds(30),
            environment={
                "TABLE_NAME": table.table_name,
                "S3_BUCKET": bucket.bucket_name,
                "ALLOWED_ORIGIN": _allowed_origin,
            },
        )

        self.contexts_api_lambda = lambda_.Function(
            self,
            "ContextsApiLambda",
            function_name="incident-investigator-contexts-api",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="orchestration.contexts_api.handler.lambda_handler",
            code=lambda_.Code.from_asset(_CONTEXTS_API_DIR, exclude=_ASSET_EXCLUDES),
            timeout=Duration.seconds(30),
            environment={
                "CONTEXTS_TABLE_NAME": contexts_table.table_name,
                "ALLOWED_ORIGIN": _allowed_origin,
            },
        )

        self.discovery_api_lambda = lambda_.Function(
            self,
            "DiscoveryApiLambda",
            function_name="incident-investigator-discovery-api",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="orchestration.discovery_api.handler.lambda_handler",
            code=lambda_.Code.from_asset(_DISCOVERY_API_DIR, exclude=_ASSET_EXCLUDES),
            timeout=Duration.seconds(30),
            environment={
                "ALLOWED_ORIGIN": _allowed_origin,
            },
        )

        # Grant only the DynamoDB actions the API handler actually uses.
        table.grant(self.api_lambda, "dynamodb:GetItem")
        contexts_table.grant(
            self.contexts_api_lambda,
            "dynamodb:GetItem",
            "dynamodb:PutItem",
            "dynamodb:Query",
            "dynamodb:DeleteItem",
        )
        self.api_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[f"{bucket.bucket_arn}/investigations/*/final-report.json"],
            )
        )

        # ----------------------------------------------------------------
        # REST API Gateway
        # ----------------------------------------------------------------
        _cors = apigw.CorsOptions(
            allow_origins=[_allowed_origin],
            allow_methods=["POST", "GET", "PATCH", "DELETE", "OPTIONS"],
            allow_headers=[
                "Content-Type",
                "Authorization",
                "Idempotency-Key",
                "X-Amz-Date",
                "X-Api-Key",
            ],
        )

        self.api = apigw.RestApi(
            self,
            "InvestigationsApi",
            rest_api_name="incident-investigator-api",
            default_cors_preflight_options=_cors,
            deploy_options=apigw.StageOptions(stage_name="prod"),
        )

        # POST /investigations
        investigations_resource = self.api.root.add_resource("investigations")
        investigations_resource.add_method(
            "POST",
            apigw.LambdaIntegration(self.trigger_lambda, proxy=True),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=self.cognito_authorizer,
            authorization_scopes=["openid"],
        )

        # GET /investigations/{incidentId}
        incident_resource = investigations_resource.add_resource("{incidentId}")
        incident_resource.add_method(
            "GET",
            apigw.LambdaIntegration(self.api_lambda, proxy=True),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=self.cognito_authorizer,
            authorization_scopes=["openid"],
        )

        # GET /investigations/{incidentId}/report
        report_resource = incident_resource.add_resource("report")
        report_resource.add_method(
            "GET",
            apigw.LambdaIntegration(self.api_lambda, proxy=True),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=self.cognito_authorizer,
            authorization_scopes=["openid"],
        )

        contexts_resource = self.api.root.add_resource("incident-contexts")
        contexts_resource.add_method(
            "POST",
            apigw.LambdaIntegration(self.contexts_api_lambda, proxy=True),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=self.cognito_authorizer,
            authorization_scopes=["openid"],
        )
        contexts_resource.add_method(
            "GET",
            apigw.LambdaIntegration(self.contexts_api_lambda, proxy=True),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=self.cognito_authorizer,
            authorization_scopes=["openid"],
        )

        context_by_id = contexts_resource.add_resource("{contextId}")
        context_by_id.add_method(
            "GET",
            apigw.LambdaIntegration(self.contexts_api_lambda, proxy=True),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=self.cognito_authorizer,
            authorization_scopes=["openid"],
        )
        context_by_id.add_method(
            "PATCH",
            apigw.LambdaIntegration(self.contexts_api_lambda, proxy=True),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=self.cognito_authorizer,
            authorization_scopes=["openid"],
        )
        context_by_id.add_method(
            "DELETE",
            apigw.LambdaIntegration(self.contexts_api_lambda, proxy=True),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=self.cognito_authorizer,
            authorization_scopes=["openid"],
        )

        discovery_resource = contexts_resource.add_resource("discovery")
        discovery_log_groups = discovery_resource.add_resource("log-groups")
        discovery_log_groups.add_method(
            "GET",
            apigw.LambdaIntegration(self.discovery_api_lambda, proxy=True),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=self.cognito_authorizer,
            authorization_scopes=["openid"],
        )

        discovery_metrics = discovery_resource.add_resource("metrics")
        discovery_metrics.add_method(
            "GET",
            apigw.LambdaIntegration(self.discovery_api_lambda, proxy=True),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=self.cognito_authorizer,
            authorization_scopes=["openid"],
        )

        discovery_metric_namespaces = discovery_metrics.add_resource("namespaces")
        discovery_metric_namespaces.add_method(
            "GET",
            apigw.LambdaIntegration(self.discovery_api_lambda, proxy=True),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=self.cognito_authorizer,
            authorization_scopes=["openid"],
        )

        discovery_xray = discovery_resource.add_resource("xray")
        discovery_xray_services = discovery_xray.add_resource("services")
        discovery_xray_services.add_method(
            "GET",
            apigw.LambdaIntegration(self.discovery_api_lambda, proxy=True),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=self.cognito_authorizer,
            authorization_scopes=["openid"],
        )

        cdk.CfnOutput(
            self,
            "ApiEndpoint",
            value=f"{self.api.url}investigations",
            description="Base URL for /investigations endpoints",
        )
        cdk.CfnOutput(
            self,
            "CognitoUserPoolId",
            value=self.user_pool.user_pool_id,
            description="Cognito User Pool ID for frontend auth configuration",
        )
        cdk.CfnOutput(
            self,
            "CognitoUserPoolClientId",
            value=self.user_pool_client.user_pool_client_id,
            description="Cognito app client ID for frontend auth configuration",
        )
        cdk.CfnOutput(
            self,
            "CognitoHostedUiDomain",
            value=self.user_pool_domain.domain_name,
            description="Cognito Hosted UI domain for browser sign-in",
        )
        cdk.CfnOutput(
            self,
            "CognitoRegion",
            value=cdk.Aws.REGION,
            description="AWS region of the Cognito User Pool",
        )
