"""
Main CDK stack for the Incident Investigator.

Provisioning order:
  1. StorageConstructs  — DynamoDB table + S3 bucket
  2. WorkflowLambdas    — workflow Lambda functions
  3. InvestigationWorkflow — Step Functions state machine
  4. ApiConstructs      — Trigger Lambda + API Gateway

IAM grants are applied here after all constructs are created to make
the dependency graph explicit.
"""

import aws_cdk as cdk
from aws_cdk import aws_iam as iam
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cloudwatch_actions
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subscriptions
from constructs import Construct

from infra.storage.storage_constructs import StorageConstructs
from infra.lambda_constructs import WorkflowLambdas
from infra.stepfunctions.workflow import InvestigationWorkflow
from infra.api.api_constructs import ApiConstructs


class IncidentInvestigatorStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ----------------------------------------------------------------
        # 1. Storage
        # ----------------------------------------------------------------
        storage = StorageConstructs(self, "Storage")

        # ----------------------------------------------------------------
        # 2. Workflow Lambdas
        # ----------------------------------------------------------------
        lambdas = WorkflowLambdas(
            self,
            "Lambdas",
            bucket_name=storage.bucket.bucket_name,
        )

        # S3 write grants for Lambdas that persist artifacts
        storage.bucket.grant_put(lambdas.scope_builder)
        storage.bucket.grant_put(lambdas.hypotheses_builder)
        storage.bucket.grant_put(lambdas.final_report_builder)

        # Minimal IAM for worker Lambdas to call their respective AWS APIs.
        # Scoped to all resources for MVP; restrict to specific ARNs post-demo.
        lambdas.metrics_worker.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "cloudwatch:GetMetricStatistics",
                    "cloudwatch:ListMetrics",
                ],
                resources=["*"],
            )
        )
        lambdas.logs_worker.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:StartQuery",
                    "logs:GetQueryResults",
                ],
                resources=["*"],
            )
        )
        lambdas.traces_worker.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "xray:GetTraceSummaries",
                    "xray:BatchGetTraces",
                ],
                resources=["*"],
            )
        )

        # Bedrock InvokeModel permission scoped to the specific model used by the evaluator.
        # Model ID must match BEDROCK_MODEL_ID in lambda_constructs.py.
        lambdas.ai_hypothesis_evaluator.add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel"],
                resources=[
                    f"arn:aws:bedrock:{self.region}:{self.account}:inference-profile/eu.amazon.nova-micro-v1:0",
                    "arn:aws:bedrock:*::foundation-model/amazon.nova-micro-v1:0",
                ],
            )
        )

        # ----------------------------------------------------------------
        # 3. Step Functions workflow
        # DynamoDB grants for SaveCompletedStatus/SaveFailedStatus are applied
        # inside InvestigationWorkflow (table.grant_write_data(state_machine.role)).
        # ----------------------------------------------------------------
        workflow = InvestigationWorkflow(
            self,
            "Workflow",
            lambdas=lambdas,
            table=storage.table,
        )

        # Grant the state machine permission to invoke the workflow Lambdas.
        # LambdaInvoke tasks handle this automatically via CDK's IAM inference,
        # but an explicit grant is added here as a safety net.
        for fn in (
            lambdas.scope_builder,
            lambdas.metrics_worker,
            lambdas.logs_worker,
            lambdas.traces_worker,
            lambdas.hypotheses_builder,
            lambdas.ai_hypothesis_evaluator,
            lambdas.final_report_builder,
        ):
            fn.grant_invoke(workflow.state_machine.role)

        # ----------------------------------------------------------------
        # 4. API Gateway + Trigger Lambda + Read API Lambda
        # ----------------------------------------------------------------
        api = ApiConstructs(
            self,
            "Api",
            table=storage.table,
            contexts_table=storage.contexts_table,
            bucket=storage.bucket,
            state_machine=workflow.state_machine,
        )

        api.discovery_api_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:DescribeLogGroups",
                    "cloudwatch:ListMetrics",
                    "xray:GetTraceSummaries",
                ],
                resources=["*"],
            )
        )

        alarm_topic = sns.Topic(
            self,
            "OperationalAlertsTopic",
            topic_name="incident-investigator-operational-alerts",
        )

        alert_email = self.node.try_get_context("alerts_email")
        if isinstance(alert_email, str) and alert_email.strip():
            alarm_topic.add_subscription(
                sns_subscriptions.EmailSubscription(alert_email.strip())
            )

        alarm_action = cloudwatch_actions.SnsAction(alarm_topic)

        # ----------------------------------------------------------------
        # 5. Operational alarms (no deployment actions attached in-code)
        # ----------------------------------------------------------------
        workflow_failed_alarm = cloudwatch.Alarm(
            self,
            "WorkflowExecutionsFailedAlarm",
            alarm_name="incident-investigator-workflow-failures",
            alarm_description="Alarm when at least one investigation workflow execution fails in 5 minutes.",
            metric=workflow.state_machine.metric_failed(
                period=cdk.Duration.minutes(5),
                statistic="sum",
            ),
            threshold=1,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )
        workflow_failed_alarm.add_alarm_action(alarm_action)

        api_5xx_alarm = cloudwatch.Alarm(
            self,
            "ApiServerErrorsAlarm",
            alarm_name="incident-investigator-api-5xx",
            alarm_description="Alarm when API Gateway reports at least one 5XX in 5 minutes.",
            metric=api.api.metric_server_error(
                period=cdk.Duration.minutes(5),
                statistic="sum",
            ),
            threshold=1,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )
        api_5xx_alarm.add_alarm_action(alarm_action)

        dlq_backlog_alarm = cloudwatch.Alarm(
            self,
            "WorkflowLambdaDlqBacklogAlarm",
            alarm_name="incident-investigator-lambda-dlq-backlog",
            alarm_description="Alarm when workflow Lambda DLQ has visible messages.",
            metric=lambdas.dlq.metric_approximate_number_of_messages_visible(
                period=cdk.Duration.minutes(5),
                statistic="max",
            ),
            threshold=1,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )
        dlq_backlog_alarm.add_alarm_action(alarm_action)

        # ----------------------------------------------------------------
        # 6. Basic cost guardrails (small, low-complexity)
        # Billing metrics are emitted in us-east-1 only. Create these alarms
        # only when stack region is us-east-1 to keep synth portable.
        # ----------------------------------------------------------------
        if self.region == "us-east-1":
            bedrock_cost_alarm = cloudwatch.Alarm(
                self,
                "BedrockEstimatedChargesAlarm",
                alarm_name="incident-investigator-cost-bedrock",
                alarm_description="Alarm when estimated Bedrock charges exceed configured threshold (USD).",
                metric=cloudwatch.Metric(
                    namespace="AWS/Billing",
                    metric_name="EstimatedCharges",
                    dimensions_map={
                        "Currency": "USD",
                        "ServiceName": "Amazon Bedrock",
                    },
                    statistic="Maximum",
                    period=cdk.Duration.hours(6),
                ),
                threshold=float(self.node.try_get_context("cost_alarm_bedrock_usd") or 10),
                evaluation_periods=1,
                datapoints_to_alarm=1,
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            )
            bedrock_cost_alarm.add_alarm_action(alarm_action)

            cloudwatch_cost_alarm = cloudwatch.Alarm(
                self,
                "CloudWatchEstimatedChargesAlarm",
                alarm_name="incident-investigator-cost-cloudwatch",
                alarm_description="Alarm when estimated CloudWatch charges exceed configured threshold (USD).",
                metric=cloudwatch.Metric(
                    namespace="AWS/Billing",
                    metric_name="EstimatedCharges",
                    dimensions_map={
                        "Currency": "USD",
                        "ServiceName": "AmazonCloudWatch",
                    },
                    statistic="Maximum",
                    period=cdk.Duration.hours(6),
                ),
                threshold=float(self.node.try_get_context("cost_alarm_cloudwatch_usd") or 10),
                evaluation_periods=1,
                datapoints_to_alarm=1,
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            )
            cloudwatch_cost_alarm.add_alarm_action(alarm_action)

            stepfunctions_cost_alarm = cloudwatch.Alarm(
                self,
                "StepFunctionsEstimatedChargesAlarm",
                alarm_name="incident-investigator-cost-stepfunctions",
                alarm_description="Alarm when estimated Step Functions charges exceed configured threshold (USD).",
                metric=cloudwatch.Metric(
                    namespace="AWS/Billing",
                    metric_name="EstimatedCharges",
                    dimensions_map={
                        "Currency": "USD",
                        "ServiceName": "AWS Step Functions",
                    },
                    statistic="Maximum",
                    period=cdk.Duration.hours(6),
                ),
                threshold=float(self.node.try_get_context("cost_alarm_stepfunctions_usd") or 5),
                evaluation_periods=1,
                datapoints_to_alarm=1,
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            )
            stepfunctions_cost_alarm.add_alarm_action(alarm_action)
