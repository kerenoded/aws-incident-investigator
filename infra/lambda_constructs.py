"""
Lambda CDK construct definitions for the real workflow Lambdas.

Trigger Lambda is defined separately in infra/api/api_constructs.py alongside
the API Gateway construct.
"""

import os
from aws_cdk import (
    aws_lambda as lambda_,
    aws_sqs as sqs,
    Duration,
)
from constructs import Construct

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_BACKEND = os.path.join(_REPO_ROOT, "backend")

# Exclude test artifacts from Lambda deployment bundles.
_ASSET_EXCLUDES = ["tests", "__pycache__", "*.pyc"]

class WorkflowLambdas(Construct):
    """CDK Lambda Function definitions for all real workflow Lambdas.

    Each Lambda runs Python 3.12. Environment variables are kept minimal:
      - S3_BUCKET: passed to Lambdas that write investigation artifacts to S3.
      - No other env vars are set here; the trigger Lambda's vars are in ApiConstructs.
    """

    def __init__(self, scope: Construct, id: str, *, bucket_name: str) -> None:
        super().__init__(scope, id)

        env = {"S3_BUCKET": bucket_name}

        # Shared DLQ for all workflow Lambdas.  Failed invocations land here
        # rather than being silently discarded, enabling CloudWatch alerting.
        self._dlq = sqs.Queue(
            self,
            "WorkflowLambdaDLQ",
            queue_name="incident-investigator-lambda-dlq",
            retention_period=Duration.days(14),
        )
        # Expose DLQ to parent stack for operational alarms.
        self.dlq = self._dlq

        # ----------------------------------------------------------------
        # Scope Builder
        # Bundled from its own directory. Scope derivation is context-snapshot
        # based and no longer depends on repo-managed service config assets.
        # ----------------------------------------------------------------
        self.scope_builder = lambda_.Function(
            self,
            "ScopeBuilder",
            function_name="incident-investigator-scope-builder",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(
                os.path.join(_BACKEND, "orchestration", "scope_builder"),
                exclude=_ASSET_EXCLUDES,
            ),
            timeout=Duration.seconds(30),
            memory_size=256,
            tracing=lambda_.Tracing.ACTIVE,
            dead_letter_queue=self._dlq,
            environment=env,
        )

        # ----------------------------------------------------------------
        # Evidence workers
        # Bundled from backend/ root so shared/ is on the module path.
        # ----------------------------------------------------------------
        self.metrics_worker = lambda_.Function(
            self,
            "MetricsWorker",
            function_name="incident-investigator-metrics-worker",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="workers.metrics.handler.lambda_handler",
            code=lambda_.Code.from_asset(
                _BACKEND,
                exclude=_ASSET_EXCLUDES,
            ),
            timeout=Duration.seconds(60),
            memory_size=256,
            tracing=lambda_.Tracing.ACTIVE,
            dead_letter_queue=self._dlq,
            environment=env,
        )

        self.logs_worker = lambda_.Function(
            self,
            "LogsWorker",
            function_name="incident-investigator-logs-worker",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="workers.logs.handler.lambda_handler",
            code=lambda_.Code.from_asset(
                _BACKEND,
                exclude=_ASSET_EXCLUDES,
            ),
            timeout=Duration.seconds(120),
            memory_size=256,
            tracing=lambda_.Tracing.ACTIVE,
            dead_letter_queue=self._dlq,
            environment=env,
        )

        self.traces_worker = lambda_.Function(
            self,
            "TracesWorker",
            function_name="incident-investigator-traces-worker",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="workers.traces.handler.lambda_handler",
            code=lambda_.Code.from_asset(
                _BACKEND,
                exclude=_ASSET_EXCLUDES,
            ),
            timeout=Duration.seconds(60),
            memory_size=256,
            tracing=lambda_.Tracing.ACTIVE,
            dead_letter_queue=self._dlq,
            environment=env,
        )

        # ----------------------------------------------------------------
        # Orchestration: Candidate Hypotheses Builder
        # ----------------------------------------------------------------
        self.hypotheses_builder = lambda_.Function(
            self,
            "HypothesesBuilder",
            function_name="incident-investigator-hypotheses-builder",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(
                os.path.join(
                    _BACKEND, "orchestration", "candidate_hypotheses_builder"
                ),
                exclude=_ASSET_EXCLUDES,
            ),
            timeout=Duration.seconds(30),
            memory_size=256,
            tracing=lambda_.Tracing.ACTIVE,
            dead_letter_queue=self._dlq,
            environment=env,
        )

        # ----------------------------------------------------------------
        # Orchestration: Final Report Builder
        # ----------------------------------------------------------------
        self.final_report_builder = lambda_.Function(
            self,
            "FinalReportBuilder",
            function_name="incident-investigator-final-report-builder",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(
                os.path.join(_BACKEND, "orchestration", "final_report_builder"),
                exclude=_ASSET_EXCLUDES,
            ),
            timeout=Duration.seconds(30),
            memory_size=256,
            tracing=lambda_.Tracing.ACTIVE,
            dead_letter_queue=self._dlq,
            environment=env,
        )

        # ----------------------------------------------------------------
        # AI: Hypothesis Evaluator
        # Higher memory for Bedrock SDK overhead and JSON parsing.
        # ----------------------------------------------------------------
        self.ai_hypothesis_evaluator = lambda_.Function(
            self,
            "AIHypothesisEvaluator",
            function_name="incident-investigator-ai-hypothesis-evaluator",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(
                os.path.join(_BACKEND, "ai", "hypothesis_evaluator"),
                exclude=_ASSET_EXCLUDES,
            ),
            timeout=Duration.seconds(60),
            memory_size=512,
            tracing=lambda_.Tracing.ACTIVE,
            dead_letter_queue=self._dlq,
            environment={
                **env,
                "BEDROCK_MODEL_ID": "eu.amazon.nova-micro-v1:0",
            },
        )
