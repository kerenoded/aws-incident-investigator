"""
Step Functions investigation workflow CDK construct.

State machine flow:
  ScopeBuilder (Lambda, critical)
    → GatherEvidence (Parallel — 3 branches)
        Branch 0: MetricsWorker (Lambda + catch → FallbackMetrics Pass)
        Branch 1: LogsWorker    (Lambda + catch → FallbackLogs Pass)
        Branch 2: TracesWorker  (Lambda + catch → FallbackTraces Pass)
    → AggregateWorkerOutputs (Pass — reshapes array[0..2] → keyed dict)
    → CandidateHypothesesBuilder (Lambda, critical)
    → AIHypothesisEvaluator (Lambda → Bedrock)
    → FinalReportBuilder (Lambda, critical)
    → SaveCompletedStatus (DynamoDB SDK — sets COMPLETED + summary + s3ReportKey)
    → InvestigationComplete (Succeed)

Critical step failures (ScopeBuilder, CandidateHypothesesBuilder, FinalReportBuilder):
  → SaveFailedStatus (DynamoDB SDK — sets FAILED)
  → InvestigationFailed (Fail)

Worker failures (non-critical, ADR-009):
  Each parallel branch has its own Catch → Fallback<Source> Pass state.
  Fallback returns the standard empty worker envelope so execution continues.

Parallel branch order is fixed as: [0]=metrics, [1]=logs, [2]=traces.
This order is relied upon by AggregateWorkerOutputs.

Data flow (full payloads in state — no S3 keys passed between stages):
  Execution input      = incident payload (schemas/incident.schema.json)
  After ScopeBuilder   = { ...incident, scope: { incidentId, service, region, ... } }
  After Aggregate      = { incidentId, scope, workerOutputs: { metrics, logs, traces } }
  After Hypotheses     = { ...prev, hypotheses: { incidentId, hypotheses: [...] } }
  After AI eval        = { ...prev, aiEvaluation: {} }
  FinalReportBuilder in = { scope, workerOutputs, hypotheses, aiEvaluation }
                          (matches final_report_builder/handler.py event contract exactly)
  After FinalReport    = { ...prev, reportOutput: { incidentId, summary, topHypotheses, ... } }
"""

import aws_cdk as cdk
from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_dynamodb as dynamodb,
)
from aws_cdk.aws_stepfunctions_tasks import (
    LambdaInvoke,
    DynamoUpdateItem,
    DynamoAttributeValue,
)
from constructs import Construct


class InvestigationWorkflow(Construct):
    """CDK construct for the Step Functions investigation state machine."""

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        lambdas,  # WorkflowLambdas — duck-typed to avoid circular import
        table: dynamodb.ITable,
    ) -> None:
        super().__init__(scope, id)

        # ------------------------------------------------------------------
        # Terminal states
        # ------------------------------------------------------------------
        investigation_complete = sfn.Succeed(self, "InvestigationComplete")

        investigation_failed = sfn.Fail(
            self,
            "InvestigationFailed",
            cause="Investigation workflow encountered a critical failure",
            error="WorkflowError",
        )

        # ------------------------------------------------------------------
        # SaveFailedStatus — DynamoDB SDK integration
        # Reached on critical step failures (scope builder, hypotheses, report).
        # ------------------------------------------------------------------
        save_failed_status = DynamoUpdateItem(
            self,
            "SaveFailedStatus",
            table=table,
            key={
                "PK": DynamoAttributeValue.from_string(
                    sfn.JsonPath.format(
                        "INCIDENT#{}",
                        sfn.JsonPath.string_at("$.incidentId"),
                    )
                ),
                "SK": DynamoAttributeValue.from_string("META"),
            },
            update_expression="SET #s = :status, updatedAt = :updatedAt",
            expression_attribute_names={"#s": "status"},
            expression_attribute_values={
                ":status": DynamoAttributeValue.from_string("FAILED"),
                ":updatedAt": DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$$.State.EnteredTime")
                ),
            },
            result_path=sfn.JsonPath.DISCARD,
        )
        save_failed_status.add_retry(
            errors=["DynamoDB.ProvisionedThroughputExceededException", "DynamoDB.ThrottlingException", "States.TaskFailed"],
            interval=cdk.Duration.seconds(2),
            max_attempts=3,
            backoff_rate=2.0,
        )
        save_failed_status.next(investigation_failed)

        # ------------------------------------------------------------------
        # SaveCompletedStatus — DynamoDB SDK integration
        # Reached after FinalReportBuilder succeeds.
        # ------------------------------------------------------------------
        save_completed_status = DynamoUpdateItem(
            self,
            "SaveCompletedStatus",
            table=table,
            key={
                "PK": DynamoAttributeValue.from_string(
                    sfn.JsonPath.format(
                        "INCIDENT#{}",
                        sfn.JsonPath.string_at("$.incidentId"),
                    )
                ),
                "SK": DynamoAttributeValue.from_string("META"),
            },
            update_expression=(
                "SET #s = :status, updatedAt = :updatedAt,"
                " s3ReportKey = :s3Key, summary = :summary"
            ),
            expression_attribute_names={"#s": "status"},
            expression_attribute_values={
                ":status": DynamoAttributeValue.from_string("COMPLETED"),
                ":updatedAt": DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$$.State.EnteredTime")
                ),
                ":s3Key": DynamoAttributeValue.from_string(
                    sfn.JsonPath.format(
                        "investigations/{}/final-report.json",
                        sfn.JsonPath.string_at("$.incidentId"),
                    )
                ),
                ":summary": DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.reportOutput.summary")
                ),
            },
            result_path=sfn.JsonPath.DISCARD,
        )
        save_completed_status.add_retry(
            errors=["DynamoDB.ProvisionedThroughputExceededException", "DynamoDB.ThrottlingException", "States.TaskFailed"],
            interval=cdk.Duration.seconds(2),
            max_attempts=3,
            backoff_rate=2.0,
        )
        save_completed_status.next(investigation_complete)

        # ------------------------------------------------------------------
        # FinalReportBuilder
        # Receives full state; handler reads: scope, workerOutputs, hypotheses,
        # aiEvaluation.
        # result_path="$.reportOutput" so $.incidentId and $.reportOutput.summary
        # are both accessible by SaveCompletedStatus.
        # ------------------------------------------------------------------
        final_report_builder = LambdaInvoke(
            self,
            "FinalReportBuilder",
            lambda_function=lambdas.final_report_builder,
            payload_response_only=True,
            result_path="$.reportOutput",
        )
        final_report_builder.add_catch(
            save_failed_status, errors=["States.ALL"], result_path="$.errorInfo"
        )
        final_report_builder.next(save_completed_status)

        # ------------------------------------------------------------------
        # AIHypothesisEvaluator (Lambda → Bedrock)
        # Receives full state; evaluator reads scope, workerOutputs, hypotheses.
        # Falls back to {} on error so the workflow never breaks.
        # ------------------------------------------------------------------
        ai_eval = LambdaInvoke(
            self,
            "AIHypothesisEvaluator",
            lambda_function=lambdas.ai_hypothesis_evaluator,
            payload_response_only=True,
            result_path="$.aiEvaluation",
        )
        # Non-critical: on any failure, store empty dict and continue.
        ai_eval.add_catch(
            sfn.Pass(
                self,
                "FallbackAIEvaluation",
                result=sfn.Result.from_object({}),
                result_path="$.aiEvaluation",
            ),
            errors=["States.ALL"],
            result_path="$.errorInfo",
        )

        # ------------------------------------------------------------------
        # CandidateHypothesesBuilder
        # Receives full state; handler reads event["scope"] and
        # event["workerOutputs"] (both present after AggregateWorkerOutputs).
        # ------------------------------------------------------------------
        hypotheses_builder = LambdaInvoke(
            self,
            "CandidateHypothesesBuilder",
            lambda_function=lambdas.hypotheses_builder,
            payload_response_only=True,
            result_path="$.hypotheses",
        )
        hypotheses_builder.add_catch(
            save_failed_status, errors=["States.ALL"], result_path="$.errorInfo"
        )

        # ------------------------------------------------------------------
        # GatherEvidence (Parallel)
        # Each branch has its own Catch → Fallback Pass (ADR-009).
        # Fallback reads $.scope.incidentId from the branch's input state.
        # result_path="$.workerOutputsArray" preserves the rest of the state.
        # ------------------------------------------------------------------

        # Fallback Pass states for worker branch failures
        fallback_metrics = sfn.Pass(
            self,
            "FallbackMetrics",
            parameters={
                "incidentId.$": "$.scope.incidentId",
                "source": "metrics",
                "findings": [],
                "errors": [{"source": "metrics", "reason": "worker_failed"}],
            },
        )
        fallback_logs = sfn.Pass(
            self,
            "FallbackLogs",
            parameters={
                "incidentId.$": "$.scope.incidentId",
                "source": "logs",
                "findings": [],
                "errors": [{"source": "logs", "reason": "worker_failed"}],
            },
        )

        # Worker Lambda branches
        # payload sends only the scope dict; workers expect the scope as their event.
        # output_path="$.Payload" strips the SDK response wrapper {StatusCode, Payload}
        # so the parallel branch result is the raw worker output dict.
        # Note: payload_response_only=True must NOT be used here — in direct-ARN mode
        # CDK renders TaskInput.from_json_path_at as a bare string, not a path reference,
        # causing workers to receive the literal string "$.scope" instead of the scope dict.
        metrics_branch = LambdaInvoke(
            self,
            "MetricsWorker",
            lambda_function=lambdas.metrics_worker,
            payload=sfn.TaskInput.from_json_path_at("$.scope"),
            output_path="$.Payload",
        )
        metrics_branch.add_catch(
            fallback_metrics, errors=["States.ALL"], result_path="$.errorInfo"
        )

        logs_branch = LambdaInvoke(
            self,
            "LogsWorker",
            lambda_function=lambdas.logs_worker,
            payload=sfn.TaskInput.from_json_path_at("$.scope"),
            output_path="$.Payload",
        )
        logs_branch.add_catch(
            fallback_logs, errors=["States.ALL"], result_path="$.errorInfo"
        )

        fallback_traces = sfn.Pass(
            self,
            "FallbackTraces",
            parameters={
                "incidentId.$": "$.scope.incidentId",
                "source": "traces",
                "findings": [],
                "errors": [{"source": "traces", "reason": "worker_failed"}],
            },
        )

        traces_branch = LambdaInvoke(
            self,
            "TracesWorker",
            lambda_function=lambdas.traces_worker,
            payload=sfn.TaskInput.from_json_path_at("$.scope"),
            output_path="$.Payload",
        )
        traces_branch.add_catch(
            fallback_traces, errors=["States.ALL"], result_path="$.errorInfo"
        )

        # Parallel state: 3 branches in fixed order defined by WORKER_BRANCH_ORDER.
        # AggregateWorkerOutputs maps workerOutputsArray[0..2] to keyed dict entries
        # by the same index. Both are driven by the single constant below so that
        # reordering branches here automatically stays in sync with the aggregate step.
        WORKER_BRANCH_ORDER = ["metrics", "logs", "traces"]
        _branch_map = {
            "metrics": metrics_branch,
            "logs": logs_branch,
            "traces": traces_branch,
        }

        gather_evidence = sfn.Parallel(
            self,
            "GatherEvidence",
            result_path="$.workerOutputsArray",
        )
        for _name in WORKER_BRANCH_ORDER:
            gather_evidence.branch(_branch_map[_name])  # inserted in WORKER_BRANCH_ORDER order

        # Rebuild aggregate_outputs parameters using the same order constant.
        aggregate_outputs = sfn.Pass(
            self,
            "AggregateWorkerOutputs",
            parameters={
                "incidentId.$": "$.incidentId",
                "scope.$": "$.scope",
                "workerOutputs": {
                    f"{name}.$": f"$.workerOutputsArray[{i}]"
                    for i, name in enumerate(WORKER_BRANCH_ORDER)
                },
            },
        )

        # ------------------------------------------------------------------
        # ScopeBuilder
        # Receives the full execution input (incident payload).
        # result_path="$.scope" adds scope to the state without overwriting
        # the top-level incidentId needed for error-path DynamoDB writes.
        # ------------------------------------------------------------------
        scope_builder = LambdaInvoke(
            self,
            "ScopeBuilder",
            lambda_function=lambdas.scope_builder,
            payload_response_only=True,
            result_path="$.scope",
        )
        scope_builder.add_catch(
            save_failed_status, errors=["States.ALL"], result_path="$.errorInfo"
        )

        # ------------------------------------------------------------------
        # Main chain
        # Compose deterministic investigation chain.
        # ------------------------------------------------------------------
        definition = (
            sfn.Chain.start(scope_builder)
            .next(gather_evidence)
            .next(aggregate_outputs)
            .next(hypotheses_builder)
            .next(ai_eval)
            .next(final_report_builder)
        )

        # ------------------------------------------------------------------
        # State machine
        # ------------------------------------------------------------------
        self.state_machine = sfn.StateMachine(
            self,
            "InvestigationStateMachine",
            state_machine_name="IncidentInvestigationWorkflow",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=cdk.Duration.minutes(15),
        )

        # Grant the state machine role DynamoDB write access for the
        # SaveCompletedStatus and SaveFailedStatus SDK integration steps.
        table.grant_write_data(self.state_machine.role)
