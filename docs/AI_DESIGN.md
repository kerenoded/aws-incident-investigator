# AI Design

## Why AI Exists in This Project
The AI is not here only to rewrite text. It should contribute to bounded investigation reasoning.

The intended value of AI in this project is:
- evaluate competing hypotheses
- identify ambiguity or missing evidence
- help produce a clear incident explanation

## AI Components

### 1. AI Hypothesis Evaluator
Input:
- incident summary
- structured worker findings
- candidate hypotheses

Output:
- plausibility assessment per hypothesis
- reasoning notes
- uncertainty / missing evidence notes
- structured machine-readable response

Purpose:
- compare competing causes
- help interpret evidence that spans multiple telemetry sources
- surface missing evidence that would improve investigation confidence

## AI Input Rules
AI should receive:
- structured evidence only
- concise summaries and key metrics
- candidate hypotheses with evidence references

AI should **not** receive:
- full raw log streams
- unbounded telemetry dumps
- open-ended “do anything” instructions

## AI Output Rules
All AI output should be structured and machine-readable.

At minimum:
- include selected/assessed hypothesis names
- include rationale
- include confidence/uncertainty
- include evidence references where possible

## Best Practices
- keep prompts concise and explicit
- ask the model to state uncertainty when evidence is incomplete
- prefer JSON output
- validate output structure before continuing workflow
- fail safely if output is malformed
- keep a deterministic pre-AI hypothesis shortlist
- avoid letting AI manufacture new unsupported evidence

## What AI Must Not Do
- invent evidence
- replace deterministic telemetry retrieval
- claim certainty without support

## Recommended Division of Responsibility
- deterministic workers: detect abnormality/suspicious signals
- deterministic hypothesis builder: create plausible cause shortlist
- AI hypothesis evaluator: compare and assess plausible causes
- final report stage: assemble clear output for UI/storage

## Example Hypothesis Evaluation Output
```json
{
  "topHypothesis": "checkout-service runtime latency regression",
  "assessments": [
    {
      "cause": "checkout-service runtime latency regression",
      "plausibility": 0.78,
      "reason": "Latency spike and timeout errors align with trace and log evidence."
    },
    {
      "cause": "database latency increase",
      "plausibility": 0.16,
      "reason": "DB latency rose slightly, but traces do not show it as the dominant contributor."
    }
  ],
  "missingEvidence": [
    "dependency timeout distribution in traces",
    "container restart count"
  ]
}
```

---
