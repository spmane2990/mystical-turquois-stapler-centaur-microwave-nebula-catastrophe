# Claude Certified Associate: Master Study Guide & Notes

A comprehensive, structured study repository covering the 7 core domains of the **Claude Certified Associate** curriculum. This guide consolidates best practices, practical frameworks, architecture decisions, governance rules, and troubleshooting workflows for building with Anthropic's Claude.

---

## 📚 Repository Structure & Module Overview

| Domain       | File Name                                                                                                    | Primary Focus & Core Concepts                                                      |
| ------------ | ------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------- |
| **Domain 1** | [`Domain - 1 - Prompting-and-Task-Execution.md`](./Domain%20-%202%20-%20Output-Evaluation-and-Validation.md) | • **R-C-T-C-F Framework** (Role, Context, Task, Constraints, Format)[cite: 10]<br> |

<br>• The "Guess Test" & prompt maturity ladders[cite: 10]<br>

<br>• Task decomposition (Sequential, Parallel, Hierarchical)[cite: 10]<br>

<br>• Strategy by task type (Analysis, Research, Drafting, Brainstorming)[cite: 10] |
| **Domain 2** | [`Domain - 2 - Output-Evaluation-and-Validation.md`](./Domain%20-%202%20-%20Output-Evaluation-and-Validation.md) | • **Golden Rule**: _Confident is not correct_[cite: 11]<br>

<br>• Accuracy vs. Completeness & The Completeness Trap[cite: 11]<br>

<br>• Hallucinations vs. Inconsistencies vs. Bias[cite: 11]<br>

<br>• Source-grounding, verification, and human-in-the-loop review tiers[cite: 11] |
| **Domain 3** | [`Domain - 3 - Product-and-Model-Selection.md`](./Domain%20-%203%20-%20Product-and-Model-Selection.md) | • Feature selection: Chat vs. Projects vs. Artifacts vs. Research Mode[cite: 12]<br>

<br>• Model selection matrix: **Haiku**, **Sonnet**, **Opus**[cite: 12]<br>

<br>• Context window dynamics & the Whiteboard Metaphor[cite: 12]<br>

<br>• Context hygiene: Restart, Summarize, and Persist[cite: 12] |
| **Domain 4** | [`Domain - 4 - Workflow-Integration-and-Solution-Design.md`](./Domain%20-%204%20-%20Workflow-Integration-and-Solution-Design.md) | • Translating vague wishes into testable requirements[cite: 13]<br>

<br>• Process mapping & bottleneck optimization[cite: 13]<br>

<br>• Minimum Viable Piece (MVP) & layered solution development[cite: 13]<br>

<br>• Integration strategies: **Augment** (default) vs. **Redesign**[cite: 13] |
| **Domain 5** | [`Domain - 5 - Configuration-and-Knowledge-Management.md`](./Domain%20-%205%20-%20Configuration-and-Knowledge-Management.md) | • The Two-Box Architecture (System Instructions vs. Project Knowledge)[cite: 14]<br>

<br>• Static Uploads vs. Dynamic Live Connectors[cite: 14]<br>

<br>• Effective System Instructions: 4 core questions & affirmative guidance[cite: 14]<br>

<br>• Mitigating configuration drift (Inform, Maintain, Update)[cite: 14] |
| **Domain 6** | [`Domain - 6 - Governance-Risk-and-Responsible-Use.md`](./Domain%20-%206%20-%20Governance-Risk-and-Responsible-Use.md) | • **Core Principle**: _Capability is not suitability_[cite: 15]<br>

<br>• Appropriate (AI Assists) vs. Inappropriate (AI Decides)[cite: 15]<br>

<br>• Data classification, minimization, masking & the Postcard Rule[cite: 15]<br>

<br>• Ethics pillars: Honesty, Fairness, Transparency, and Harm Prevention[cite: 15] |
| **Domain 7** | [`Domain - 7 - Troubleshooting-and-Optimization.md`](./Domain%20-%207%20-%20Troubleshooting-and-Optimization.md) | • Systematic root-cause diagnosis (Prompt vs. Data vs. Setup)[cite: 16]<br>

<br>• Feedback calibration & the Single-Variable Rule[cite: 16]<br>

<br>• Workflow tuning: Balancing efficiency with effectiveness[cite: 16]<br>

<br>• The Complete Claude Certified Associate Toolkit[cite: 16] |

---

## 🎯 Quick Navigation & Core Principles

````
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  D1: PROMPT CRAFT    ──► Structure briefs with R-C-T-C-F & decompose complex tasks     │
│  D2: VALIDATION      ──► Confidence != correctness; audit accuracy & completeness      │
│  D3: PRODUCT & MODEL ──► Match features (Artifacts/Projects) and models to stakes     │
│  D4: WORKFLOW DESIGN ──► Augment existing pipelines; map bottlenecks before automating │
│  D5: CONFIGURATION   ──► Centralize rules in Instructions and documents in Knowledge   │
│  D6: GOVERNANCE      ──► Enforce suitability, mask sensitive data, and retain humans  │
│  D7: OPTIMIZATION    ──► Isolate root causes systematically; eliminate slow steps      │
└────────────────────────────────────────────────────────────────────────────────────────┘
```[cite: 8, 10, 11, 12, 13, 14, 15, 16]

---

## 🛠️ Key Takeaways Cheat Sheet

* **Prompting**: A prompt is an instruction brief, not a wish[cite: 10]. If an intern would have to guess, Claude will guess[cite: 10]. See [`Domain - 1 - Prompting-and-Task-Execution.md`](./Domain%20-%201%20-%20Prompting-and-Task-Execution.md)[cite: 10].
* **Output Checking**: Never judge an output by how sure it sounds[cite: 11]. Verify against primary source data and require citations/receipts[cite: 11]. See [`Domain - 2 - Output-Evaluation-and-Validation.md`](./Domain%20-%202%20-%20Output-Evaluation-and-Validation.md)[cite: 11].
* **Model Selection**: Start with **Sonnet** as your default baseline[cite: 12]. Shift to **Haiku** for high-volume, low-complexity tasks, or **Opus** for multi-step reasoning and complex domain ambiguity[cite: 12]. See [`Domain - 3 - Product-and-Model-Selection.md`](./Domain%20-%203%20-%20Product-and-Model-Selection.md)[cite: 12].
* **Context**: Claude does not have cross-session memory; it works on an active whiteboard and re-reads the active window on every turn[cite: 12]. See [`Domain - 3 - Product-and-Model-Selection.md`](./Domain%20-%203%20-%20Product-and-Model-Selection.md)[cite: 12].
* **Workflow Design**: Automating a broken process scales waste; map bottlenecks and augment human steps first[cite: 13]. See [`Domain - 4 - Workflow-Integration-and-Solution-Design.md`](./Domain%20-%204%20-%20Workflow-Integration-and-Solution-Design.md)[cite: 13].
* **Knowledge Setup**: Rules you would say out loud go into **Instructions**; documents you would hand over go into **Knowledge**[cite: 14]. See [`Domain - 5 - Configuration-and-Knowledge-Management.md`](./Domain%20-%205%20-%20Configuration-and-Knowledge-Management.md)[cite: 14].
* **Governance**: The tool has no ethics—the person operating it does[cite: 15]. Keep a human in the loop for any decision affecting a person's life, finances, health, or legal status[cite: 15]. See [`Domain - 6 - Governance-Risk-and-Responsible-Use.md`](./Domain%20-%206%20-%20Governance-Risk-and-Responsible-Use.md)[cite: 15].
* **Troubleshooting**: Do not just re-run a bad prompt[cite: 16]. Isolate the cause (Prompt, Data, or Setup) and change **one variable at a time**[cite: 16]. See [`Domain - 7 - Troubleshooting-and-Optimization.md`](./Domain%20-%207%20-%20Troubleshooting-and-Optimization.md)[cite: 16].

````
