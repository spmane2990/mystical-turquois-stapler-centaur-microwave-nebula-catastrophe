**Domain 2: Output Evaluation and Validation**

**The Golden Rule**: **Confident is not correct.** Claude always sounds authoritative and sure, whether it is stating an accurate fact or generating a total error. Confidence is a writing style, not evidence. The user is the final checkpoint, not the model.

---

**Lecture 1: Evaluating Outputs for Accuracy & Completeness**

Evaluating an output requires two distinct checks:

- **Accuracy (What is present)**: Verifying visible data. Hunting for wrong facts, incorrect math, fabricated figures, or inaccurate names.

- **Completeness (What is missing)**: Looking for invisible omissions, skipped requirements, or missing procedural steps.

- **The Completeness Trap**: Claude does not announce when it skips an instruction (e.g., answering 3 of 4 prompts and silently omitting the cost estimation). Silence does not mean "nothing to report"; missing work looks identical to finished work.

- **The Prompt Checklist Technique**: Use your original prompt as a line-by-line verification checklist upon receiving the output.

- **Error Clustering**: Errors cluster around exact numbers, names, the very last requested instruction, and hyper-specific details. Passing unit tests proves what code does, not that it fulfills every requested requirement.

---

**Lecture 2: Hallucinations, Inconsistencies & Bias**

| Error Type        | Scope                      | Simple Meaning                            | Real-World Example                                          |
| ----------------- | -------------------------- | ----------------------------------------- | ----------------------------------------------------------- |
| **Hallucination** | Wrong vs. **The World**    | Claude invented an ungrounded claim       | Citing a non-existent law or dead URL                       |
| **Inconsistency** | Wrong vs. **Itself**       | Claude contradicts its own earlier output | Stating complaints rose on slide 2, but declined on slide 5 |
| **Bias**          | Unbalanced vs. **Reality** | Claude generates a one-sided perspective  | Listing only benefits and zero operational risks            |

- **Mechanism of Hallucination**: Models predict the most probable next token. Lying requires intent; hallucination is a confident wrong guess with no internal alarm mechanism.

- **Risk Heatmap**:
- _High Risk_: Exact statistics, citations, case laws, verbatim quotes, recent events.

- _Low Risk_: General concepts, brainstorming, and summarizing user-provided text.

- **Red Flags**: Overly round numbers, unhedged certainty (absence of "roughly" or "around"), links that fail to resolve, and obscure citations presented with extreme precision.

- **Prompt-Induced Bias**: Asking _"Why should we do X?"_ forces a biased output. Run the **Neutral Test** by prompting for counter-arguments (_"Why should we NOT do X?"_).

---

**Lecture 3: Fact-Checking & Validation Techniques**

- **Spend Your Verification Budget on Risk**: Focus verification strictly where errors cause financial loss, legal liability, or damage to trust.

- **Technique 1: Go to the Source**: Verify claims against primary external files, government databases, or raw sales records.

- _Critical Rule_: Never ask Claude _"Are you sure?"_ within the same chat. A model cannot serve as its own witness.

- **Technique 2: Make Claude Show Its Work**: Require verbatim source quotes for every claim, and enforce a `"NOT IN SOURCE"` directive to turn invisible hallucinations into visible alerts.

- **Technique 3: Ground in Your Own Data**: Supply source documentation inside the prompt. Reading provided data has significantly lower hallucination risk than recalling from parametric memory.

- **Technique 4: Ask Twice and Compare**: Re-prompt the task in a fresh session. Matching answers provide weak signal (not proof), while conflicting answers reveal an immediate error.

---

**Lecture 4: When Human Review Is Required**

Risk is determined by **consequences**, not model confidence. The same text carries low risk in an internal chat, but high risk on a public website.

- **The Stakes Ladder**:
- _Level 1 (Nobody Affected)_: Personal ideation $\rightarrow$ Skim check.

- _Level 2 (Internal / Fixable)_: Team emails, operational drafts $\rightarrow$ Self-review.

- _Level 3 (External / Costly)_: Customer communications, live pricing $\rightarrow$ Peer check.

- _Level 4 (High Stakes)_: Legal, tax, medical, safety, HR decisions $\rightarrow$ **Mandatory qualified expert sign-off**.

- **The Four Red Zones**: Legal, Financial, Medical/Safety, and People Decisions (Hiring/Firing). In these zones, Claude may assist with drafts, but a human must make the final decision.

- **The Signature Test**:

1. _Would I put my personal name and reputation on this output?_

2. _Am I professionally qualified to judge and sign off on this?_ If no, escalate immediately.

---

**Lecture 5: Editing, Adapting & Comparing Outputs**

Correctness is the baseline floor, not the finished product. An output must be tailored to the specific reader:

- **Core Actions**:
- **Edit**: Correct errors and update factual figures.

- **Adapt**: Transform identical facts for different audiences (e.g., technical depth for engineers vs. business impact for leadership vs. reassurance for customers).

- **Refine**: Tighten language, remove filler, and enforce strict word limits.

- **Compare**: Request two contrasting variations (e.g., warm vs. direct) in one prompt to evaluate which fits best.

- **The Audience Test**: Determine what the reader already knows, what action they must take, and how much time they have.

---

**Lecture 6: Organizing Information & Choosing Output Formats**

- **Curation**: Cut unneeded details and place the primary conclusion or action item first.

- **Format Selection Framework**:
- **Inline**: Best for ephemeral queries, conversational thinking, and one-time reading inside the chat.

- **Artifact**: Best for assets that must be saved, edited, reused, downloaded, or shared.

- **Structured Data (JSON / CSV / Tables)**: Mandatory when downstream software, databases, or parsers read the output.

- **Rules for Structured Data**:

1. Specify the exact JSON schema.

2. Explicitly ban markdown wrappers, preambles, and conversational intros to avoid breaking parsers.

3. Define explicit missing-value handling (e.g., `"use null, never invent values"`).

---

**Domain 2 Summary: The 5-Question Habit**

1. **Accuracy & Completeness**: _Is it right? Is anything missing?_

2. **Integrity**: _Is anything invented, contradictory, or one-sided?_

3. **Verification**: _Can I prove the critical claims against primary sources?_

4. **Accountability**: _Who else must review this before it ships, and would I sign it?_

5. **Usability**: _Is it adapted for the intended reader in the correct container?_
