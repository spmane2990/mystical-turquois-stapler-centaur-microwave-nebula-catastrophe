**Core Philosophy**
A prompt is a brief, not a wish. Claude is an execution engine whose output quality directly reflects the clarity, constraints, and structure provided in the instruction. Weak outputs stem from instruction problems, not model limitations.

---

**Lecture 1: Anatomy of an Effective Prompt (The 5 Building Blocks)**

High-performing prompts use the **R-C-T-C-F** framework ("Right Context Takes Clear Form"):

- **Role (Who)**: Defines persona, perspective, domain expertise, and tone (Optional/Conditional).

- _Example_: `"You are a senior Python reviewer..."`

- **Context (Why / Background)**: Grounding facts, business situation, audience details, or reference history (Conditional).

- _Example_: `"This function calculates order totals for PyStack Mart. GST must be applied AFTER the discount."`

- **Task (What to do)**: The primary, active-verb instruction (**Mandatory**).

- _Example_: `"Find and fix the bug."`

- **Constraints (Boundaries / Rules)**: Negative rules, word limits, banned terms, policy guardrails (Conditional).

- _Example_: `"Do not change the function signature. Keep it under 20 lines."`

- **Format (How to present)**: The structural schema (Almost Always Needed).

- _Example_: `"Corrected code block + 2-line explanation of what was wrong."`

**The Maturity Ladder & The Guess Test**

- **Level 1 (Bare Prompt)**: `"Write about tea."` $\rightarrow$ Claude must guess audience, length, tone, purpose, and format.

- **Level 2 (Task + Format)**: `"Write 3 short bullet points on why masala chai is popular in India."` $\rightarrow$ Task and format clear, but tone and audience missing.

- **Level 3 (Full 5 Blocks)**: Full brief with role, background, task, exact constraints, and format $\rightarrow$ Zero guesswork.

- **The Guess Test Rule**: Ask before sending: _"What is Claude being forced to guess right now?"_ Every guess is an assumption that can produce a defect.

---

**Lecture 2: Task Decomposition**

Complex requests fail when asked all at once because effort spreads thin, early errors silently poison downstream steps, and partial fixes become impossible without full regeneration.

- **Definition**: Splitting one complex request into smaller tasks that each produce a single, checkable output.

- **Where to Cut**: _"Would I want to see and inspect this before moving on?"_ If yes, make it its own step.

- **Three Splitting Patterns**:
- **Sequential**: Output of Step $A$ directly feeds Step $B$ (e.g., Extract Q&A $\rightarrow$ Categorize $\rightarrow$ Rewrite $\rightarrow$ Output JSON).

- **Parallel**: Independent pieces processed separately and synthesized later (e.g., Summarizing 5 reports independently).

- **Hierarchical**: Broad topics branching into sub-modules (e.g., Course $\rightarrow$ Sections $\rightarrow$ Lectures).

- **Avoid Over-Decomposition**: If a step has no output worth inspecting, it is overhead rather than decomposition.

---

**Lecture 3: Iterating Prompts to Improve Output Quality**

"Make it better" is a feeling, not feedback. Iteration is systematically diagnosing the gap and stating it with measurable precision.

**The Diagnostic Framework (Ask 3 Questions)**

1. **What is MISSING?** (A fact, detail, or unfulfilled requirement)

2. **What is WRONG?** (An inaccurate statement or hallucination)

3. **What is EXTRA?** (Filler, conversational padding, or unasked content)

**The Diagnosis & Correction Table**

| Subjective Feeling | Actionable Prompt Instruction |
| ------------------ | ----------------------------- |

| "Too long"

| `"Cut to 100 words"`<br> |
| "Too formal"

| `"Rewrite as if explaining to a friend"`<br> |
| "Wrong focus"

| `"Lead with the price advantage, not the features"`<br> |
| "Made things up"

| `"Only use facts from the document I shared"`<br> |
| "Boring"

| `"Open with a question, use active voice"`<br> |

- **Few-Shot Examples**: Providing a concrete input-output sample communicates tone, structure, and length better than descriptive adjectives.

- **The 4-Follow-Up Rule**: If a chat thread requires $\ge 4$ follow-up adjustments, the original prompt foundation is broken. Rewrite the base prompt and restart in a clean window.

- **When to Stop**: Stop iterating when output meets stated criteria or when minor edits are faster by hand. If $4\text{--}5$ iterations fail to improve, change the approach (decompose task, supply missing context, or switch model).

---

**Lecture 4: Adapting Prompting Strategies by Task Type**

A tight constraint set enhances drafting but restricts brainstorming. Match the prompt architecture to the operational objective:

| Task Type        | Objective & Core Fear        | Key Prompt Requirements | Special Prompting Move |
| ---------------- | ---------------------------- | ----------------------- | ---------------------- |
| **Analysis**<br> | **Want**: Insight & judgment |

<br>

<br>**Fear**: Surface summary

| Supply full raw data; specify analytical lens (e.g., cost, retention)

| Ask for underlying reasoning and what the data _cannot_ tell you.

|
| **Research**<br> | **Want**: Verified facts

<br>

<br>**Fear**: Hallucinations

| Separate known facts from inferences; demand citations

| Require explicit uncertainty flags (`"Mark as Confirmed/Uncertain; say 'not sure' instead of guessing"`).

|
| **Drafting**<br> | **Want**: Production copy

<br>

<br>**Fear**: Generic filler

| Lock the 4 pillars: Audience, Tone, Length, Format

| Provide a reference sample of the target voice.

|
| **Brainstorming**<br> | **Want**: Volume & range

<br>

<br>**Fear**: Safe, obvious ideas

| Remove constraints; explicitly permit unconventional ideas

| **Diverge first** (ask for 15+ ideas), then **Converge** (filter/rank) in a second prompt.

|

---

**Exam Summary & Key Takeaways**

- **Specificity Beats Length**: Replace adjectives with numbers and measurable targets.

- **Task Decomposition**: Break complex tasks at natural inspection points.

- **Diagnosis Over Persistence**: Translate vague dissatisfaction into concrete constraints or few-shot examples.

- **Divergence vs. Convergence**: Never apply tight constraints in the same prompt as a creative brainstorm.
