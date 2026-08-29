**Domain 7: Troubleshooting and Optimization**

**Core Philosophy**
When an answer is poor, do not simply type the prompt again and hope for a better result. Trying again is not fixing. Systematic troubleshooting begins by diagnosing the root cause across three possible failure points: the prompt, the information, or the workspace setup.

---

**Lecture 1: Diagnosing and Fixing Poor Outputs**

Every bad output happens for a reason. Diagnosing the cause must happen before changing anything.

- **The 3 Common Root Causes**:

1. **Unclear Prompt**: You did not clearly specify what you actually wanted (constraints, tone, format).

2. **Missing Information**: Claude lacked a critical reference fact, document, or context variable needed to complete the task.

3. **Wrong Setup**: The task used the wrong model tier, an overloaded context window, or outdated files in the Project.

- **The 3 Sequential Diagnosis Questions**:

1. _Did I say clearly what I wanted?_ $\rightarrow$ If **No**, rewrite the prompt with clear constraints and formatting.

2. _Did Claude have all the facts?_ $\rightarrow$ If **No**, supply the missing source data or documentation.

3. _Is my setup right for this task?_ $\rightarrow$ If **No**, switch the model tier or refresh the workspace files.
   (Most operational issues stop at Question 1: the prompt simply did not give enough direction.)

- **The Doctor Analogy**: A competent doctor asks diagnostic questions to identify the illness before prescribing medication; a bad doctor randomly hands out pills hoping one works. Random prompt tweaking is guessing, not troubleshooting.

- **Single-Variable Calibration**: Change **one thing at a time**, then check the result. Modifying prompt wording, model selection, and context files simultaneously prevents you from knowing which change actually resolved the problem.

---

**Lecture 2: Adjusting Your Approach From Feedback**

Every response Claude generates provides diagnostic feedback. Read the output, determine the size of the gap, and match the scale of the correction.

- **Fix Sizing Framework**:
- **Small Gap (Tone, length, minor detail)**: Request a targeted single-line correction without discarding the good draft.

- **Medium Gap (Wrong focus, omitted requirement)**: Adjust the specific constraint in the prompt while preserving the remaining structure.

- **Large Gap (Completely off-target or hallucinated)**: Discard the thread, rewrite the core instruction with full R-C-T-C-F blocks, and restart clean.

- **Providing Actionable Feedback**:
- _Vague Feedback (Unusable)_: `"Make it better"`, `"I don't like it"`, `"Fix it"`.

- _Actionable Feedback (Effective)_: `"Make it shorter—under 2 lines"`, `"The tone is too formal; make it warmer"`, `"Place the price at the end"`.

- _The Driving Directions Analogy_: Telling Claude _"make it better"_ is like telling a driver _"drive better"_. Clear instructions (_"Turn left at the next signal"_) get you to the destination quickly.

- **Knowing When to Stop**:
- Stop iterating once the output fulfills the core business criteria and is good enough to use in production.

- Avoid endlessly tweaking an already functional draft to chase unattainable perfection. Good enough and shipped today beats perfect and delayed.

---

**Lecture 3: Optimizing Your Workflow**

Achieving a high-quality answer once is only the first step. Optimizing means structuring your environment so you achieve high quality quickly and consistently every day.

- **Eliminate Repetitive Setup**:
- If you paste the same reference documents daily $\rightarrow$ Move them into **Project Knowledge**.

- If you type the same tone and formatting rules daily $\rightarrow$ Store them in **Project Instructions**.

- If you rewrite common prompt structures daily $\rightarrow$ Save and templatize them.

- **Balancing Efficiency and Effectiveness**:
- **Efficiency**: Doing tasks faster (e.g., eliminating manual context pasting).

- **Effectiveness**: Doing tasks better (e.g., achieving consistent accuracy and brand alignment).

- _Goal_: Fast and correct. Fast but inaccurate is useless; accurate but slow creates operational drag.

- **The Kitchen Mise-en-Place Analogy**: A smart chef organizes knives, spices, and cookware once before cooking so every subsequent dish is prepared rapidly. Setting up a Claude Project once streamlines every subsequent task.

- **Avoid Over-Optimization**: Focus on optimizing frequent, high-volume daily tasks. Spending two hours automating a task executed once a year wastes more time than it saves.

---

**Domain 7 & Course Summary: The Complete Associate Toolkit**

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  D1: PROMPT CRAFT    ──► Structure briefs with R-C-T-C-F & decompose steps[cite: 16]          │
│  D2: VALIDATION      ──► Accuracy vs. completeness; never trust unhedged tone[cite: 16]      │
│  D3: PRODUCT & MODEL ──► Match features (Projects/Artifacts) and models to stakes[cite: 16]   │
│  D4: WORKFLOW DESIGN ──► Augment existing pipelines; map bottlenecks before AI[cite: 16]      │
│  D5: CONFIGURATION   ──► Build Projects (Instructions + Knowledge); prevent drift[cite: 16]   │
│  D6: GOVERNANCE      ──► Enforce suitability, mask PII, and maintain human control[cite: 16]  │
│  D7: OPTIMIZATION    ──► Diagnose causes systematically; optimize daily workflows[cite: 16]   │
└────────────────────────────────────────────────────────────────────────────────────────┘

```
