**Domain 3: Product and Model Selection**

**Core Principle**: Same Claude underneath, different container around it. Features and containers do not alter output quality—they determine what context Claude can access and what assets you can retain. Model tier selection balances task difficulty, throughput volume, and the cost of error.

---

**Lecture 1: Selecting the Right Claude Product Feature**

Choosing the wrong workspace feature does not cause an inaccurate answer; it creates unnecessary friction and wastes operational time.

- **Chat (Default)**: Best for quick queries, iterative brainstorming, and single-use, throwaway drafting.

- _Weakness_: Inefficient when tasks require re-pasting the same reference files daily or when assets must be saved and edited across sessions.

- **Projects (Persistent Workspace)**: A centralized workspace where reference files (knowledge) and standing rules (custom instructions) stay pre-loaded.

- _Core Rule_: Chat requires the user to carry the context manually; a Project ensures the context waits for the user. If a document or rule is pasted more than once, it belongs in a Project.

- **Artifacts (Standalone Output Panel)**: Dedicated side-panel windows for code, documents, SVGs, or structured data.

- _Decision Test_: Ask, _"Will I come back to, edit, download, or share this output?"_ If yes, use an Artifact instead of standard inline chat text.

- **Research Mode (Multi-Source Web Search)**: Claude searches across multiple online sources to synthesize current facts with citations.

- _Trade-off_: Slower by design; buys external depth at the expense of latency. Do not use Research Mode when the source document is already in hand.

---

**Lecture 2: Differentiating Claude Model Types**

The model family operates as a toolbox rather than a ladder. Haiku is not an "outdated" model, and Opus is not a universal default.

| Model      | Best Use Case                            | Core Strength                |
| ---------- | ---------------------------------------- | ---------------------------- |
| **Haiku**  | High-volume, low-complexity work         | Lowest latency and cost      |
| **Sonnet** | Standard drafting, coding, synthesis     | Strong speed-quality balance |
| **Opus**   | Deep reasoning and high-stakes ambiguity | Highest reasoning depth      |

**Key Selection Guidelines**

**Key Selection Guidelines**

- **Start at Sonnet**: Sonnet is the balanced standard baseline[cite: 12]. Move to Haiku for simple, high-volume tasks or Opus for genuinely deep reasoning only when there is a clear operational reason[cite: 12].
- **Task Difficulty vs. Model Tier**: On simple tasks (e.g., fixing a sentence's spelling), Opus produces the same output as Haiku while running slower and costing significantly more[cite: 12].
- **Two Classic Mistakes**:
  1. _Opus for everything_: Burns time and budget on simple jobs[cite: 12].
  2. _Switching models instead of fixing the prompt_: If output is flawed due to vague instructions, upgrading to a larger model will not resolve the defect[cite: 12].

---

**Lecture 3: Aligning Model Selection with Task Requirements**

Select a model by answering three sequential questions[cite: 12]:

1. **How hard is the thinking?**[cite: 12]
   - _Simple / Fixed list / Routine_: Haiku[cite: 12].
   - _Standard drafting, coding, synthesis_: Sonnet[cite: 12].
   - _Complex trade-offs, multi-step logic_: Opus[cite: 12].
2. **How many times will it run?**[cite: 12]
   - _Single run_: Optimize primarily for quality[cite: 12].
   - _Thousands of runs (Scale)_: Cost per item dictates the decision; the cheapest model that meets the bar is the correct engineering choice[cite: 12].
3. **What happens if it is wrong? (The Overruling Question)**[cite: 12]
   - _Low stakes (Minor tag error)_: Cheap model[cite: 12].
   - _Medium stakes (Internal drafts)_: Default model[cite: 12].
   - _High stakes (Major financial, legal, or strategic impact)_: Most capable model **plus mandatory qualified human review**[cite: 12]. Stakes overrule cost and latency[cite: 12].

- **Empirical Testing**: Run a benchmark sample of 20 real test cases on Haiku first[cite: 12]. Upgrade to Sonnet or Opus only if the cheaper tier fails quality thresholds[cite: 12].

---

**Lecture 4: Context Limitations and Memory**

**The Whiteboard Dynamic**: Claude has no memory between turns or across chats[cite: 12]. Every time a user submits a prompt, Claude re-reads the active context window from top to bottom[cite: 12].

> **The Whiteboard Metaphor**:
>
> - A notebook remembers across pages.
> - A whiteboard shows only what fits on the active surface.
> - When the board is full, early content gets wiped to make room for new text.

**Context Window Eviction & Warning Signs**

- When a chat reaches high message counts, early rules (e.g., `"always use ₹, never $"`) fall out of view and get ignored[cite: 12].
- _Warning Signs_: Claude ignores early instructions, repeats previous explanations, or drifts off the original topic[cite: 12].
- _Anti-Pattern_: Arguing with Claude (_"I told you this in message 1!"_) adds another message, pushing earlier instructions even further out of view[cite: 12].

**The Three Context Fixes**

| Duration Needed                          | Strategy                | Action                                                                                             |
| :--------------------------------------- | :---------------------- | :------------------------------------------------------------------------------------------------- |
| **This specific chat only**[cite: 12]    | _Do Nothing_[cite: 12]  | Proceed inside current window[cite: 12].                                                           |
| **This piece of ongoing work**[cite: 12] | **Summarize**[cite: 12] | Ask Claude to summarize agreed decisions into 10 bullet points; paste into a fresh chat[cite: 12]. |
| **Every chat, permanently**[cite: 12]    | **Persist**[cite: 12]   | Place standing rules and files into **Project Instructions / Knowledge**[cite: 12].                |
| **Topic changed**[cite: 12]              | **Restart**[cite: 12]   | Open a fresh chat window[cite: 12].                                                                |

- **Context Hygiene**: Do not attach files "just in case"[cite: 12]. Every unneeded file takes up context space and pushes vital instructions out of view[cite: 12]. Maintain a **one chat = one job** workflow[cite: 12].

---

**Exam Summary & Key Takeaways**

- **Feature Selection**: Reuse context $\rightarrow$ **Project**; Keep/Edit output $\rightarrow$ **Artifact**; Broad web data $\rightarrow$ **Research Mode**; Quick one-off $\rightarrow$ **Chat**[cite: 12].
- **Model Selection**: Start at **Sonnet**[cite: 12]. Use **Haiku** for volume/speed and **Opus** for deep reasoning[cite: 12].
- **Consequence Overrules Speed/Cost**: High-stakes decisions demand the most capable model combined with qualified human sign-off[cite: 12].
- **Memory Management**: Claude re-reads, it does not remember[cite: 12]. Manage the window via **Restart**, **Summarize**, or **Persist**[cite: 12].

```

```
