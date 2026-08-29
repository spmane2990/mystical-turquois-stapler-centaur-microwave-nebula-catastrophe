**Domain 5: Configuration and Knowledge Management**

**Core Principle**: A single team member holding all the prompt templates and knowledge files creates an operational bottleneck. A configured **Claude Project** packages standing rules and reference documents into a reusable workspace for the whole team.

---

**Lecture 1: Configuring Claude Projects**

A Claude Project consists of two primary configuration areas:

| INSTRUCTIONS (How Claude Acts)             | KNOWLEDGE (What Claude Knows)              |
| ------------------------------------------ | ------------------------------------------ |
| Rules, behavior, persona, and tone         | Files, policy binders, price lists, FAQs   |
| Runs silently on every chat in the Project | Reference shelf Claude reads automatically |
| "What you would say out loud"              | "What you would hand over as a document"   |

- **The Two-Box Test**: If it is a rule (_"Always use local currency ₹"_, _"Draft replies only"_), put it in **Instructions**[cite: 14]. If it is a document (_Brand & tone guide_, _Delivery policy_, _20 best past replies_), put it in **Knowledge**[cite: 14].
- **The New Employee's Desk Analogy**: Setting up a Project is like preparing a desk before a new hire's first day[cite: 14]. Put the standing rules note in Instructions and the policy binders in Knowledge so anyone sitting at the desk can work immediately[cite: 14].
- **Anti-Pattern**: Pasting files manually into every individual chat or embedding long multi-page documents inside the instructions box[cite: 14].

---

**Lecture 2: Managing Knowledge and Connectors**

Static uploads and dynamic connectors serve distinct operational needs[cite: 14]:

| Vector                  | Storage Mechanism                             | Freshness                                  | Best For                                                                   | Operational Risk                                            |
| :---------------------- | :-------------------------------------------- | :----------------------------------------- | :------------------------------------------------------------------------- | :---------------------------------------------------------- |
| **Upload**[cite: 14]    | Static file copied into the Project[cite: 14] | Fixed at upload time ("A Photo")[cite: 14] | Things that rarely change (Brand guides, complaint categories)[cite: 14]   | Becomes wrong without warning if reality changes[cite: 14]. |
| **Connector**[cite: 14] | Live link (Google Drive, Gmail)[cite: 14]     | Always current ("A Live View")[cite: 14]   | Things that change often (Live delivery policies, active emails)[cite: 14] | Requires maintained access permissions[cite: 14].           |

- **The Train Schedule Analogy**: A printed timetable is an upload—fixed and dangerous if schedules shift[cite: 14]. A live website is a connector—always updated[cite: 14].
- **Knowledge Hygiene & Pruning**: Extra, outdated, or duplicate files confuse model retrieval and slow performance[cite: 14].
  - _Test_: _"Would this file help answer a real question?"_ If no, delete it[cite: 14]. Keep only the core relevant files or upload just the useful excerpt[cite: 14].

---

**Lecture 3: Creating Effective System Instructions**

System instructions run quietly on **every message** across all chats in a Project[cite: 14]. Vague instructions like _"Be helpful and friendly"_ fail quietly by leaving limits and formatting to chance[cite: 14].

**The 4 Essential Components of Strong Instructions**

1. **Who is Claude?**: Role and company persona (e.g., _"Support assistant for PyStack Mart"_)[cite: 14].
2. **How to behave?**: Tone, style, and formatting rules (e.g., _"Warm, simple tone; use ₹, never $"_)[cite: 14].
3. **What are the limits?**: Guardrails and negative boundaries (e.g., _"Draft replies only; never promise refunds"_)[cite: 14].
4. **What to do when stuck?**: The fallback escape hatch (e.g., _"If order details are missing, ask for them; if unsure, say so and do not guess"_)[cite: 14].

- **Affirmative Guidance (Do vs. Don't)**: A rule that only forbids leaves an empty hole[cite: 14]. Always pair a restriction with an affirmative direction[cite: 14]:
  - _Instead of just_: _"Don't be formal"_ $\rightarrow$ _Write_: _"Write like you're helping a friend"_[cite: 14].
  - _Instead of just_: _"Don't make promises"_ $\rightarrow$ _Write_: _"Say 'I'll pass this to the team'"_[cite: 14].

---

**Lecture 4: Informing, Maintaining & Updating**

A Project is a **garden to tend, not a static statue**[cite: 14]. Over time, **configuration drift** occurs: real-world prices, products, and policies shift while the Project setup stays frozen, causing silent errors[cite: 14].

**The 3 Maintenance Jobs**

- **Inform**: Add what is new (e.g., a new complaint category or product launch)[cite: 14].
- **Maintain**: Check existing infrastructure (e.g., verify Google Drive connector permissions)[cite: 14].
- **Update**: Fix what changed (e.g., replace legacy price sheets with active lists)[cite: 14].

- **The Monthly 15-Minute Review**: Run a recurring audit:
  1. _Are prices and policies in Knowledge still correct?_[cite: 14]
  2. _Are there new complaint types to add?_[cite: 14]
  3. _Do Instructions still match active company policy?_[cite: 14]
  4. _Are connectors still linked and working?_[cite: 14]
- **Safe Updating Practice**: Change **one element at a time**, then validate the change with 3 real sample test cases before rolling it out to the entire team[cite: 14]. Never rewrite instructions and swap all knowledge files simultaneously[cite: 14].

---

**Exam Summary & Key Takeaways**

- **Instructions vs. Knowledge**: Behavioral rules go in Instructions; reference documents go in Knowledge[cite: 14].
- **Upload vs. Connect**: Upload stable reference files; connect dynamic sources that update frequently[cite: 14].
- **Instruction Design**: Must define Role, Behavior, Limits, and **What to do when stuck**[cite: 14].
- **Maintenance**: Guard against silent configuration drift with regular **Inform, Maintain, Update** routines[cite: 14].

```

```
