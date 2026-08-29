**Domain 6: Governance, Risk, and Responsible Use**

**Core Philosophy**
"Can Claude do it?" and "Should we use Claude for it?" are two fundamentally different questions. Capability is about technical power; judgment is about suitability. The golden rule of this domain: **A human must stay in control of any decision that affects a person**.

---

**Lecture 1: Appropriate & Inappropriate Use Cases**

Just because Claude _can_ perform a task does not mean it _should_. The most dangerous risks arise from the wrong jobs executed well without oversight.

- **The Dividing Line**:
- **Appropriate (Claude Assists)**: Drafting replies for human sending, suggesting candidate shortlists, summarizing medical/legal cases, and recommending loan options for human approval.

- **Inappropriate (Claude Decides)**: Sending unreviewed messages to customers, making final hiring/firing decisions, delivering binding medical/legal rulings, or approving loans automatically.

- **The Pattern**: The more an automated system makes unreviewed decisions _about a person_, the less appropriate it is.

- **The Suitability Test (3 Mandatory Questions)**:

1. _Who is affected if it is wrong?_ (If a person's life or money is involved $\rightarrow$ exercise extreme caution).

2. _Can a human check it in time?_ (If no $\rightarrow$ do not automate).

3. _Is a human still deciding?_ (If no $\rightarrow$ wrong job for AI).

- **The Brilliant Intern Metaphor**: You would trust a capable intern to research options and draft a summary, but you would never allow them to sign a contract, fire an employee, or approve a loan alone.

---

**Lecture 2: Data Sensitivity, Privacy & Regulation**

What you put **in** matters as much as what you get **out**. In many enterprise workflows, the primary operational risk is not what the AI outputs, but what sensitive data was handed to it in the prompt.

- **Data Classification Levels**:
- **Public (Safe to use)**: Product names, public catalogs, standard market prices.

- **Internal (Use with care)**: Draft roadmaps, internal notes, team meeting summaries.

- **Sensitive (Strictly Protect)**:
- _Personal (PII)_: Names, home addresses, phone numbers, government IDs.

- _Financial_: Card numbers, bank details, salary records.

- _Health_: Medical notes, diagnostic records, physical conditions.

- _Secret_: Passwords, API keys, private business trade secrets.

- _Sensitivity Test_: _"Would this person be harmed or upset if this leaked?"_ If yes, treat as sensitive.

- **The Fix: Minimize and Mask**:
- **Minimize**: Supply only the exact context attributes needed to solve the task.

- **Mask**: Replace real identifiers with neutral placeholders (e.g., replace _"Rahul Verma, card 4521..."_ with _"A customer on order #1002"_).

- _Rule_: Claude cannot leak what you never provided in the prompt.

- **The Postcard Rule**: Sending sensitive data to external AI tools is like sending a postcard through the mail—anyone along the path can view it, and once sent, it cannot be recalled. Always adhere to data protection regulations (such as India's DPDP Act and Europe's GDPR).

---

**Lecture 3: Following AI Policies and Governance**

Individual good judgment cannot protect an entire organization; shared rules make safety an inherent property of the system rather than a lucky personal trait.

- **Why Policies Exist**: Without policy, team members guess, safety varies by person, and mistakes repeat. With policy, safety is standardized across the organization.

- **The 5 Core Areas of an Enterprise AI Policy**:

1. **Approved Uses**: What tasks AI can legitimately be used for.

2. **Banned Uses**: What workflows must never involve AI.

3. **Data Rules**: What data classes may be shared, and how.

4. **Review Rules**: What outputs require mandatory human inspection prior to release.

5. **Disclosure Rules**: When and how to inform users that AI was involved in creating the output.

- **Handling Gaps**: If a use case is neither explicitly permitted nor banned: **Pause**, **Ask the policy owner**, and **Default to caution**. Unwritten rules do not equal permission.

- **The Traffic System Metaphor**: Traffic lights exist not because individual drivers lack skill, but because thousands of diverse users must safely share the road.

---

**Lecture 4: Ethical Implications of AI Use**

Passing every legal and company rule still leaves the ultimate ethical test: **Is this actually right?**

- **The 4 Ethical Questions & Daily Habits**:
- **Is it honest?** $\rightarrow$ _Habit_: Inform people whenever AI is involved. Never allow a bot to pretend to be a real human agent (e.g., signing as _"Rahul, your human agent"_).

- **Is it fair?** $\rightarrow$ _Habit_: Actively audit outcomes across demographic groups to catch unintended bias (e.g., screening tools favoring specific names or locations).

- **Is it transparent?** $\rightarrow$ _Habit_: Disclose AI's role rather than disguising it.

- **Who could it harm?** $\rightarrow$ _Habit_: Evaluate downstream side effects on people before shipping any workflow.

- **Accountability**: _"The AI did it"_ is never a valid excuse—the human operating the tool owns the final output.

- **The Loudspeaker Metaphor**: AI acts as a giant amplifier. It scales helpful messages rapidly, but it equally scales mistakes, biases, and deception. The tool possesses no inherent ethics; the person holding it does.

---

**Domain 6 Summary Checklist**

1. **SUITABILITY** ──► Ask "Should we?" and keep humans in control of decisions affecting people[cite: 15].
2. **DATA GUARD** ──► Classify data, apply the Postcard Rule, minimize and mask all sensitive PII[cite: 15].
3. **GOVERNANCE** ──► Follow approved/banned policy rules; in ambiguous gaps, pause and escalate[cite: 15].
4. **ETHICS** ──► Ensure outputs are Honest, Fair, Transparent, and Harmless; own the outcome[cite: 15].
