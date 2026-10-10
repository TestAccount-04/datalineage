# DaLa — Interactive In-App Guide

## What are we building?

DaLa is a character inside the Data Lineage application that **travels with the user**, points at UI elements, and explains each part with short messages.

The experience should feel like an interactive story:

```text
User opens Story Mode
        ↓
DaLa appears
        ↓
"Hey! I'm DaLa 👋"
        ↓
DaLa moves to Incident
        ↓
"Let's find out what broke."
        ↓
DaLa moves to Evidence
        ↓
"We need three clues."
        ↓
DaLa moves to Lineage Graph
        ↓
"Now let's follow the data."
        ↓
DaLa moves to Impact Simulator
        ↓
"Let's see what this change affects."
```

DaLa should **not** just be a chatbot fixed in the corner. The character should move through the application.

---

# 1. Architecture

```text
                         USER
                          │
                          ▼
                  ┌───────────────┐
                  │   Story Mode  │
                  └───────┬───────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │ DaLa Guide      │
                 │ Engine          │
                 └────────┬────────┘
                          │
            ┌─────────────┼─────────────┐
            │             │             │
            ▼             ▼             ▼
       Character       Message       Tour State
       Controller      Manager       Controller
            │             │             │
            └─────────────┼─────────────┘
                          │
                          ▼
                  Current UI Target
                          │
                          ▼
                 Scroll + Highlight
                          │
                          ▼
                    DaLa moves
                          │
                          ▼
                  Message appears
                          │
                          ▼
                   User continues
```

---

# 2. Main components

## DaLa Character

Responsible for:

- Character rendering
- Expressions
- Floating animation
- Movement
- Speech bubble

Suggested states:

```text
idle
welcome
explaining
thinking
pointing
celebrating
warning
```

## Movement Controller

Finds the target UI element and moves DaLa beside it.

Do **not** use fixed coordinates.

Bad:

```javascript
x = 800;
y = 400;
```

Better:

```javascript
const target = document.querySelector(
  "[data-dala='lineage-graph']"
);

const rect = target.getBoundingClientRect();
```

Then calculate the best position around the target.

## Message Manager

Controls short contextual messages.

Example:

```javascript
{
  target: "[data-dala='lineage-graph']",
  message: "This is your data map."
}
```

Keep messages short — normally 1–2 sentences.

## Tour Controller

Controls:

```text
Next
Back
Skip
Replay
Pause
Resume
```

It also knows which step the user has completed.

---

# 3. Give UI elements stable DaLa identifiers

Add stable attributes to important elements:

```jsx
<section data-dala="incident">
```

```jsx
<div data-dala="static-evidence">
```

```jsx
<div data-dala="metadata-evidence">
```

```jsx
<div data-dala="runtime-evidence">
```

```jsx
<div data-dala="lineage-graph">
```

```jsx
<button data-dala="impact-simulator">
```

Prefer this:

```javascript
"[data-dala='lineage-graph']"
```

over fragile selectors such as:

```javascript
".card:nth-child(4)"
```

---

# 4. Recommended frontend architecture

```text
src/
│
├── dala/
│   ├── DaLaCharacter.jsx
│   ├── DaLaBubble.jsx
│   ├── DaLaController.jsx
│   ├── DaLaSpotlight.jsx
│   ├── DaLaAnimations.jsx
│   └── DaLaPositioner.js
│
├── story/
│   ├── dalaStory.js
│   ├── storyController.js
│   ├── storyState.js
│   └── storyActions.js
│
└── ...
```

Keep the character logic separate from Story Mode.

---

# 5. Story configuration

Don't hardcode the entire story inside `DaLaCharacter.jsx`.

Use configuration:

```javascript
const dalaStory = [
  {
    id: "welcome",
    target: "[data-dala='incident']",
    message: "Hey! I'm DaLa. Let's investigate why the dashboard broke.",
    action: "scroll"
  },

  {
    id: "static-evidence",
    target: "[data-dala='static-evidence']",
    message: "First, let's inspect the source code.",
    action: "highlight"
  },

  {
    id: "metadata-evidence",
    target: "[data-dala='metadata-evidence']",
    message: "Now let's verify the database metadata.",
    action: "highlight"
  },

  {
    id: "runtime-evidence",
    target: "[data-dala='runtime-evidence']",
    message: "Runtime evidence tells us what actually happened.",
    action: "highlight"
  },

  {
    id: "lineage-graph",
    target: "[data-dala='lineage-graph']",
    message: "This graph shows how data moves through the system.",
    action: "highlight"
  },

  {
    id: "impact",
    target: "[data-dala='impact-simulator']",
    message: "Let's see what could break if we change this.",
    action: "highlight"
  }
];
```

This makes it easy to change the story without changing DaLa itself.

---

# 6. DaLa movement

Use an animation library.

Recommended:

**Motion for React**

Basic concept:

```jsx
<motion.div
  animate={{
    x: targetX,
    y: targetY
  }}
  transition={{
    type: "spring",
    stiffness: 90,
    damping: 18
  }}
>
  <DaLaCharacter />
</motion.div>
```

The actual implementation should calculate `targetX` and `targetY` from the target element.

Flow:

```text
Find target
    ↓
getBoundingClientRect()
    ↓
Calculate best position
    ↓
Animate DaLa
```

---

# 7. Recommended tour library

## Driver.js

Use it for:

- Spotlight
- Highlighting
- Popovers
- Guided steps
- Tour lifecycle

https://driverjs.com/

Recommended architecture:

```text
             Driver.js
                 │
        Highlight / Spotlight
                 │
                 ▼
             UI Target
                 ▲
                 │
          DaLa Controller
                 │
                 ▼
          DaLa Character
```

### Alternative: Shepherd.js

https://www.shepherdjs.dev/

Good for customizable guided tours.

| Feature | Driver.js | Shepherd.js |
|---|---|---|
| Simple tour | Excellent | Excellent |
| Spotlight | Excellent | Good |
| Customization | Good | Excellent |
| Easy setup | Excellent | Good |
| DaLa integration | Excellent | Excellent |

**Recommendation: Driver.js + custom DaLa movement.**

---

# 8. Story Mode journey

## Step 1 — Incident

DaLa appears near the incident.

```text
"Hey! I'm DaLa 👋
Let's investigate what broke."
```

Then:

```text
"Something changed upstream.
Let's find out what."
```

---

## Step 2 — Evidence

DaLa travels to the evidence cards.

### Static analysis

> "First, let's inspect the source code."

### Metadata

> "Now let's verify the database structure."

### Runtime

> "Finally, let's check what actually happened at runtime."

After all three:

> "Great! We have three clues."

DaLa should wait for the user to actually collect each clue.

---

# 9. Step 3 — Dataset Identity

DaLa travels to dataset identity.

```text
"Here's the tricky part."
```

Then:

```text
"We found several datasets called CUSTOMER."
```

Then:

```text
"Same name doesn't always mean the same dataset."
```

Highlight:

```text
DEV.CUSTOMER
TEST.CUSTOMER
PROD.CUSTOMER
```

Then:

```text
"We need the full identity to know which one we're talking about."
```

---

# 10. Step 4 — Lineage Graph

DaLa moves to the graph.

```text
"Welcome to the data map."
```

Then:

```text
"Every node represents something in the system."
```

Highlight:

```text
Table
Method
Pipeline
Column
Dashboard
```

Then:

```text
"And every connection tells us how data moves."
```

---

# 11. Upstream

DaLa points to a source table:

```text
CUSTOMER
    ↓
Risk Service
    ↓
CUSTOMER_RISK
```

Message:

> "This is upstream — where did this data come from?"

---

# 12. Downstream

DaLa points to the output:

```text
CUSTOMER_RISK
      ↓
dbt
      ↓
Power BI
      ↓
Dashboard
```

Message:

> "This is downstream — where does this data go next?"

---

# 13. Column lineage

DaLa zooms into a column:

```text
customer.credit_score
          ↓
calculateRisk()
          ↓
risk_score
          ↓
customer_risk.risk_score
```

Message:

> "We can go deeper than tables."

Then:

> "Column lineage lets us follow individual fields through transformations."

---

# 14. Impact analysis

DaLa travels to the impact simulator.

```text
"Now let's break something."
```

User changes a column.

The application highlights:

```text
Column
   ↓
Method
   ↓
Table
   ↓
Pipeline
   ↓
BI Dataset
   ↓
Dashboard
```

DaLa:

> "We found the downstream systems that could be affected."

---

# 15. Make DaLa react to user actions

Don't make the experience:

```text
Next
Next
Next
Next
```

Instead:

```text
DaLa asks user to do something
        ↓
User performs action
        ↓
Application detects action
        ↓
DaLa reacts
        ↓
Next step
```

Example:

```text
DaLa:
"Click the CUSTOMER table."
```

User clicks it.

DaLa:

```text
"Nice! Now we're looking at the source dataset."
```

This makes the tour feel interactive.

---

# 16. Story state

Maintain state such as:

```javascript
const storyState = {
  currentStep: "lineage-graph",

  completedSteps: [
    "welcome",
    "static-evidence",
    "metadata-evidence"
  ],

  selectedNode: "customer",

  evidenceCollected: 3,

  completed: false
};
```

This lets DaLa understand where the user is in the story.

---

# 17. DaLa should understand current application context

Keep context such as:

```text
Current route:
 /story

Current section:
 lineage

Selected node:
 CUSTOMER

Selected level:
 table
```

Later, AI DaLa can use this context to give relevant answers.

---

# 18. Version 1 should NOT use AI

First build DaLa as a deterministic guide.

```text
User action
     ↓
Story Controller
     ↓
DaLa Controller
     ↓
Target element
     ↓
Character movement
     ↓
Predefined message
```

Advantages:

- Predictable
- Fast
- No API cost
- No hallucination
- Easy to debug
- Consistent story

Do not use an LLM to decide where DaLa moves.

---

# 19. Version 2 — AI DaLa

After the scripted version works well, add an interactive assistant.

Example:

User:

> "Why does this dashboard depend on CUSTOMER?"

DaLa gets:

```text
Current UI
+
Selected node
+
Lineage graph
+
Evidence
```

Then retrieves:

```text
CUSTOMER
   ↓
RiskService
   ↓
CUSTOMER_RISK
   ↓
dbt model
   ↓
Power BI dataset
   ↓
Dashboard
```

DaLa responds:

> "This dashboard depends on CUSTOMER through RiskService and the CUSTOMER_RISK transformation."

Then DaLa highlights the path.

---

# 20. AI DaLa architecture

```text
                     USER
                      │
                      ▼
               "Why is this
                connected?"
                      │
                      ▼
                DaLa AI Agent
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
       Current      Lineage     Evidence
       UI State      Graph       Store
          │           │           │
          └───────────┼───────────┘
                      ▼
                 Explanation
                      │
                      ▼
                DaLa Response
                      │
                      ▼
               Highlight Path
```

---

# 21. Don't turn DaLa into a generic chatbot

Avoid:

```text
┌──────────────────────┐
│        DaLa          │
│                      │
│ Ask me anything      │
└──────────────────────┘
```

Instead:

```text
DaLa = Guide + Storyteller + Contextual Assistant
```

DaLa should know:

```text
Where the user is
What the user is looking at
What step they completed
What node is selected
What the lineage graph says
What evidence exists
```

---

# 22. Responsive behavior

DaLa must work on:

- Desktop
- Laptop
- Tablet
- Smaller screens

Never use:

```text
left: 700px
top: 300px
```

Calculate dynamically:

```text
Target bounding box
        ↓
Viewport dimensions
        ↓
Best DaLa position
        ↓
Keep inside viewport
```

If the target is near the right edge:

```text
Place DaLa on the left
```

If the target is near the bottom:

```text
Place DaLa above
```

---

# 23. Collision handling

DaLa should choose one of:

```text
LEFT
RIGHT
TOP
BOTTOM
```

depending on available space.

This prevents the character from covering important content.

---

# 24. Animation states

Recommended:

```text
IDLE
 ↓
WELCOME
 ↓
MOVE
 ↓
POINT
 ↓
EXPLAIN
 ↓
WAIT
 ↓
MOVE
 ↓
CELEBRATE
```

Examples:

- **Idle:** small floating animation
- **Move:** fly toward target
- **Explain:** point toward target
- **Thinking:** eyes/antenna animation
- **Warning:** alert animation
- **Celebrate:** jump/wave/spin

Keep animations subtle.

---

# 25. Speech bubble

Example:

```text
┌───────────────────────────────────┐
│ DaLa                              │
│                                   │
│ "This is your data map."          │
│                                   │
│                 Back     Next →   │
└───────────────────────────────────┘
```

For interactive steps:

```text
┌───────────────────────────────────┐
│ DaLa                              │
│                                   │
│ "Click the CUSTOMER table."       │
│                                   │
│          Waiting for you...       │
└───────────────────────────────────┘
```

---

# 26. Recommended technology stack

### Character

Start with:

```text
SVG / PNG
```

Later:

```text
Lottie
Rive
3D model
```

### Animation

```text
Motion for React
```

### Tour / spotlight

```text
Driver.js
```

Alternative:

```text
Shepherd.js
```

### Application

Use your existing:

```text
React
```

### State

Use existing state management.

If needed:

```text
Zustand
```

or:

```text
React Context
```

### AI later

Potential:

```text
OpenAI
Gemini
Claude
```

---

# 27. Implementation phases

## Phase 1 — Character

Build:

```text
DaLaCharacter
DaLaBubble
DaLaAnimations
```

Features:

- Idle
- Wave
- Explain
- Celebrate
- Speech bubble

## Phase 2 — Movement

Build:

```text
DaLaController
```

Features:

- Find target
- Scroll target
- Calculate position
- Move DaLa
- Handle viewport boundaries

## Phase 3 — Story Mode

Connect DaLa to:

```text
Incident
Evidence
Dataset Identity
Lineage Graph
Impact Simulator
```

## Phase 4 — Interactive actions

Add:

```text
Click target
Collect evidence
Select node
Explore upstream
Explore downstream
Run simulation
```

DaLa reacts to each action.

## Phase 5 — AI DaLa

Add:

```text
Ask DaLa
```

DaLa can answer using:

```text
Current UI
Lineage Graph
Evidence
Dataset metadata
Documentation
```

---

# 28. Suggested folder structure

```text
src/
│
├── dala/
│   ├── DaLaCharacter.jsx
│   ├── DaLaBubble.jsx
│   ├── DaLaController.jsx
│   ├── DaLaSpotlight.jsx
│   ├── DaLaAnimations.jsx
│   ├── DaLaPositioner.js
│   └── dala.css
│
├── story/
│   ├── dalaStory.js
│   ├── storyController.js
│   ├── storyState.js
│   └── storyActions.js
│
├── lineage/
│   ├── graph/
│   ├── evidence/
│   └── impact/
│
└── ...
```

---

# 29. Important engineering rules

### Rule 1

DaLa should never block the application.

Always provide:

```text
Skip
Close
Explore manually
```

### Rule 2

Never depend on fixed screen coordinates.

### Rule 3

DaLa should not cover important information.

### Rule 4

Keep messages short.

### Rule 5

Don't overanimate.

### Rule 6

Don't use an LLM for deterministic navigation.

Use:

```text
Code → State → Action
```

not:

```text
LLM → Guess where to move
```

---

# 30. Final architecture

```text
                         USER
                           │
                           ▼
                    ┌─────────────┐
                    │  Story Mode │
                    └──────┬──────┘
                           │
                           ▼
                ┌────────────────────┐
                │   Story Controller │
                └─────────┬──────────┘
                          │
                          ▼
                ┌────────────────────┐
                │   DaLa Controller  │
                └─────────┬──────────┘
                          │
              ┌───────────┼───────────┐
              │           │           │
              ▼           ▼           ▼
          Position     Message     Animation
          Engine       Manager      Manager
              │           │           │
              └───────────┼───────────┘
                          │
                          ▼
                    DaLa Character
                          │
                          ▼
                      Target UI
                          │
                          ▼
                  Spotlight / Scroll
                          │
                          ▼
                    User Action
                          │
                          ▼
                   Story continues
```

Later:

```text
                    DaLa AI
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
    UI Context     Lineage Graph    Evidence
        │              │              │
        └──────────────┼──────────────┘
                       ▼
                  Explanation
                       │
                       ▼
                 Highlight Path
```

---

# 31. Final product experience

The user should not feel:

> "I'm reading documentation."

They should feel:

> "DaLa is taking me through the system."

Example:

```text
                    DaLa 🤖
                       │
                       ▼
              "Something broke."
                       │
                       ▼
              "Let's investigate."
                       │
                       ▼
          ┌────────────────────┐
          │   Static Evidence  │
          └────────────────────┘
                       │
                       ▼
          "Here's what the code says."
                       │
                       ▼
          ┌────────────────────┐
          │  Metadata Evidence │
          └────────────────────┘
                       │
                       ▼
          "Let's verify the data."
                       │
                       ▼
          ┌────────────────────┐
          │   Lineage Graph    │
          └────────────────────┘
                       │
                       ▼
          "Follow the data."
                       │
                       ▼
          ┌────────────────────┐
          │ Impact Simulation  │
          └────────────────────┘
                       │
                       ▼
          "Here's what could break."
                       │
                       ▼
                     🎉
              "Mystery solved!"
```

## Final recommendation

Build **DaLa as a custom animated character**, using:

```text
React
+
Motion for React
+
Driver.js
+
Story Controller
+
Your existing Lineage UI
```

Start completely scripted.

Then add AI after the experience is stable.

The long-term vision is:

```text
             DaLa
              │
      ┌───────┴────────┐
      │                │
   Guide            Assistant
      │                │
      ▼                ▼
 Story Mode       User Questions
      │                │
      └───────┬────────┘
              ▼
        Lineage Platform
              │
      ┌───────┼────────┐
      ▼       ▼        ▼
     Code   Metadata  Runtime
              │
              ▼
         Evidence Graph
```

The key differentiator is that DaLa understands the application itself.

It doesn't merely say:

> "Click here."

It can eventually say:

> **"You're looking at `CUSTOMER_RISK`. Follow me — I'll show you exactly where this data came from and which dashboards depend on it."**
