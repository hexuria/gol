# AGENTS.md

## Engineering Principle

For algorithmically significant, stateful, concurrent, distributed, or orchestration-heavy work, do not jump directly from requirements to implementation.

Model the semantic core first.

Use formal methods as a design tool, not merely as post-implementation verification.

The default workflow is:

```text
Requirements
    ↓
Abstract model
    ↓
TLA+ exploration
    ↓
Counterexample analysis
    ↓
Architecture simplification
    ↓
Lean proofs where appropriate
    ↓
Implementation
    ↓
Tests derived from the model
```

The goal is not to maximize formalism.

The goal is to discover the smallest correct design before implementation complexity hides the underlying state machine.

---

# 1. When Formal Methods Are Required

Use formal modeling whenever the work contains one or more of the following:

- agent harnesses
- agent turn loops
- retry loops
- execution loops
- schedulers
- worker orchestration
- control planes
- execution planes
- queues
- event loops
- state machines
- concurrent operations
- distributed coordination
- cancellation
- resumable execution
- checkpoints
- retries
- leases
- locks
- ownership
- resource allocation
- transaction coordination
- workflow engines
- async tool execution
- streaming state
- task graphs
- DAG execution
- multi-step plans
- recursive algorithms
- algorithms where termination matters
- algorithms where optimality or minimality is claimed
- logic where duplicate execution would be harmful
- logic where race conditions could violate correctness

Do not require formal modeling for:

- trivial CRUD
- pure data mapping
- formatting
- UI-only changes
- simple configuration
- straightforward adapters
- obvious wrappers
- code whose correctness is locally evident and contains no meaningful state progression

Use engineering judgment.

---

# 2. Model the Semantic Core

Formal models must remain smaller than the production implementation.

Do not model infrastructure unless it affects correctness.

Avoid modeling details such as:

- HTTP
- JSON
- database drivers
- logging
- telemetry
- serialization
- framework-specific APIs
- UI rendering
- dependency injection
- filesystem layout

Instead model the underlying semantic entities.

For an agent harness, prefer concepts such as:

```text
Session
Turn
Step
Plan
ToolCall
ToolResult
Retry
Cancel
Suspend
Resume
Complete
Fail
```

The formal model should answer:

```text
What states can exist?

What transitions are allowed?

Who owns each piece of work?

What must never happen?

What must eventually happen?

What causes termination?

Which transitions may race?

Which transitions may repeat?

Which operations must be idempotent?
```

---

# 3. TLA+ Policy

Use TLA+ primarily for behavioral and temporal correctness.

TLA+ should be the first formal tool used for systems involving:

- state machines
- concurrency
- distributed state
- asynchronous execution
- orchestration
- cancellation
- retries
- queues
- scheduling
- ownership
- coordination
- event ordering

TLA+ is used to explore possible executions.

It should answer questions such as:

```text
Can this state ever be reached?

Can two operations race?

Can the same work execute twice?

Can the system deadlock?

Can the system livelock?

Can cancelled work resume?

Can a stale result overwrite newer state?

Can two workers own the same task?

Can a task disappear?

Can a turn complete while a child step is active?

Can retries continue forever?

Can an execution become permanently stuck?
```

---

# 4. Required TLA+ Model Structure

For substantial state machines, define:

## Variables

The minimal state necessary to model the system.

Example:

```text
sessionState
turnState
stepState
owner
attempt
pendingTools
completedTools
cancelled
```

## Initial State

Define valid starting states explicitly.

## Actions

Define every legal transition.

Example:

```text
StartSession
StartTurn
StartStep
RequestTool
ReceiveToolResult
RetryStep
CompleteStep
FailStep
CancelTurn
CompleteTurn
CompleteSession
```

## Next

Define the complete transition relation.

## Safety Properties

Properties that must always hold.

Examples:

```text
NoDuplicateExecution
SingleOwner
CompletedIsTerminal
CancelledIsTerminal
NoResultWithoutRequest
NoActiveChildAfterParentCompletion
AttemptsNeverDecrease
```

## Liveness Properties

Properties describing required progress.

Examples:

```text
AcceptedWorkEventuallyTerminates
RequestedToolEventuallyResolvesOrFails
ActiveTurnEventuallyCompletesFailsOrCancels
OwnedWorkIsEventuallyReleased
```

## Fairness

Add fairness assumptions only when justified.

Do not use fairness to hide a broken state machine.

---

# 5. TLA+ Model Checking

Run the model checker over bounded state spaces.

Use deliberately small bounds first.

Examples:

```text
Sessions = 1
Turns = 2
Steps = 3
Workers = 2
Retries = 2
Tools = 2
```

Small models are usually enough to expose structural bugs.

Increase the bounds only when necessary.

The purpose of TLC is not to simulate production scale.

The purpose is to exhaustively explore representative state combinations and interleavings.

---

# 6. Counterexamples Are Design Feedback

When TLA+ produces a counterexample:

Do not immediately patch the implementation.

Do not add arbitrary guards until the trace disappears.

First determine:

```text
Which assumption was wrong?

Which state should not exist?

Which transition is too permissive?

Which ownership rule is missing?

Which invariant was incomplete?

Can the architecture be simplified instead?
```

Prefer removing states or transitions over adding compensating logic.

Bad:

```text
race discovered
→ add more conditionals
→ add another lock
→ add another retry
```

Preferred:

```text
race discovered
→ identify ambiguous ownership
→ remove ambiguous transition
→ establish one owner
→ simplify model
```

---

# 7. Lean 4 Policy

Use Lean 4 when correctness depends on mathematical or structural properties independent of scheduling.

Lean is appropriate for:

- transition validity
- invariant preservation
- algorithm correctness
- termination
- recursive algorithms
- ordering guarantees
- data structure invariants
- idempotency
- equivalence
- protocol transformations
- deterministic planners
- reducers
- schedulers
- cost functions
- minimality proofs
- lower bounds

Lean should answer questions such as:

```text
Does every valid transition preserve validity?

Can an illegal state be constructed?

Does this recursive algorithm terminate?

Does this transformation preserve meaning?

Does retry preserve idempotency?

Does execution order preserve dependencies?

Is the selected solution actually minimal?

Can a lower-cost valid solution exist?
```

---

# 8. Lean Modeling Pattern

Prefer a small abstract model.

Example:

```lean
inductive State
  | idle
  | running
  | waiting
  | completed
  | failed
  | cancelled
```

Define transitions explicitly.

Example:

```lean
def step : State → Event → State
```

Define validity:

```lean
def Valid : State → Prop
```

Then prove:

```lean
theorem step_preserves_validity :
  Valid s →
  Allowed s e →
  Valid (step s e)
```

For termination, define a decreasing measure.

Example:

```lean
measure : State → Nat
```

Then establish that relevant transitions reduce it.

For idempotency:

```lean
apply e (apply e s) = apply e s
```

when the operation is required to be idempotent.

---

# 9. TLA+ vs Lean

Use the tools for different purposes.

## TLA+

Use TLA+ for:

```text
What executions are possible?
What interleavings exist?
Can something bad happen?
Can progress stop?
```

## Lean

Use Lean for:

```text
Why is this property always true?
Why does this algorithm terminate?
Why is this transformation correct?
Why is this result minimal?
```

A rough rule:

```text
TLA+ searches.

Lean proves.
```

They may be used together.

---

# 10. Shortest or Simplest Solution Policy

Do not interpret "shortest solution" as fewest lines of code.

Optimize the semantic machine.

When comparing alternative designs, prefer fewer:

1. independent states
2. state variables
3. legal transitions
4. exceptional transitions
5. mutable values
6. synchronization points
7. ownership transfers
8. retry paths
9. implicit side effects
10. execution branches

A smaller state machine is generally preferable to a smaller source file.

Example:

```text
Design A

17 states
48 transitions
6 retry modes
4 ownership states
3 synchronization mechanisms
```

versus:

```text
Design B

7 states
14 transitions
1 retry mechanism
1 ownership rule
1 synchronization mechanism
```

Prefer Design B if both satisfy the required properties.

---

# 11. Formal Optimization

When claiming that an algorithm or execution path is:

- shortest
- minimal
- optimal
- least expensive
- minimum-step
- minimum-state

define the optimization objective explicitly.

Example:

```text
cost(solution) =
    transition_count
  + exceptional_transition_count
  + mutable_state_count
  + synchronization_point_count
```

Or for path length:

```text
cost(path) = number_of_transitions(path)
```

Then establish:

```text
candidate satisfies specification
```

and either:

```text
bounded exhaustive search found no lower-cost candidate
```

or prove:

```text
∀ x,
  Valid x →
  cost candidate ≤ cost x
```

Do not claim global optimality from intuition.

---

# 12. Harness-Specific Requirements

For an agent harness, explicitly model at least:

```text
Session
Turn
Step
Execution
ToolCall
ToolResult
Retry
Cancellation
Completion
Failure
```

Depending on architecture, also model:

```text
Worker
Lease
Checkpoint
Branch
Plan
Queue
Resource
Parent
Child
```

The model must clearly establish ownership.

At any point, it should be possible to answer:

```text
Who owns this execution?

Can ownership change?

Who may complete it?

Who may retry it?

Who may cancel it?

What happens to late results?

What happens if the owner disappears?
```

---

# 13. Harness Invariants

At minimum, consider the following invariants.

## Exactly One Active Owner

An execution must not have multiple active owners unless explicitly designed for replicated execution.

## Terminal Means Terminal

After:

```text
Completed
Failed
Cancelled
```

the execution cannot transition back to an active state.

## No Duplicate Tool Completion

One logical tool invocation may produce at most one accepted terminal result.

Late or duplicate results must not cause duplicate progression.

## No Completion With Active Children

A parent cannot be considered complete while mandatory child work remains active.

## Retry Does Not Duplicate Logical Work

Retrying a logical operation must not result in multiple accepted completions.

## Cancellation Dominates Future Progress

Once cancellation becomes authoritative, stale events must not reactivate the operation.

## Ownership Is Explicit

No work may exist in an ambiguous state where multiple components believe another component owns it.

## Every Active State Has an Exit

Every active nonterminal state must have at least one defined path to:

```text
Completed
Failed
Cancelled
```

under documented assumptions.

---

# 14. Turn Loop Policy

Treat turn loops as state machines, not ordinary while-loops.

Avoid designs conceptually equivalent to:

```text
while true:
    inspect state
    maybe call model
    maybe call tool
    maybe retry
    maybe continue
```

unless the actual transition model is explicit.

Prefer:

```text
State
+
Event
→
Transition
→
New State
```

The turn engine should ideally behave like a reducer.

Conceptually:

```text
reduce(state, event) -> state
```

Side effects should be triggered from explicit transition decisions.

This makes the state machine easier to:

- test
- replay
- model
- reason about
- persist
- recover
- formally verify

---

# 15. Separate Decisions From Effects

Prefer separating:

```text
Decision Plane

State + Event
→ Decision
```

from:

```text
Effect Plane

Decision
→ Side Effect
```

Example:

```text
TurnState
+
ToolRequested
→
ExecuteTool(toolCall)
```

Then:

```text
ExecuteTool(toolCall)
→ external execution
```

and later:

```text
ToolCompleted(result)
→ reducer
```

Avoid embedding side effects directly inside complex state mutation logic.

---

# 16. Control Plane vs Execution Plane

For systems with a control plane and execution plane, enforce a clear boundary.

## Control Plane

Responsible for:

- desired state
- scheduling
- orchestration
- ownership
- lifecycle
- policy
- retries
- cancellation
- checkpointing
- dependency tracking

## Execution Plane

Responsible for:

- executing assigned work
- reporting results
- reporting failure
- heartbeats where required
- respecting cancellation
- returning deterministic execution metadata

The execution plane should not independently redefine workflow state.

The control plane remains authoritative.

---

# 17. Prefer Explicit State Over Hidden State

Avoid correctness-critical state encoded indirectly through:

- missing database rows
- nullable timestamps
- queue presence
- thread lifetime
- exception type
- network connection state
- boolean combinations
- implicit retries
- background task existence

Prefer explicit domain state.

Bad:

```text
started = true
finished = false
cancelled = false
error = null
```

Prefer:

```text
ExecutionState::Running
```

Illegal combinations should ideally be impossible to represent.

---

# 18. Reduce Boolean State Explosion

Avoid multiple booleans representing mutually exclusive lifecycle state.

Bad:

```text
running
waiting
retrying
cancelled
completed
failed
```

This creates invalid combinations.

Prefer:

```text
enum State {
    Running,
    Waiting,
    Retrying,
    Cancelled,
    Completed,
    Failed,
}
```

Make illegal states structurally unrepresentable where practical.

---

# 19. Retry Policy

Retries must be modeled explicitly.

Define:

```text
what is being retried
why it may retry
maximum retries
retry ownership
idempotency requirements
what state survives retry
what state resets
what happens after exhaustion
```

Avoid generic catch-and-retry loops around large sections of orchestration logic.

Retry the smallest safe semantic operation.

---

# 20. Cancellation Policy

Cancellation is a protocol, not merely a boolean.

Model:

```text
CancelRequested
CancelAcknowledged
Cancelled
```

when the distinction matters.

Explicitly define behavior for events racing with cancellation.

Examples:

```text
ToolResult arrives after CancelRequested

Worker completes while cancellation is propagating

Retry scheduled while cancellation occurs

Child starts immediately before parent cancellation
```

Determine which event wins and why.

---

# 21. Idempotency Policy

Any operation that may be:

- retried
- replayed
- redelivered
- resumed
- duplicated

must define idempotency behavior.

Examples:

```text
tool calls
task dispatch
workflow transitions
job completion
billing
external mutations
message delivery
checkpoint commits
```

Where possible, use stable logical operation identifiers.

Do not rely solely on "this normally executes once."

---

# 22. Termination

Loops must have explicit termination semantics.

For each loop identify:

```text
termination condition
progress measure
maximum attempts if bounded
external assumptions required for progress
```

For deterministic loops, prove or clearly establish a decreasing measure where feasible.

Examples:

```text
remaining tasks decreases

unresolved dependencies decreases

retry budget decreases

search frontier decreases
```

Do not create unbounded retry or repair loops without an explicit reason.

---

# 23. Liveness vs Safety

Keep them separate.

## Safety

Something bad never happens.

Examples:

```text
a task never has two owners

completed tasks never execute again

a parent never completes before required children
```

## Liveness

Something good eventually happens.

Examples:

```text
accepted work eventually finishes or fails

locks are eventually released

waiting tasks eventually become schedulable or terminal
```

Passing safety checks does not imply liveness.

Verify both when relevant.

---

# 24. Search for Smaller State Machines

After the model passes, attempt simplification before implementation.

Ask:

```text
Can two states be merged?

Is this intermediate state observable or necessary?

Can ownership transfer be removed?

Can retries reuse the same lifecycle?

Can this branch become data instead of state?

Can this synchronization point disappear?

Can this transition be eliminated?

Can one authoritative component replace coordination?
```

Re-run the model after simplification.

The preferred architecture is the smallest architecture that still satisfies the required behavior.

---

# 25. Implementation Correspondence

Production code must remain recognizably related to the formal model.

Avoid producing a clean formal model followed by unrelated implementation logic.

Maintain an approximate mapping:

```text
Formal State
↔
Production State

Formal Action
↔
Production Transition

Formal Invariant
↔
Production Constraint/Test
```

If production requirements invalidate the model, update the model.

Do not silently diverge.

---

# 26. Tests Derived From Formal Models

Formal verification does not replace testing.

Generate tests from important model behaviors.

For every discovered TLA+ counterexample that results in a design fix, create a regression test when practical.

Useful test classes include:

- transition tests
- invariant tests
- replay tests
- property tests
- concurrency tests
- cancellation races
- duplicate delivery tests
- retry tests
- crash recovery tests

The formal model and the tests should reinforce each other.

---

# 27. Property-Based Testing

When formal proof is excessive but ordinary unit tests are weak, use property-based testing.

Good candidates:

```text
reducers
serializers
planners
schedulers
dependency resolution
state transitions
retry logic
idempotent operations
```

Properties should mirror formal invariants where possible.

---

# 28. Failure Injection

For concurrent and distributed systems, intentionally test:

- delayed results
- duplicated results
- reordered events
- dropped events
- worker death
- process restart
- cancellation races
- retry races
- stale checkpoints
- stale ownership
- partial failure
- network timeout

Assume production will eventually produce unusual ordering.

Design accordingly.

---

# 29. Required Output Before Major Implementation

For significant algorithmic or stateful work, provide the following before writing the full production implementation.

## Model

Describe:

```text
states
events
transitions
ownership
terminal states
```

## Properties

List:

```text
safety invariants
liveness properties
termination requirements
idempotency requirements
```

## TLA+ Findings

Report:

```text
counterexamples
deadlocks
livelocks
bad interleavings
unreachable states
unnecessary transitions
```

If no issue was found, state the explored bounds.

## Lean Findings

When Lean is used, report:

```text
proved properties
assumptions
unproved properties
limitations
```

## Simplification

State whether any:

```text
state
transition
branch
lock
retry mode
ownership transfer
intermediate representation
```

can be eliminated.

## Implementation Mapping

Explain how formal concepts map to production concepts.

Then implement.

---

# 30. Do Not Over-Formalize

Formal methods are not an excuse for slowing down obvious work.

The purpose is high-leverage reasoning.

Use the smallest formal model capable of answering the important correctness questions.

It is acceptable to formally model only the critical semantic core.

Example:

```text
Production system:
20,000 lines

Formal state model:
150 lines

Lean core:
80 lines
```

That may be entirely appropriate.

---

# 31. Escalation Levels

Use the following rough hierarchy.

## Level 0 — Ordinary Code

Use normal tests.

Appropriate for:

- simple utilities
- formatting
- straightforward CRUD
- stateless adapters

## Level 1 — State Diagram

Document states and transitions.

Appropriate for:

- simple lifecycle logic
- bounded local workflows

## Level 2 — Property Tests

Add generative/property-based tests.

Appropriate for:

- reducers
- planners
- deterministic algorithms

## Level 3 — TLA+

Required when meaningful concurrency, retries, cancellation, scheduling, ownership, or distributed state appears.

## Level 4 — Lean

Use for critical algorithmic correctness, termination, structural invariants, or proofs of minimality.

## Level 5 — TLA+ + Lean

Use both for critical systems where:

```text
runtime interleavings matter
+
mathematical correctness matters
```

Agent harnesses and orchestration engines will often fall into this category.

---

# 32. Default Harness Workflow

For a new harness component, use this sequence:

```text
1. Write the semantic requirements.

2. Identify state.

3. Identify events.

4. Define terminal states.

5. Define ownership.

6. Define transitions.

7. Define safety invariants.

8. Define liveness requirements.

9. Build the TLA+ model.

10. Run TLC.

11. Inspect counterexamples.

12. Remove unnecessary state and transitions.

13. Repeat until the model is structurally clean.

14. Identify mathematically critical algorithms.

15. Model those algorithms in Lean.

16. Prove the important properties.

17. Implement the production version.

18. Add regression tests from counterexamples.

19. Add property-based tests from invariants.

20. Verify implementation/model correspondence.
```

---

# 33. Example Harness State Machine

A simple harness might begin with:

```text
Idle
  ↓
Planning
  ↓
Running
  ├──→ WaitingForTool
  │          ↓
  │      ToolResult
  │          ↓
  └────── Running
  │
  ├──→ Retrying
  │        ↓
  │      Running
  │
  ├──→ Suspended
  │        ↓
  │      Running
  │
  ├──→ Cancelled
  ├──→ Failed
  └──→ Completed
```

Do not assume this is the correct machine.

Attempt to reduce it.

For example, ask whether:

```text
Planning must be a lifecycle state

Retrying must be a state instead of transition metadata

Suspended differs semantically from Waiting

ToolResult requires a distinct lifecycle state

Running can represent multiple substates through explicit pending work
```

Fewer states are preferable when semantics remain clear.

---

# 34. Questions Every Harness Design Must Answer

Before considering the design complete, answer:

```text
What is the source of truth?

Who owns execution?

What identifies one logical execution?

What makes an operation idempotent?

How is duplicate execution detected?

What happens when workers crash?

What happens when the controller crashes?

What happens to in-flight work after restart?

How are stale results identified?

How does cancellation propagate?

Can cancellation race completion?

What happens when both happen?

How are retries bounded?

Which errors are retryable?

Can retries duplicate external effects?

How are child operations tracked?

Can parents complete before children?

What guarantees progress?

What prevents infinite loops?

What state must be persisted?

What state can be reconstructed?

Which operations are replay-safe?
```

If these answers are unclear, the architecture is not complete.

---

# 35. Bias Toward Deterministic Cores

Where practical, design orchestration logic as deterministic state transitions.

Prefer:

```text
State + Event → NewState + Effects
```

over:

```text
large async function
with hidden mutations
network calls
exceptions
retries
and branching control flow
```

A deterministic core enables:

- replay
- simulation
- formal modeling
- property testing
- debugging
- auditing
- deterministic recovery

This is strongly preferred for control-plane logic.

---

# 36. Event Replay

When practical, architecture should permit replaying state transitions from events.

Replayability is valuable for:

- debugging
- recovery
- simulation
- reproducing races
- model comparison
- regression testing

Do not require event sourcing everywhere.

But favor transition designs that do not depend on invisible mutable context.

---

# 37. Every Transition Must Have a Reason

Avoid transitions created solely because implementation structure happens to produce them.

Each state and transition must correspond to a meaningful domain distinction.

If removing a state changes nothing observable about correctness, ownership, or required behavior, consider removing it.

---

# 38. Prefer One Authority

Distributed coordination complexity often grows from competing authorities.

Where practical, establish one authoritative owner for:

- workflow state
- task assignment
- cancellation
- completion
- retry decisions

Replicated execution may exist, but authority should remain explicit.

---

# 39. Avoid Accidental Distributed Transactions

Do not create workflows where correctness implicitly requires several independent systems to update atomically unless that requirement is intentionally designed.

Prefer protocols involving:

- idempotency
- monotonic state
- reconciliation
- explicit acknowledgement
- leases
- retries

and model those protocols explicitly.

---

# 40. Prefer Monotonic State

Where possible, use state that moves forward rather than oscillating.

Good:

```text
Pending
→ Running
→ Completed
```

Riskier:

```text
Running
→ Pending
→ Running
→ Pending
```

If backward transitions exist, justify and model them carefully.

Monotonic state generally reduces the number of possible interleavings.

---

# 41. Final Engineering Rule

For critical algorithmic components:

```text
Do not ask:

"Does this code look correct?"

Ask:

"What are all possible states?"

"What executions are possible?"

"What properties must always hold?"

"What must eventually happen?"

"Can we remove a state or transition?"

"Can we prove the critical property?"

"What counterexample would break this design?"
```

The preferred solution is not the cleverest implementation.

The preferred solution is the smallest understandable state machine whose critical properties can be demonstrated.

---

# 42. Rust API and Test Practice

This section is gol project practice. It sits on top of the formal-methods rule.

Public types that other crates construct use a typestate builder.

- Required inputs are type states. They are not optional fields checked at runtime.
- `PhantomData` marks the builder state. An illegal build order does not compile.
- Callees accept the finished type.

Tests are written before the behavior they describe.

- Derive the assertion from a formal trace or an invariant.
- Call the public API the way a dependent crate would.
- Assert a literal outcome.

Provider HTTP tests use wiremock.

Hot paths have Criterion benchmarks. The fold and the harness reducer are the first two. A benchmark is a regression signal. It is not a claim that the algorithm is optimal.

---

# 43. Bend Policy

Bend is the language for verified pure workflow logic. It does not replace TLA+ or Lean, and it is not a place to rewrite the Rust harness.

Run `bend guide` before a non-trivial Bend change. Current upstream docs win when an older Bend or HVM1 article disagrees.

Laws live in `LAWS.bend`. Proofs live in `PROOF.bend`. Run the proofs before committing. `./scripts/verify-bend.sh` is the gate: syntax, types, laws, proofs, and Rust compatibility. A proof that fails means stop. Never weaken a law to make a proof pass. When a law fails, decide whether the implementation or the spec is wrong.

Bend is for pure verified computation. Parallelize only independent computation. Effects stay in Rust. A passed proof is not a sandbox: Rust keeps the timeout, the kill, and the output limit.

Benchmark a Bend claim against idiomatic Rust. Do not claim a speedup the boundary benchmark did not measure.

---

# 44. Lock Files

`Cargo.lock` and `coworker/bun.lock` are committed. The workspace ships binaries, CI builds what the lock names, and `cargo deny` audits those exact versions.

Only the package manager writes a lock file: `cargo update -p <crate>`, `cargo generate-lockfile`, or `bun install`. The result is pushed with git as one ordinary commit next to the manifest change that caused it.

Never split a lock file into pieces, write it by hand, or have a workflow rebuild it. No workflow commits or pushes to a branch. An agent that cannot push a large file from a git checkout does not change dependencies.
