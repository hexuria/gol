# Bend in gol

Bend 2.0.28 is a pure, terminating language with executable proofs. gol keeps the harness in Rust. Bend expresses the whole workflow IR (tool calls with input, agent spawns, sequences, `on_counter`, `complete`, `fail`) and emits one reference program in the v2 encoding, with laws about it. A proof check is not permission to touch the disk, the network, or a tool.

The sources that were checked for this note are `bend guide`, `bend guide effects`, `bend --help`, and `bend base --types` from the 2.0.28 binary, plus the files under `experiments/bend/`. Older Bend and HVM1 writeups are ignored where they disagree with that guide.

## What Bend is today

Bend 2.0.28 is one dynamically linked CLI, `bend`, 82,822,344 bytes, linked to libc. The language is Python-shaped and Haskell/Lean-shaped underneath. Programs are affine by default, `Data` values may be copied, and recursion must shrink a matched argument or the def is outside the proof guarantee (`@unsafe`). There is one universe (`Type : Type`) and no positivity check. Live code must terminate. Dead code (types, erased arguments, equations) is not evidence.

The standard library is Base: `Nat`, `U32`, `F32`, `Bool`, `Maybe`, `Result`, `List`, `Array`, `String`, `Map`, and an `IO` monad for files, sockets, windows, audio, and, since 2.0.28, child processes (`Process.run`). There is no `I64` and no JSON type. `Fail` is already a Base constructor, so a gol command cannot use that name. The workflow model uses `GolHalt` and prints the token `fail`.

Parallelism is a fork-join annotation (`a b = f(x) g(y)`, and `!` for the GPU). The JavaScript target runs those calls sequentially. gol does not use Bend parallelism.

Laws are types. Proofs are defs. There are no tactics. `{==}` is reflexivity after computation, `%e : P` rewrites, and `?TODO` leaves a hole. `bend PROOF.bend` fails while any imported law is open. A `PROOF.bend` next to a `LAWS.bend` is rejected unless it imports `./LAWS.bend`.

## Compile and execute path

`bend file.bend` typechecks, then:

- a `main` that returns `IO` is compiled and the event loop runs it
- a `main` that returns a value is normalized by the checker and printed
- a file with no `main` is only checked

`bend file.bend --check-only` checks and runs nothing. `bend file.bend -o out` emits a native binary via clang. `-o out.c` and `-o out.js` emit those sources. The generated C is one program: CPU code and, when `!` is used, the GPU kernel. clang 18 on this machine linked a small IO program. The binary's own flags are `--threads` and `--gpu`. Those flags do not apply to the checker.

A value `main` of type `String` prints a Bend string literal, quotes included. `workflow.bend` prints:

```text
"v2 tool.736561726368.71 tool.636f756e746572. spawn.68656c706572.7a65726f complete end seq seq fail on_counter end seq seq"
```

Confirmed on 2.0.27 and again on 2.0.28: an `IO` main that calls `IO.print` prints when run, and prints nothing under `--check-only`. A `def` that returns `String` cannot `import` a `.c` file. Foreign imports typecheck only on a def whose result is `IO(...)` directly.

## Embed in Rust, or not

Not as a library. The release is one executable. There is no `.so`, no C header for the compiler, and `bend --help` has no compile-server or FFI command. `bend guide effects` says the C effect ABI is the runtime's internals, a release may rename any of it, and there is no ABI promise.

Calling Bend through native FFI, or linking the generated C into the harness, would fork that runtime or depend on names the next release can change. gol does not do either.

## Process boundary

The boundary, smallest one that exists today:

1. Native FFI: not shipped.
2. A generated native artifact: `bend -o` works, and a hello binary started in about a millisecond, but it is a whole program with its own `main`. Loading it into Rust is the unstable C ABI above.
3. A stable compiler API: the CLI is the API. There is no separate library protocol.
4. Subprocess: this is the boundary `workflow-bend` uses.

`compile` copies `workflow.bend`, `evals.bend`, `LAWS.bend`, and `PROOF.bend` into a new private temp directory (mode 0700, random name, files written new at mode 0600). It refuses a symlink and a file over 64KiB. It then runs:

```text
unshare -r -n -- env -i BEND_NO_TELEMETRY=1 bend version
unshare -r -n -- env -i BEND_NO_TELEMETRY=1 bend PROOF.bend --check-only
unshare -r -n -- env -i BEND_NO_TELEMETRY=1 bend workflow.bend
```

The tests also run `bend evals.bend` in the same sandbox, with a 120-second limit instead of 30 (see "How laws and proofs are checked"). `verify-bend.sh` runs the checker on the repo tree with no time limit.

`unshare -r -n` is a new user namespace and a new network namespace. `unshare -n` alone is not permitted here; the user namespace is what makes the network namespace available. The child environment is cleared except `BEND_NO_TELEMETRY=1`. The checker completed with that variable set and with an empty `HOME`. A local check also completed inside `unshare -r -n`, and it did not create a file next to the source.

The process is its own group. On timeout or an output overrun, Rust sends `SIGKILL` to the group and reaps it. stdout is capped at 64KiB and stderr at 16KiB. The wait is 30 seconds. stdin is closed.

`workflow.bend` and `evals.bend` each run only when their one real `def main` returns `String`. The guard follows Bend 2.0.28, which lexes these the same as 2.0.27: space, tab, `\n`, and a bare `\r` are whitespace; whole names and numbers are single tokens, so `xdef` is a name but `1.5def` is a float followed by a real `def`; a `#` comment runs to the next `\n`; a `"` string, escapes included, hides the text inside it, line breaks too. A `def main() -> String:` sitting inside a string is not the main. Any other real main is refused before `bend` starts, so an `IO` main cannot run. A `String` result is normalized by the checker. An `IO` result would be compiled and run.

## Formats across the boundary

The encoding is one versioned line inside the Bend string literal: `v2`, then the program in postfix, tokens separated by single spaces.

| Token | Meaning |
| --- | --- |
| `tool.<name>.<input>` | `Decision::Tool`; each text is lowercase hex of its UTF-8, and the input may be empty |
| `spawn.<agent>.<input>` | `Decision::SpawnAgent` |
| `complete`, `fail` | `Decision::Complete`, `Decision::Fail` |
| `on_counter` | `Decision::OnCounter` of the three decisions before it (missing, zero, other) |
| `end` | an empty sequence |
| `seq` | the decision before a sequence, put in front of it |

So `seq([a, b])` is `a b end seq seq`. Names are 1 to 128 bytes and inputs at most 65536, as in the Rhai and JS frontends. Neither output cap binds in practice: the pinned checker cannot print a `String` main much past 10,000 characters (10,000 printed; 11,000 overflowed its stack, measured in the container with an 8MiB stack), so a v2 line holds about 5KB of text as hex, and `evals.bend`'s line, at about 6,200 bytes, already uses about 60% of the ceiling. A line has at most 2048 tokens after `v2`, which bounds nesting to about 1023 levels, looser than the frontends' 1024 decisions and depth 64. The line must leave exactly one program. A `v1` line is refused (`bend_v1_line_rejected`). Backslashes, quotes, and any character that is not printable ASCII or a single space are rejected. Rust evaluates the program with `evaluate_program`, as for Rhai and JavaScript.

Bend's own model mirrors the IR: `GolDec` (a sequence is a `GolSeq{head, tail}` list ending in `GolEnd`; `GolDec` also admits a `GolSeq` whose tail is not a sequence, which Rust's parser rejects), `GolText` as bytes of two hex nibbles, and `GolRec` records. Bend's `eval_program` is written as the same cursor walk as Rust, and `bend_agrees_with_rust` checks it against Rust on five programs: a tool call or spawn whose record is at the cursor is done, the first without one is the command, a mismatch or running out is `fail`, and `on_counter` reads the counter recorded before its cursor, walking `missing` first when there is none. A counter output of `"0"` is zero; any other output is other. Running out of fuel returns a separate `stuck` result that neither the spec nor Rust produces.

`LAWS.bend` is not checked alone. An open law is a hole, and `bend LAWS.bend --check-only` reports those holes. That is the intended split: the law file states claims, `PROOF.bend` closes them. `bend PROOF.bend --check-only` checks the law file, `workflow.bend`, and the proofs together because of the imports.

## JSON

Base has no JSON type, parser, or printer. A Bend `String` can hold JSON text, and the checker prints it as a Bend literal (`"` and `\` escaped). Reading that back would mean parsing Bend's printer. gol does not do that. The token line has no escapes. JSON stays on the Rust side of the harness, where it already is.

## Can Rust load a Bend artifact

It can run the official binary. It cannot load the compiler or the generated object as a library. `-o file.c` emits a C program whose entry is `main`; the file inspected here was about 75KB and was not a library. Treating that C as an artifact to link would be a private fork of the runtime.

What Rust loads is the token line, after the proof gate, validated into `WorkflowProgram`. Steady-state decisions do not call Bend. `BendDriver` implements `WorkflowDriver` by calling `evaluate_program`. `harness-core::transition` consumes that step the same way it consumes any other driver's.

## Startup and steady state

On this machine, five runs of the checker against a one-line string program took about 78–87ms. Five runs of the native IO binary, after clang had already produced it, took about 0.6–0.8ms. `compile` pays for the version check, the proof gate, and normalization of `main`, each in its own sandboxed process.

`cargo bench -p workflow-bend --bench boundary` (criterion, 10 samples, this machine) measured:

| Bench | What it times | Point estimate |
| --- | --- | --- |
| `rust_eval` | `evaluate_program` on the six histories, program already built | 93.463 ns |
| `bend_compile` | `compile` of `experiments/bend`, proofs included | 206.35 ms |
| `loaded_eval` | the same six histories on the program `compile` returned | 95.320 ns |

The intervals were 93.388–93.602 ns, 201.69–212.42 ms, and 94.739–96.208 ns. The two eval rows call the same Rust function. All three figures predate the v2 program: `loaded_eval` now evaluates the reference program while `rust_eval` still evaluates `counter_program`, so the two rows no longer time the same program.

The load is the cost. Steady-state evaluation is `evaluate_program` either way. This is not a claim that Bend made the decision faster.

## How laws and proofs are checked

`experiments/bend/LAWS.bend` imports `workflow.bend` and states three laws:

- `round_trip`: for every program `p`, decoding its postfix token list gives back `p`. `PROOF.bend` closes it by structural induction on `p`, through the lemma `decode_encode` (decoding a program's tokens pushes that program).
- `program_agrees`: for every history drawn from eight records (counter records with outputs `"0"`, `"1"` and `"00"`, a `count` record, a `search` record, an `other` record, a `helper` spawn, and an `other` spawn), `eval_program(search_program(), h)` equals `spec(h)`, a table written separately from the walk. `PROOF.bend` splits the history to depth three; past that the program has finished, so the rest of the list is never read.
- `encoding`: `encode_line(search_program())` is the v2 line above.

`round_trip` covers the token list, not the text rendering; the exact `encoding` law and Rust's parser tests cover the text. Two negative controls fail as expected: a spec that fails after the helper spawn breaks `program_agrees`, and a decoder that swaps the `seq` operands breaks `decode_encode`.

`evals.bend` prints Bend's evaluation of five programs on every history of length 0 to 3 over the eight records: 2,925 results in a fixed order, with short codes (`c`, `f`, `s`) so the checker can print the line. The five programs are the reference program, `counter_program`, a counter recorded before `on_counter`'s cursor, a missing arm that records no counter, and a missing arm that records the counter and more. Together they reach every frame of the walk. `bend_agrees_with_rust` runs it and requires each result to be the command Rust's `evaluate_program` returns on the same program and history. That is the link from the Bend laws to Rust: the laws hold for Bend's walk, and the test shows Rust's walk agrees with it on those runs. Five wrong walks (swapped arms, a counter lookup that ignores older records, prefix text matching, and others) each fail it.

`./scripts/verify-bend.sh` requires `bend 2.0.28`, checks `workflow.bend` and `evals.bend`, requires `PROOF.bend --check-only` to print `All terms check.`, requires the v2 line, then runs `cargo test -p workflow-bend`. CI runs that script. A failing proof, a drifted encoding, or a Rust disagreement exits non-zero.

## Compile, proof, and runtime failures

| Failure | What 2.0.28 prints (2.0.27 printed the same) | Exit |
| --- | --- | --- |
| Syntax, `def` with no name | `expected : a name` on stderr | 1 |
| Type, `String` where `U32` is required | `expected : U32` / `observed : String` | 1 |
| False proof, `{1n == 0n}` closed by `{==}` | `expected : 1n` / `observed : 0n` | 1 |
| Open law | `Error: N TODO found.` on stderr | 1 |
| `PROOF.bend` that does not import `./LAWS.bend` | `PROOF.bend must import ./LAWS.bend` | 1 |
| Checker success | `All terms check.` on stdout, empty stderr | 0 |

The syntax error from a bare `def` was printed on the combined stream in the first probe; later checks showed proof failures on stderr and `All terms check.` on stdout. `workflow-bend` treats a non-zero status as failure and surfaces stderr, and it also rejects a zero status whose stdout is not exactly `All terms check.\n`.

Runtime, for this workflow, is Rust. Bend is not invoked per history. If Bend were invoked per history, a non-terminating `main` cannot be written inside the checker: termination is mandatory. The timeout still exists because the process can hang outside the checker (install, compiler bug, a future IO main that slipped the guard).

## Sandbox and resource controls

Rust imposes the controls. Bend does not.

- Network: new network namespace, plus `BEND_NO_TELEMETRY=1`. The daily version check is a GET that sends version, OS, and CPU type. The variable turns it off. The namespace is the hard stop.
- Files: the child starts in a private temp copy of the four sources. Each run `main` returns `String`, so the checker normalizes it instead of running the IO loop. The repo directory is not the child's cwd.
- Time: 30 second timeout, then `SIGKILL` on the process group.
- Output: 64KiB stdout, 16KiB stderr, then the same kill.
- Secrets: the child environment is empty except the telemetry switch. The parent does not pass the ambient environment through.
- Proofs: a passing proof does not relax any of the above.

The generated binary's `--threads` and `--gpu` caps are unused. gol does not build that binary on the decision path. GPU heap limits are not a substitute for the process kill.

There is no Bend-level memory quota on the checker. The output cap and the timeout are what stop a runaway print. Source files are refused past 64KiB before they are copied.

## What is stable enough to build on

- The pinned 2.0.28 release. `scripts/install-bend.sh` refuses the linux-x64 archive unless its sha256 is `22bb6d5f6bce8ae2c5b340371fedddcbd90edc07a48b6e2b351a944c4558a3eb`. The installed `~/.bend/bin/bend` hashes to `871df0ae7b0895236e14a5c2dac4fa014fbe770c3470ee3f60f6dcfe8ae4cfcf`. `bend version` must print `bend 2.0.28`. The upstream `bend-lang.com/install.sh` installs the newest release, so it is not used.
- `bend guide` for the language that binary implements.
- `--check-only`, the `LAWS.bend` / `PROOF.bend` split, and `All terms check.`
- A pure `String` `main` as a one-line encoding, re-validated in Rust.
- `WorkflowProgram` / `WorkflowStep` as the only types the adapter returns. Bend types do not enter `workflow-core`.

`workflow-bend` is a frontend in the same sense as `workflow-rhai` and `workflow-js`: `compile` produces the shared program. It is a directory of Bend sources rather than one script string because the proof gate requires `LAWS.bend` beside `PROOF.bend`. Folding those files into one string would drop the gate the compiler itself enforces.

## What must not be built yet

- A Rust rewrite of the harness, the scheduler, or tool execution in Bend.
- Bend `IO` for tools, files, network, cancellation, ownership, or persistence. The decision stays pure. Rust performs the effect.
- Linking generated C, or wrapping effect symbols, as if the ABI were stable.
- A parser for Bend syntax. The adapter reads one quoted token line.
- A Bend JSON stack.
- A duplicate of `protocol::reduce` or of a TLA+ model in Bend. TLA+ covers concurrent writers, and Rust tests cover the reducer. Bend covers this decision and its local laws.
- GPU or data-parallel scheduling of effects. Parallelism in Bend is for independent pure work. This decision has none.
- Publishing to the Bend hub, or `bend login`.
- Treating `All terms check.` as authorization to run the next Bend program with ambient rights.
- A performance claim that is not the boundary benchmark.
