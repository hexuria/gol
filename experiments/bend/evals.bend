import Base
import ./workflow.bend as W

# Bend's evaluation of five programs on every history of length 0 to 3 over
# the eight records, one result per program and history. Rust compares each
# token with evaluate_program on the same program and history, in the same
# order: programs as listed in main, then histories by length, the first
# record varying slowest, records in W.GolKind order.

def kinds() -> +List<W.GolKind>:
  [W.KZero{}, W.KOne{}, W.KZeroZero{}, W.KCount{}, W.KSearch{}, W.KOther{}, W.KHelper{}, W.KStranger{}]

def app(a: +List<+List<W.GolKind>>, b: +List<+List<W.GolKind>>) -> +List<+List<W.GolKind>>:
  match a:
    case Nil{}:
      b
    case Con{x, rest}:
      x <> app(rest, b)

def prepend_all(+k: W.GolKind, hs: +List<+List<W.GolKind>>) -> +List<+List<W.GolKind>>:
  match hs:
    case Nil{}:
      []
    case Con{h, rest}:
      (k <> h) <> prepend_all(k, rest)

def by_kinds(ks: +List<W.GolKind>, +hs: +List<+List<W.GolKind>>) -> +List<+List<W.GolKind>>:
  match ks:
    case Nil{}:
      []
    case Con{k, rest}:
      app(prepend_all(k, hs), by_kinds(rest, hs))

# Every history of length n; the first record varies slowest.
def level(n: Nat) -> +List<+List<W.GolKind>>:
  match n:
    case 0n:
      [[]]
    case 1n+m:
      by_kinds(kinds(), level(m))

# One result: `c` complete, `f` fail, `s` out of fuel, or the tool or spawn
# token. The short codes keep the whole line under what the checker's
# normalizer can print.
def code(c: W.GolCmd) -> String:
  match c:
    case W.CComplete{}:
      "c"
    case W.CHalt{}:
      "f"
    case W.CStuck{}:
      "s"
    case W.CExec{name, input}:
      W.show_cmd(W.CExec{name, input})
    case W.CSpawn{agent, input}:
      W.show_cmd(W.CSpawn{agent, input})

# Each token, then `after`. Threading the rest of the output through keeps
# every ++ short on its left, which the checker's normalizer needs.
def show_all(+p: W.GolDec, hs: +List<+List<W.GolKind>>, after: String) -> String:
  match hs:
    case Nil{}:
      after
    case Con{h, rest}:
      " " ++ code(W.eval_program(p, W.embed(h))) ++ show_all(p, rest, after)

def histories() -> +List<+List<W.GolKind>>:
  app(level(0n), app(level(1n), app(level(2n), level(3n))))

def main() -> String:
  "e2" ++ show_all(W.search_program(), histories(), show_all(W.counter_program(), histories(), show_all(W.counter_first_program(), histories(), show_all(W.no_counter_program(), histories(), show_all(W.missing_then_arm_program(), histories(), "")))))
