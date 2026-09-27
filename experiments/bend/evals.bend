import Base
import ./workflow.bend as W

# Bend's evaluation of the reference program on every history of length 0 to
# 3 over the six records, one token per history. Rust compares each token
# with evaluate_program on the same history, in the same order: length first,
# then, for each position from the first, the records in W.GolKind order.

def kinds() -> +List<W.GolKind>:
  [W.KZero{}, W.KOne{}, W.KSearch{}, W.KOther{}, W.KHelper{}, W.KStranger{}]

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

def show_all(hs: +List<+List<W.GolKind>>) -> String:
  match hs:
    case Nil{}:
      ""
    case Con{h, rest}:
      " " ++ W.show_cmd(W.eval_program(W.search_program(), W.embed(h))) ++ show_all(rest)

def main() -> String:
  "e2" ++ show_all(app(level(0n), app(level(1n), app(level(2n), level(3n)))))
