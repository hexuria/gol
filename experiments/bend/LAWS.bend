import Base
import ./counter.bend as C

# Counter workflow. Do not weaken a law to make a proof pass.
# Missing asks for the counter tool. Only non-negative zero completes.
# Every positive magnitude fails. Every negative sign fails.
# Rust i64 zero is NonNeg and 0n. i64::MIN is negative. i64::MAX is positive.

law missing_executes:
  {C.decide(C.GolMissing{}) == C.GolExecute{} : C.GolCmd}

law zero_completes:
  {C.decide(C.GolRecorded{C.GolNonNeg{}, 0n}) == C.GolComplete{} : C.GolCmd}

law positive_fails:
  for n: Nat
  {C.decide(C.GolRecorded{C.GolNonNeg{}, 1n+n}) == C.GolHalt{} : C.GolCmd}

law negative_fails:
  for n: Nat
  {C.decide(C.GolRecorded{C.GolNeg{}, n}) == C.GolHalt{} : C.GolCmd}

law program_agrees:
  for h: C.GolHist
  {C.eval_program(C.counter_program(), h) == C.decide(h) : C.GolCmd}

law encoding:
  {C.encode(C.counter_program()) == "v1 on_counter execute complete fail" : String}
