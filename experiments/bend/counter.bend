import Base

type GolCmd is Data:
  GolExecute{}
  GolComplete{}
  GolHalt{}

type GolSign is Data:
  GolNonNeg{}
  GolNeg{}

type GolHist is Data:
  GolMissing{}
  GolRecorded{sign: GolSign, magnitude: Nat}

type GolClass is Data:
  GolClassMissing{}
  GolClassZero{}
  GolClassOther{}

type GolProgram is Data:
  GolOnCounter{missing: GolCmd, zero: GolCmd, other: GolCmd}

def classify(h: GolHist) -> GolClass:
  match h:
    case GolMissing{}:
      GolClassMissing{}
    case GolRecorded{sign, magnitude}:
      match sign:
        case GolNeg{}:
          GolClassOther{}
        case GolNonNeg{}:
          match magnitude:
            case 0n:
              GolClassZero{}
            case 1n+p:
              GolClassOther{}

def counter_program() -> GolProgram:
  GolOnCounter{GolExecute{}, GolComplete{}, GolHalt{}}

def arm(p: GolProgram, c: GolClass) -> GolCmd:
  match p:
    case GolOnCounter{missing, zero, other}:
      match c:
        case GolClassMissing{}:
          missing
        case GolClassZero{}:
          zero
        case GolClassOther{}:
          other

def decide(h: GolHist) -> GolCmd:
  match h:
    case GolMissing{}:
      GolExecute{}
    case GolRecorded{sign, magnitude}:
      match sign:
        case GolNeg{}:
          GolHalt{}
        case GolNonNeg{}:
          match magnitude:
            case 0n:
              GolComplete{}
            case 1n+p:
              GolHalt{}

def eval_program(p: GolProgram, h: GolHist) -> GolCmd:
  arm(p, classify(h))

def show_cmd(c: GolCmd) -> String:
  match c:
    case GolExecute{}:
      "execute"
    case GolComplete{}:
      "complete"
    case GolHalt{}:
      "fail"

def encode(p: GolProgram) -> String:
  match p:
    case GolOnCounter{missing, zero, other}:
      "v1 on_counter " ++ show_cmd(missing) ++ " " ++ show_cmd(zero) ++ " " ++ show_cmd(other)

def main() -> String:
  encode(counter_program())
