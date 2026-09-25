import Base
import ./LAWS.bend as Laws
import ./counter.bend as C

def Laws.missing_executes():
  {==}

def Laws.zero_completes():
  {==}

def Laws.positive_fails(n):
  {==}

def Laws.negative_fails(n):
  {==}

def Laws.program_agrees(h):
  match h:
    case C.GolMissing{}:
      {==}
    case C.GolRecorded{sign, magnitude}:
      match sign:
        case C.GolNeg{}:
          {==}
        case C.GolNonNeg{}:
          match magnitude:
            case 0n:
              {==}
            case 1n+p:
              {==}

def Laws.encoding():
  {==}
