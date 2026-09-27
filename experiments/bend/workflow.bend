# The gol workflow IR in Bend, encoding v2. See docs/bend.md.
import Base


type GolNib is Data:
  N0{}
  N1{}
  N2{}
  N3{}
  N4{}
  N5{}
  N6{}
  N7{}
  N8{}
  N9{}
  NA{}
  NB{}
  NC{}
  ND{}
  NE{}
  NF{}

type GolByte is Data:
  GolB{hi: GolNib, lo: GolNib}

type GolText is Data:
  TNil{}
  TCon{head: GolByte, tail: GolText}

type GolDec is Data:
  GolTool{name: GolText, input: GolText}
  GolSpawn{agent: GolText, input: GolText}
  GolComplete{}
  GolHalt{}
  GolOnCounter{missing: GolDec, zero: GolDec, other: GolDec}
  GolSeq{head: GolDec, tail: GolDec}
  GolEnd{}

type GolTok is Data:
  TokTool{name: GolText, input: GolText}
  TokSpawn{agent: GolText, input: GolText}
  TokComplete{}
  TokFail{}
  TokOnCounter{}
  TokSeq{}
  TokEnd{}

def encode(p: GolDec, rest: +List<GolTok>) -> +List<GolTok>:
  match p:
    case GolTool{name, input}:
      TokTool{name, input} <> rest
    case GolSpawn{agent, input}:
      TokSpawn{agent, input} <> rest
    case GolComplete{}:
      TokComplete{} <> rest
    case GolHalt{}:
      TokFail{} <> rest
    case GolOnCounter{missing, zero, other}:
      encode(missing, encode(zero, encode(other, TokOnCounter{} <> rest)))
    case GolSeq{head, tail}:
      encode(head, encode(tail, TokSeq{} <> rest))
    case GolEnd{}:
      TokEnd{} <> rest

def step(t: GolTok, s: +List<GolDec>) -> Maybe<+List<GolDec>>:
  match t:
    case TokTool{name, input}:
      Some{GolTool{name, input} <> s}
    case TokSpawn{agent, input}:
      Some{GolSpawn{agent, input} <> s}
    case TokComplete{}:
      Some{GolComplete{} <> s}
    case TokFail{}:
      Some{GolHalt{} <> s}
    case TokEnd{}:
      Some{GolEnd{} <> s}
    case TokSeq{}:
      match s:
        case Con{tail, Con{head, rest}}:
          Some{GolSeq{head, tail} <> rest}
        case _:
          None{}
    case TokOnCounter{}:
      match s:
        case Con{other, Con{zero, Con{missing, rest}}}:
          Some{GolOnCounter{missing, zero, other} <> rest}
        case _:
          None{}

def step_m(t: GolTok, ms: Maybe<+List<GolDec>>) -> Maybe<+List<GolDec>>:
  match ms:
    case None{}:
      None{}
    case Some{s}:
      step(t, s)

def decode(ts: +List<GolTok>, ms: Maybe<+List<GolDec>>) -> Maybe<+List<GolDec>>:
  match ts:
    case Nil{}:
      ms
    case Con{t, rest}:
      decode(rest, step_m(t, ms))

def single(ms: Maybe<+List<GolDec>>) -> Maybe<GolDec>:
  match ms:
    case Some{Con{p, Nil{}}}:
      Some{p}
    case _:
      None{}

def decode_program(ts: +List<GolTok>) -> Maybe<GolDec>:
  single(decode(ts, Some{[]}))

def t_search() -> GolText:
  TCon{GolB{N7{}, N3{}}, TCon{GolB{N6{}, N5{}}, TCon{GolB{N6{}, N1{}}, TCon{GolB{N7{}, N2{}}, TCon{GolB{N6{}, N3{}}, TCon{GolB{N6{}, N8{}}, TNil{}}}}}}}

def t_q() -> GolText:
  TCon{GolB{N7{}, N1{}}, TNil{}}

def t_counter() -> GolText:
  TCon{GolB{N6{}, N3{}}, TCon{GolB{N6{}, NF{}}, TCon{GolB{N7{}, N5{}}, TCon{GolB{N6{}, NE{}}, TCon{GolB{N7{}, N4{}}, TCon{GolB{N6{}, N5{}}, TCon{GolB{N7{}, N2{}}, TNil{}}}}}}}}

def t_empty() -> GolText:
  TNil{}

def t_helper() -> GolText:
  TCon{GolB{N6{}, N8{}}, TCon{GolB{N6{}, N5{}}, TCon{GolB{N6{}, NC{}}, TCon{GolB{N7{}, N0{}}, TCon{GolB{N6{}, N5{}}, TCon{GolB{N7{}, N2{}}, TNil{}}}}}}}

def t_zero() -> GolText:
  TCon{GolB{N7{}, NA{}}, TCon{GolB{N6{}, N5{}}, TCon{GolB{N7{}, N2{}}, TCon{GolB{N6{}, NF{}}, TNil{}}}}}

def t_0() -> GolText:
  TCon{GolB{N3{}, N0{}}, TNil{}}

def t_1() -> GolText:
  TCon{GolB{N3{}, N1{}}, TNil{}}

def t_found() -> GolText:
  TCon{GolB{N6{}, N6{}}, TCon{GolB{N6{}, NF{}}, TCon{GolB{N7{}, N5{}}, TCon{GolB{N6{}, NE{}}, TCon{GolB{N6{}, N4{}}, TNil{}}}}}}

def t_other() -> GolText:
  TCon{GolB{N6{}, NF{}}, TCon{GolB{N7{}, N4{}}, TCon{GolB{N6{}, N8{}}, TCon{GolB{N6{}, N5{}}, TCon{GolB{N7{}, N2{}}, TNil{}}}}}}

def t_00() -> GolText:
  TCon{GolB{N3{}, N0{}}, TCon{GolB{N3{}, N0{}}, TNil{}}}

def t_count() -> GolText:
  TCon{GolB{N6{}, N3{}}, TCon{GolB{N6{}, NF{}}, TCon{GolB{N7{}, N5{}}, TCon{GolB{N6{}, NE{}}, TCon{GolB{N7{}, N4{}}, TNil{}}}}}}

def t_x() -> GolText:
  TCon{GolB{N7{}, N8{}}, TNil{}}

def nib_nat(n: GolNib) -> Nat:
  match n:
    case N0{}:
      0n
    case N1{}:
      1n
    case N2{}:
      2n
    case N3{}:
      3n
    case N4{}:
      4n
    case N5{}:
      5n
    case N6{}:
      6n
    case N7{}:
      7n
    case N8{}:
      8n
    case N9{}:
      9n
    case NA{}:
      10n
    case NB{}:
      11n
    case NC{}:
      12n
    case ND{}:
      13n
    case NE{}:
      14n
    case NF{}:
      15n

def nib_show(n: GolNib) -> String:
  match n:
    case N0{}:
      "0"
    case N1{}:
      "1"
    case N2{}:
      "2"
    case N3{}:
      "3"
    case N4{}:
      "4"
    case N5{}:
      "5"
    case N6{}:
      "6"
    case N7{}:
      "7"
    case N8{}:
      "8"
    case N9{}:
      "9"
    case NA{}:
      "a"
    case NB{}:
      "b"
    case NC{}:
      "c"
    case ND{}:
      "d"
    case NE{}:
      "e"
    case NF{}:
      "f"

def byte_eq(a: GolByte, b: GolByte) -> Bool:
  match a b:
    case GolB{ah, al} GolB{bh, bl}:
      Nat.is_eq(nib_nat(ah), nib_nat(bh)) && Nat.is_eq(nib_nat(al), nib_nat(bl))

def text_eq(a: GolText, b: GolText) -> Bool:
  match a b:
    case TNil{} TNil{}:
      True{}
    case TCon{ah, at} TCon{bh, bt}:
      byte_eq(ah, bh) && text_eq(at, bt)
    case _ _:
      False{}

def hex(t: GolText) -> String:
  match t:
    case TNil{}:
      ""
    case TCon{GolB{hi, lo}, rest}:
      nib_show(hi) ++ nib_show(lo) ++ hex(rest)

# A run's recorded history, oldest first.
type GolRec is Data:
  RTool{name: GolText, output: GolText}
  RSpawn{agent: GolText}

type GolCmd is Data:
  CExec{name: GolText, input: GolText}
  CSpawn{agent: GolText, input: GolText}
  CComplete{}
  CHalt{}
  # Fuel ran out. Neither spec nor Rust ever gives this, so a walk that
  # exhausts its fuel cannot pass for a real fail.
  CStuck{}

type GolClass is Data:
  GolMissing{}
  GolZero{}
  GolOther{}

def class_of(b: Bool) -> GolClass:
  match b:
    case True{}:
      GolZero{}
    case False{}:
      GolOther{}

def pick_counter(b: Bool, o: GolText, older: GolClass) -> GolClass:
  match b:
    case True{}:
      class_of(text_eq(o, t_0()))
    case False{}:
      older

# The counter class of the records before the cursor, newest first: the
# output of the latest `counter` record, "0" is zero, anything else other.
def class_before(before: +List<GolRec>) -> GolClass:
  match before:
    case Nil{}:
      GolMissing{}
    case Con{r, older}:
      match r:
        case RTool{+name, output}:
          pick_counter(text_eq(name, t_counter()), output, class_before(older))
        case RSpawn{agent}:
          class_before(older)

# Frames of work left after the current decision. Guard and Pick carry a
# value computed one step earlier, so the walk branches by matching a frame,
# never a computed value.
type GolFrame is Data:
  FRun{d: GolDec}
  FGuard{ok: Bool}
  FPick{c: GolClass, missing: GolDec, zero: GolDec, other: GolDec}
  FCheck{zero: GolDec, other: GolDec}
  FArm{c: GolClass, zero: GolDec, other: GolDec}

# The cursor walk. A tool call or spawn whose record is at the cursor is done;
# the first one without a record is the command. A record that does not
# match, or running out of decisions, is fail. on_counter reads the counter
# before its cursor; with none it walks missing, then zero or other once
# missing has recorded one. Each step spends one unit of fuel.
def run(f: Nat, d: GolDec, ks: +List<GolFrame>, +before: +List<GolRec>, after: +List<GolRec>) -> GolCmd:
  match f:
    case 0n:
      CStuck{}
    case 1n+g:
      match d:
        case GolTool{+name, input}:
          match after:
            case Nil{}:
              CExec{name, input}
            case Con{r, more}:
              match r:
                case RTool{+rn, ro}:
                  run(g, GolEnd{}, FGuard{text_eq(rn, name)} <> ks, RTool{rn, ro} <> before, more)
                case RSpawn{ra}:
                  CHalt{}
        case GolSpawn{+agent, input}:
          match after:
            case Nil{}:
              CSpawn{agent, input}
            case Con{r, more}:
              match r:
                case RSpawn{+ra}:
                  run(g, GolEnd{}, FGuard{text_eq(ra, agent)} <> ks, RSpawn{ra} <> before, more)
                case RTool{rn, ro}:
                  CHalt{}
        case GolComplete{}:
          CComplete{}
        case GolHalt{}:
          CHalt{}
        case GolSeq{head, tail}:
          run(g, head, FRun{tail} <> ks, before, after)
        case GolOnCounter{missing, zero, other}:
          run(g, GolEnd{}, FPick{class_before(before), missing, zero, other} <> ks, before, after)
        case GolEnd{}:
          match ks:
            case Nil{}:
              CHalt{}
            case Con{k, rest}:
              match k:
                case FRun{next}:
                  run(g, next, rest, before, after)
                case FGuard{ok}:
                  match ok:
                    case True{}:
                      run(g, GolEnd{}, rest, before, after)
                    case False{}:
                      CHalt{}
                case FPick{c, missing, zero, other}:
                  match c:
                    case GolMissing{}:
                      run(g, missing, FCheck{zero, other} <> rest, before, after)
                    case GolZero{}:
                      run(g, zero, rest, before, after)
                    case GolOther{}:
                      run(g, other, rest, before, after)
                case FCheck{zero, other}:
                  run(g, GolEnd{}, FArm{class_before(before), zero, other} <> rest, before, after)
                case FArm{c, zero, other}:
                  match c:
                    case GolMissing{}:
                      run(g, GolEnd{}, rest, before, after)
                    case GolZero{}:
                      run(g, zero, rest, before, after)
                    case GolOther{}:
                      run(g, other, rest, before, after)

def eval_program(p: GolDec, h: +List<GolRec>) -> GolCmd:
  run(256n, p, [], [], h)

# The reference program: search, then read the counter; on zero spawn the
# helper and complete, otherwise fail.
def search_program() -> GolDec:
  GolSeq{GolTool{t_search(), t_q()}, GolSeq{GolOnCounter{GolTool{t_counter(), t_empty()}, GolSeq{GolSpawn{t_helper(), t_zero()}, GolSeq{GolComplete{}, GolEnd{}}}, GolHalt{}}, GolEnd{}}}

# More programs evals.bend runs, each reaching a path of the walk the
# reference program does not.
def counter_program() -> GolDec:
  GolOnCounter{GolTool{t_counter(), t_empty()}, GolComplete{}, GolHalt{}}

# A counter recorded before on_counter's cursor, behind a newer spawn record.
def counter_first_program() -> GolDec:
  GolSeq{GolTool{t_counter(), t_empty()}, GolSeq{GolSpawn{t_helper(), t_zero()}, GolSeq{GolOnCounter{GolHalt{}, GolComplete{}, GolHalt{}}, GolEnd{}}}}

# A missing arm that records no counter; the sequence then goes on.
def no_counter_program() -> GolDec:
  GolSeq{GolOnCounter{GolTool{t_search(), t_q()}, GolHalt{}, GolHalt{}}, GolSeq{GolSpawn{t_helper(), t_zero()}, GolEnd{}}}

# A missing arm that records the counter and more; the zero arm runs after.
def missing_then_arm_program() -> GolDec:
  GolOnCounter{GolSeq{GolTool{t_counter(), t_empty()}, GolSeq{GolTool{t_search(), t_q()}, GolEnd{}}}, GolSeq{GolSpawn{t_helper(), t_zero()}, GolSeq{GolComplete{}, GolEnd{}}}, GolHalt{}}

def show_tok(t: GolTok) -> String:
  match t:
    case TokTool{name, input}:
      "tool." ++ hex(name) ++ "." ++ hex(input)
    case TokSpawn{agent, input}:
      "spawn." ++ hex(agent) ++ "." ++ hex(input)
    case TokComplete{}:
      "complete"
    case TokFail{}:
      "fail"
    case TokOnCounter{}:
      "on_counter"
    case TokSeq{}:
      "seq"
    case TokEnd{}:
      "end"

def render(ts: +List<GolTok>) -> String:
  match ts:
    case Nil{}:
      ""
    case Con{t, rest}:
      " " ++ show_tok(t) ++ render(rest)

def encode_line(p: GolDec) -> String:
  "v2" ++ render(encode(p, []))

def show_cmd(c: GolCmd) -> String:
  match c:
    case CExec{name, input}:
      "tool." ++ hex(name) ++ "." ++ hex(input)
    case CSpawn{agent, input}:
      "spawn." ++ hex(agent) ++ "." ++ hex(input)
    case CComplete{}:
      "complete"
    case CHalt{}:
      "fail"
    case CStuck{}:
      "stuck"

# The eight records histories are drawn from in the laws and in evals.bend.
type GolKind is Data:
  KZero{}
  KOne{}
  KZeroZero{}
  KCount{}
  KSearch{}
  KOther{}
  KHelper{}
  KStranger{}

def record(k: GolKind) -> GolRec:
  match k:
    case KZero{}:
      RTool{t_counter(), t_0()}
    case KOne{}:
      RTool{t_counter(), t_1()}
    case KZeroZero{}:
      RTool{t_counter(), t_00()}
    case KCount{}:
      RTool{t_count(), t_0()}
    case KSearch{}:
      RTool{t_search(), t_found()}
    case KOther{}:
      RTool{t_other(), t_x()}
    case KHelper{}:
      RSpawn{t_helper()}
    case KStranger{}:
      RSpawn{t_other()}

def embed(h: +List<GolKind>) -> +List<GolRec>:
  match h:
    case Nil{}:
      []
    case Con{k, rest}:
      record(k) <> embed(rest)

# What search_program must do, written as a table over the history.
def spec_after_zero(h: +List<GolKind>) -> GolCmd:
  match h:
    case Nil{}:
      CSpawn{t_helper(), t_zero()}
    case Con{k, rest}:
      match k:
        case KHelper{}:
          CComplete{}
        case _:
          CHalt{}

def spec_after_search(h: +List<GolKind>) -> GolCmd:
  match h:
    case Nil{}:
      CExec{t_counter(), t_empty()}
    case Con{k, rest}:
      match k:
        case KZero{}:
          spec_after_zero(rest)
        case _:
          CHalt{}

def spec(h: +List<GolKind>) -> GolCmd:
  match h:
    case Nil{}:
      CExec{t_search(), t_q()}
    case Con{k, rest}:
      match k:
        case KSearch{}:
          spec_after_search(rest)
        case _:
          CHalt{}

def main() -> String:
  encode_line(search_program())
