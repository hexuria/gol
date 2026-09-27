import Base
import ./LAWS.bend as Laws
import ./workflow.bend as W

# Lemma: decoding a program's tokens pushes that program.
law decode_encode:
  for p: W.GolDec
  for +rest: +List<W.GolTok>
  for +s: +List<W.GolDec>
  {W.decode(rest, Some{p <> s}) == W.decode(W.encode(p, rest), Some{s}) : Maybe<+List<W.GolDec>>}

def decode_encode(p, rest, s):
  match p:
    case W.GolTool{name, input}:
      {==}
    case W.GolSpawn{agent, input}:
      {==}
    case W.GolComplete{}:
      {==}
    case W.GolHalt{}:
      {==}
    case W.GolEnd{}:
      {==}
    case W.GolSeq{+head, +tail}:
      %decode_encode(head, W.encode(tail, W.TokSeq{} <> rest), s) : {W.decode(rest, Some{W.GolSeq{head, tail} <> s}) == _ : Maybe<+List<W.GolDec>>}
      %decode_encode(tail, W.TokSeq{} <> rest, head <> s) : {W.decode(rest, Some{W.GolSeq{head, tail} <> s}) == _ : Maybe<+List<W.GolDec>>}
      {==}
    case W.GolOnCounter{+missing, +zero, +other}:
      %decode_encode(missing, W.encode(zero, W.encode(other, W.TokOnCounter{} <> rest)), s) : {W.decode(rest, Some{W.GolOnCounter{missing, zero, other} <> s}) == _ : Maybe<+List<W.GolDec>>}
      %decode_encode(zero, W.encode(other, W.TokOnCounter{} <> rest), missing <> s) : {W.decode(rest, Some{W.GolOnCounter{missing, zero, other} <> s}) == _ : Maybe<+List<W.GolDec>>}
      %decode_encode(other, W.TokOnCounter{} <> rest, zero <> missing <> s) : {W.decode(rest, Some{W.GolOnCounter{missing, zero, other} <> s}) == _ : Maybe<+List<W.GolDec>>}
      {==}

def Laws.round_trip(p):
  %decode_encode(p, [], []) : {W.single(_) == Some{p} : Maybe<W.GolDec>}
  {==}

def Laws.program_agrees(h):
  match h:
    case Nil{}:
      {==}
    case Con{k0, t0}:
      match k0:
        case W.KZero{}:
          {==}
        case W.KOne{}:
          {==}
        case W.KZeroZero{}:
          {==}
        case W.KCount{}:
          {==}
        case W.KSearch{}:
          match t0:
            case Nil{}:
              {==}
            case Con{k1, t1}:
              match k1:
                case W.KZero{}:
                  match t1:
                    case Nil{}:
                      {==}
                    case Con{k2, t2}:
                      match k2:
                        case W.KZero{}:
                          {==}
                        case W.KOne{}:
                          {==}
                        case W.KZeroZero{}:
                          {==}
                        case W.KCount{}:
                          {==}
                        case W.KSearch{}:
                          {==}
                        case W.KOther{}:
                          {==}
                        case W.KHelper{}:
                          {==}
                        case W.KStranger{}:
                          {==}
                case W.KOne{}:
                  {==}
                case W.KZeroZero{}:
                  {==}
                case W.KCount{}:
                  {==}
                case W.KSearch{}:
                  {==}
                case W.KOther{}:
                  {==}
                case W.KHelper{}:
                  {==}
                case W.KStranger{}:
                  {==}
        case W.KOther{}:
          {==}
        case W.KHelper{}:
          {==}
        case W.KStranger{}:
          {==}

def Laws.encoding():
  {==}
