import Base
import ./workflow.bend as W

# The gol workflow IR in Bend, encoding v2. Do not weaken a law to make a
# proof pass; a failing proof means stop.

# Every program survives encoding: the postfix token list decodes back to it.
law round_trip:
  for +p: W.GolDec
  {W.decode_program(W.encode(p, [])) == Some{p} : Maybe<W.GolDec>}

# The reference program does what its table says on every history drawn
# from the eight records W.record names: search, then read the counter; on
# zero spawn the helper and complete; any other record or counter fails.
law program_agrees:
  for +h: +List<W.GolKind>
  {W.eval_program(W.search_program(), W.embed(h)) == W.spec(h) : W.GolCmd}

law encoding:
  {W.encode_line(W.search_program()) == "v2 tool.736561726368.71 tool.636f756e746572. spawn.68656c706572.7a65726f complete end seq seq fail on_counter end seq seq" : String}
