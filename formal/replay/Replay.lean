-- this spec is one counter journal; the kernel line is pull request 7 at 1291bc64c78e71b8f5a3f4745184e1472decc679, shown by cargo test -p runtime-tokio --test replay_proof; this spec has no variable for that line.

inductive Journal where
  | empty
  | zero
  | nonzero
  deriving DecidableEq, Repr

inductive Held where
  | none
  | zero
  deriving DecidableEq, Repr

inductive Command where
  | execute
  | complete
  | fail
  deriving DecidableEq, Repr

inductive Path where
  | unrecorded
  | zero
  | nonzero
  deriving DecidableEq, Repr

inductive Run where
  | «open»
  | completed
  | failed
  deriving DecidableEq, Repr

inductive Wait where
  | none
  deriving DecidableEq, Repr

structure EffectId where
  workflow : String
  path : Path
  sequence : Nat
  deriving DecidableEq, Repr

def commandOf : Journal → Command
  | .empty => .execute
  | .zero => .complete
  | .nonzero => .fail

def pathOf : Journal → Path
  | .empty => .unrecorded
  | .zero => .zero
  | .nonzero => .nonzero

def runOf : Journal → Run
  | .empty => .«open»
  | .zero => .completed
  | .nonzero => .failed

def waitOf : Wait := .none

def idOf (j : Journal) : EffectId :=
  { workflow := "counter-branch", path := pathOf j, sequence := 0 }

def performAllowed (j : Journal) (h : Held) : Bool :=
  j == .empty && h == .none

def perform (j : Journal) (_h : Held) : Journal × Held :=
  (j, .zero)

def commit (j : Journal) (h : Held) : Journal × Held :=
  match j with
  | .empty => (.zero, h)
  | .zero => (.zero, h)
  | .nonzero => (.nonzero, h)

def crash (j : Journal) : Journal × Held :=
  (j, .none)

theorem journaled_result_forces_branch :
    commandOf .empty = .execute ∧
      pathOf .empty = .unrecorded ∧
        runOf .empty = .«open» ∧
          waitOf = .none ∧
            commandOf .zero = .complete ∧
              pathOf .zero = .zero ∧
                runOf .zero = .completed ∧
                  waitOf = .none ∧
                    commandOf .nonzero = .fail ∧
                      pathOf .nonzero = .nonzero ∧
                        runOf .nonzero = .failed ∧
                          waitOf = .none ∧
                            idOf .empty = { workflow := "counter-branch", path := .unrecorded, sequence := 0 } ∧
                              idOf .zero = { workflow := "counter-branch", path := .zero, sequence := 0 } ∧
                                idOf .nonzero = { workflow := "counter-branch", path := .nonzero, sequence := 0 } := by
  repeat (first | rfl | constructor)

theorem held_is_not_a_hit :
    performAllowed .empty .none = true ∧
      (perform .empty .none).1 = .empty ∧
        commandOf (perform .empty .none).1 = .execute := by
  repeat (first | rfl | constructor)

theorem journaled_id_not_executed_again (j : Journal) (h : Held) :
    (performAllowed j h = true → j = .empty ∧ pathOf j = .unrecorded) ∧
      performAllowed .zero h = false ∧
        performAllowed .nonzero h = false := by
  cases j <;> cases h <;> simp [performAllowed, pathOf]

theorem hit_sticks :
    (commit .empty .zero).1 = .zero ∧
      (commit .zero .zero).1 ≠ .empty ∧
        (commit .nonzero .zero).1 ≠ .empty ∧
          (crash .zero).1 ≠ .empty ∧
            (crash .nonzero).1 ≠ .empty := by
  constructor
  · rfl
  constructor
  · intro h
    cases h
  constructor
  · intro h
    cases h
  constructor
  · intro h
    cases h
  · intro h
    cases h

theorem crash_reenables_perform :
    crash .empty = (.empty, .none) ∧
      performAllowed (crash .empty).1 (crash .empty).2 = true := by
  simp [crash, performAllowed]

theorem hit_disables_perform :
    crash .zero = (.zero, .none) ∧
      performAllowed (crash .zero).1 (crash .zero).2 = false ∧
        commandOf (crash .zero).1 = .complete := by
  simp [crash, performAllowed, commandOf]

theorem branch_changes_id :
    idOf .empty ≠ idOf .zero ∧
      idOf .zero ≠ idOf .nonzero ∧
        idOf .empty ≠ idOf .nonzero ∧
          (idOf .empty).sequence = 0 ∧
            (idOf .zero).sequence = 0 ∧
              (idOf .nonzero).sequence = 0 ∧
                (idOf .empty).workflow = "counter-branch" ∧
                  (idOf .zero).workflow = "counter-branch" ∧
                    (idOf .nonzero).workflow = "counter-branch" := by
  refine ⟨?_, ?_, ?_, rfl, rfl, rfl, rfl, rfl, rfl⟩
  · intro h
    have : Path.unrecorded = Path.zero := by
      simpa [idOf, pathOf] using congrArg EffectId.path h
    cases this
  · intro h
    have : Path.zero = Path.nonzero := by
      simpa [idOf, pathOf] using congrArg EffectId.path h
    cases this
  · intro h
    have : Path.unrecorded = Path.nonzero := by
      simpa [idOf, pathOf] using congrArg EffectId.path h
    cases this
