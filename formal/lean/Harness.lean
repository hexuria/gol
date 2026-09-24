import Init.Data.List.Lemmas

inductive Phase where
  | idle
  | running
  | waiting
  | completed
  | failed
  | cancelled
  deriving DecidableEq, Repr

inductive Ev where
  | start
  | requestTool
  | toolResult
  | duplicateResult
  | retry
  | advance
  | cancel
  | complete
  | fail
  deriving DecidableEq, Repr

structure State where
  phase : Phase
  attempt : Nat
  step : Nat
  pending : Bool
  answered : Bool
  deriving DecidableEq, Repr

def maxSteps : Nat := 3

def maxRetries : Nat := 2

def terminal (p : Phase) : Bool :=
  match p with
  | .completed => true
  | .failed => true
  | .cancelled => true
  | _ => false

def allowed (s : State) (e : Ev) : Bool :=
  match e with
  | .start => s.phase == .idle
  | .requestTool =>
      s.phase == .running && !s.answered && !s.pending && decide (1 ≤ s.step) && decide (s.step ≤ maxSteps)
  | .toolResult => s.phase == .waiting && s.pending && !s.answered
  | .duplicateResult => s.answered || terminal s.phase
  | .retry => s.phase == .running && s.answered && decide (s.attempt < maxRetries)
  | .advance => s.phase == .running && s.answered && decide (s.step < maxSteps)
  | .cancel => s.phase == .running || s.phase == .waiting
  | .complete => s.phase == .running
  | .fail => s.phase == .running || s.phase == .waiting

def apply (s : State) (e : Ev) : State :=
  match e with
  | .start => { s with phase := .running, step := 1 }
  | .requestTool => { s with phase := .waiting, pending := true }
  | .toolResult => { s with phase := .running, pending := false, answered := true }
  | .duplicateResult => s
  | .retry => { s with attempt := s.attempt + 1, answered := false }
  | .advance => { s with step := s.step + 1, answered := false }
  | .cancel => { s with phase := .cancelled, pending := false }
  | .complete => { s with phase := .completed, pending := false }
  | .fail => { s with phase := .failed, pending := false }

def step (s : State) (e : Ev) : State :=
  if allowed s e then apply s e else s

def validB (s : State) : Bool :=
  decide (s.attempt ≤ maxRetries) &&
    (decide (s.step ≤ maxSteps) &&
      (((s.phase == .waiting) == s.pending) &&
        ((!terminal s.phase || !s.pending) &&
          ((s.phase != .idle ||
              (decide (s.step = 0) && decide (s.attempt = 0) && !s.pending && !s.answered)) &&
            ((s.phase != .running && s.phase != .waiting) || decide (1 ≤ s.step))))))

def Valid (s : State) : Prop :=
  validB s = true

def rank (s : State) : Nat :=
  if terminal s.phase then
    0
  else if s.phase == .idle then
    8000
  else
    (maxSteps - s.step) * 1000 + (maxRetries - s.attempt) * 40 +
      if s.phase == .running && !s.answered then
        30
      else if s.phase == .waiting then
        20
      else
        10

def progress (e : Ev) : Bool :=
  e != .duplicateResult

def okStep (s : State) (e : Ev) : Bool :=
  if validB s && allowed s e then validB (apply s e) else true

def rankOk (s : State) (e : Ev) : Bool :=
  if validB s && allowed s e && progress e then decide (rank (apply s e) < rank s) else true

def phaseList : List Phase :=
  [.idle, .running, .waiting, .completed, .failed, .cancelled]

def evList : List Ev :=
  [.start, .requestTool, .toolResult, .duplicateResult, .retry, .advance, .cancel, .complete, .fail]

def boolList : List Bool :=
  [false, true]

def scanOk : Bool :=
  phaseList.all fun p =>
    (List.range (maxRetries + 1)).all fun attempt =>
      (List.range (maxSteps + 1)).all fun stepIdx =>
        boolList.all fun pending =>
          boolList.all fun answered =>
            evList.all fun e =>
              okStep
                { phase := p
                  attempt := attempt
                  step := stepIdx
                  pending := pending
                  answered := answered }
                e

def scanRank : Bool :=
  phaseList.all fun p =>
    (List.range (maxRetries + 1)).all fun attempt =>
      (List.range (maxSteps + 1)).all fun stepIdx =>
        boolList.all fun pending =>
          boolList.all fun answered =>
            evList.all fun e =>
              rankOk
                { phase := p
                  attempt := attempt
                  step := stepIdx
                  pending := pending
                  answered := answered }
                e

theorem scanOk_true : scanOk = true := by
  native_decide

theorem scanRank_true : scanRank = true := by
  native_decide

theorem phase_mem (p : Phase) : p ∈ phaseList := by
  cases p <;> simp [phaseList]

theorem ev_mem (e : Ev) : e ∈ evList := by
  cases e <;> simp [evList]

theorem bool_mem (b : Bool) : b ∈ boolList := by
  cases b <;> simp [boolList]

theorem okStep_bounded (s : State) (e : Ev) (ha : s.attempt ≤ maxRetries) (hs : s.step ≤ maxSteps) :
    okStep s e = true := by
  have h := scanOk_true
  rw [scanOk] at h
  have hP := (List.all_eq_true.mp h) s.phase (phase_mem s.phase)
  have hA := (List.all_eq_true.mp hP) s.attempt (List.mem_range.mpr (by omega))
  have hS := (List.all_eq_true.mp hA) s.step (List.mem_range.mpr (by omega))
  have hPend := (List.all_eq_true.mp hS) s.pending (bool_mem s.pending)
  have hAns := (List.all_eq_true.mp hPend) s.answered (bool_mem s.answered)
  exact (List.all_eq_true.mp hAns) e (ev_mem e)

theorem rankOk_bounded (s : State) (e : Ev) (ha : s.attempt ≤ maxRetries) (hs : s.step ≤ maxSteps) :
    rankOk s e = true := by
  have h := scanRank_true
  rw [scanRank] at h
  have hP := (List.all_eq_true.mp h) s.phase (phase_mem s.phase)
  have hA := (List.all_eq_true.mp hP) s.attempt (List.mem_range.mpr (by omega))
  have hS := (List.all_eq_true.mp hA) s.step (List.mem_range.mpr (by omega))
  have hPend := (List.all_eq_true.mp hS) s.pending (bool_mem s.pending)
  have hAns := (List.all_eq_true.mp hPend) s.answered (bool_mem s.answered)
  exact (List.all_eq_true.mp hAns) e (ev_mem e)

theorem bounds_of_valid (s : State) (hv : Valid s) : s.attempt ≤ maxRetries ∧ s.step ≤ maxSteps := by
  unfold Valid validB at hv
  obtain ⟨hA, hv⟩ := Bool.and_eq_true_iff.mp hv
  obtain ⟨hS, _⟩ := Bool.and_eq_true_iff.mp hv
  exact ⟨of_decide_eq_true hA, of_decide_eq_true hS⟩

theorem disallowed_is_identity (s : State) (e : Ev) (ha : allowed s e = false) : step s e = s := by
  simp [step, ha]

theorem duplicate_is_identity (s : State) : step s .duplicateResult = s := by
  unfold step apply
  split <;> rfl

theorem late_tool_result (s : State) (h : s.phase = .cancelled) : step s .toolResult = s := by
  simp [step, allowed, h]

theorem no_retry_after_cancel (s : State) (h : s.phase = .cancelled) : allowed s .retry = false := by
  simp [allowed, h]

theorem retry_after_cancel (s : State) (h : s.phase = .cancelled) : step s .retry = s :=
  disallowed_is_identity s .retry (no_retry_after_cancel s h)

theorem terminal_stuck (s : State) (e : Ev) (h : terminal s.phase = true) : step s e = s := by
  cases hp : s.phase
  case idle =>
    simp [terminal, hp] at h
  case running =>
    simp [terminal, hp] at h
  case waiting =>
    simp [terminal, hp] at h
  case completed =>
    cases e <;> simp [step, allowed, apply, hp, terminal]
  case failed =>
    cases e <;> simp [step, allowed, apply, hp, terminal]
  case cancelled =>
    cases e <;> simp [step, allowed, apply, hp, terminal]

theorem step_preserves_validity (s : State) (e : Ev) (hv : Valid s) (ha : allowed s e = true) :
    Valid (step s e) := by
  rcases bounds_of_valid s hv with ⟨hatt, hst⟩
  have hok := okStep_bounded s e hatt hst
  have hvB : validB s = true := by
    simpa [Valid] using hv
  have hcond : (validB s && allowed s e) = true :=
    Bool.and_eq_true_iff.mpr ⟨hvB, ha⟩
  rw [okStep, hcond] at hok
  simpa [Valid, step, ha, ite_true] using hok

theorem rank_decreases (s : State) (e : Ev) (hv : Valid s) (ha : allowed s e = true)
    (hp : progress e = true) : rank (step s e) < rank s := by
  rcases bounds_of_valid s hv with ⟨hatt, hst⟩
  have hr := rankOk_bounded s e hatt hst
  have hvB : validB s = true := by
    simpa [Valid] using hv
  have hpair : (validB s && allowed s e) = true :=
    Bool.and_eq_true_iff.mpr ⟨hvB, ha⟩
  have hcond : ((validB s && allowed s e) && progress e) = true :=
    Bool.and_eq_true_iff.mpr ⟨hpair, hp⟩
  rw [rankOk, hcond] at hr
  have hlt : rank (apply s e) < rank s := by
    simpa [ite_true] using of_decide_eq_true hr
  simpa [step, ha] using hlt
