## ADDED Requirements

### Requirement: Timing page shows an airspace micro-timeline
The Timing page SHALL render a short-window aircraft activity micro-timeline that shows when each visible aircraft was heard over the recent rolling window.

#### Scenario: Operator inspects dominant recent talkers
- **WHEN** multiple aircraft are active in the recent window
- **THEN** the panel shows per-aircraft rows with message ticks positioned by message-time arrival so the operator can identify bursty and dominant talkers quickly

#### Scenario: No recent aircraft activity is available
- **WHEN** no recent timing events with aircraft identity are available
- **THEN** the panel renders a clear waiting or empty state instead of stale aircraft rows

### Requirement: Micro-timeline row ordering is stable enough to read
The Timing page SHALL use a stable row-membership and ordering policy for the micro-timeline so rows do not reshuffle on every short-window traffic change.

#### Scenario: Activity fluctuates within the visible window
- **WHEN** aircraft message counts change moment to moment under live traffic
- **THEN** the panel preserves row stability using a slower ranking cadence and sticky membership instead of re-sorting rows on every frame

#### Scenario: A new aircraft becomes materially more active
- **WHEN** a new aircraft sustains enough activity to enter the configured visible set
- **THEN** the panel admits that aircraft without forcing all existing rows to reorder immediately
