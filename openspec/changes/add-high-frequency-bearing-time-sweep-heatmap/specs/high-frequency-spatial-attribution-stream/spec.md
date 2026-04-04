## ADDED Requirements

### Requirement: Timing page stream exposes spatially attributable recent-message events
The system SHALL expose recent high-frequency timing events on the existing Timing-page stream with an optional inline `bearing_deg` field for messages whose aircraft has a publishable receiver-relative position at message ingest time.

#### Scenario: Timing page receives spatially attributable event tuples
- **WHEN** a client is connected to the Timing-page stream during live traffic and a message belongs to an aircraft with a publishable receiver-relative position
- **THEN** the incremental timing event includes the fields needed to place that message into a bearing-based panel without querying live aircraft state afterward

#### Scenario: Message lacks trustworthy spatial attribution
- **WHEN** a recent message does not have a publishable receiver-relative position at ingest time
- **THEN** the event remains in the recent timing stream with empty spatial fields instead of being backfilled later from mutable aircraft state

### Requirement: Spatially attributable events stay aligned to the existing Beast-relative timing domain
The system SHALL preserve the current Beast-relative timing domain for spatially attributable recent-message events and MUST omit incompatible timing sources rather than mixing clocks inside the same panel stream.

#### Scenario: Incompatible timing source is encountered
- **WHEN** a message does not belong to the primary Beast timing domain used by the Timing page
- **THEN** the backend excludes it from the recent timing event stream instead of assigning it misleading relative timing and spatial attribution
