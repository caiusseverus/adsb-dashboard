## ADDED Requirements

### Requirement: Exact-point message-field stream exposes the metadata needed for spatial and analytical plots
The system SHALL expose recent exact-message events for the dedicated message-field page with the metadata needed for exact-point spatial and analytical visualisation, including optional `bearing_deg`, optional `range_nm`, optional `iid`, and trusted display-grade signal values when available.

#### Scenario: Spatially attributable message is retained for the message field
- **WHEN** a recent retained message has a publishable receiver-relative position at ingest time
- **THEN** the exact-point event includes the fields needed to place that message in bearing/range-based views without rejoining against mutable live aircraft state later

#### Scenario: Interrogator-attributable message is retained for the message field
- **WHEN** a recent retained message carries interrogator attribution
- **THEN** the exact-point event includes `iid` so the frontend can colour or filter by interrogator without a separate transport

#### Scenario: Signal is used as a displayed analytical variable
- **WHEN** signal is exposed for use as an axis or colour encoding on the dedicated message-field page
- **THEN** the value reflects readsb-aligned display semantics rather than the old raw byte scale

### Requirement: Message-field stream reuses one shared recent-event transport
The system SHALL provide the dedicated message-field page from one shared recent exact-message transport and MUST NOT open separate transports for each plot mode or control combination.

#### Scenario: Operator changes geometry or colour mode
- **WHEN** the operator changes message-field geometry, persistence, or colour/filter controls
- **THEN** the frontend updates from the existing local recent-event buffer and shared page stream without creating an additional widget-specific socket
