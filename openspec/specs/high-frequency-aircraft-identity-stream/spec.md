## ADDED Requirements

### Requirement: Timing page stream exposes aircraft identity on recent high-frequency events
The system SHALL expose recent high-frequency timing events on the existing Timing-page stream with inline aircraft identity so aircraft-centric views can be built from the same self-contained incremental event buffer.

#### Scenario: Client receives recent events for aircraft-centric panels
- **WHEN** a client is connected to the Timing-page websocket during live traffic
- **THEN** each recent event includes `icao` alongside the existing timing, DF, message-length, signal, and source metadata needed by aircraft-centric panels

#### Scenario: Stream remains self-contained across reconnects
- **WHEN** the Timing-page client reconnects and resumes ingesting incremental recent events
- **THEN** it can rebuild aircraft-centric views from the received events without needing a separate identity-mapping request or a join against mutable live aircraft state

### Requirement: Aircraft identity remains aligned to message-time timing events
The system SHALL attach aircraft identity to the same message-time event that carries Beast-relative arrival timing, rather than reconstructing identity later from potentially changed aircraft state.

#### Scenario: Aircraft state changes after a message was received
- **WHEN** later live-aircraft updates change the mutable aircraft record
- **THEN** the stored recent timing event still reflects the aircraft identity associated with that message at ingest time
