## ADDED Requirements

### Requirement: Timing page shows a bearing-time sweep heatmap
The Timing page SHALL render a high-frequency bearing-time density panel that visualizes recent spatially attributable messages by bearing sector over the current rolling timing window.

#### Scenario: Operator inspects directional traffic concentration
- **WHEN** recent attributable messages concentrate in specific azimuth sectors
- **THEN** the panel shows that concentration as localized intensity in the corresponding bearing bands over time

#### Scenario: No recent attributable messages are available
- **WHEN** the Timing page has no recent timing events with trustworthy spatial attribution
- **THEN** the bearing-time panel renders a clear waiting or empty state instead of stale heatmap cells

### Requirement: Bearing-time panel reuses the shared Timing-page stream
The Timing page SHALL derive the bearing-time heatmap from the existing shared incremental timing-event buffer and MUST NOT open a dedicated widget-specific transport.

#### Scenario: Timing window changes
- **WHEN** the operator changes the Timing page window controls
- **THEN** the bearing-time panel updates from the shared page stream and existing client buffer without creating a separate socket
