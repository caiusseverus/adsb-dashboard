## ADDED Requirements

### Requirement: Dedicated page renders one large exact-point message field
The frontend SHALL provide a dedicated page for the live message field that renders one full-width, high-detail exact-point plot rather than a collection of bucketed summary panels.

#### Scenario: Operator inspects fast transient message structure
- **WHEN** recent traffic contains rapid transient structure
- **THEN** the page shows actual message points with short persistence instead of reducing that structure to bucketed aggregates

### Requirement: Message-field page supports multiple projections of the same recent point stream
The dedicated page SHALL support both `polar` and `cartesian` views of the same recent exact-point message stream.

#### Scenario: Operator switches geometry
- **WHEN** the operator switches between `polar` and `cartesian`
- **THEN** the page reprojects the same recent exact-message field rather than requesting a different underlying data feed

### Requirement: Message-field page supports bounded persistence controls
The dedicated page SHALL support bounded persistence windows suitable for high-speed pattern inspection.

#### Scenario: Operator increases persistence window
- **WHEN** the operator selects a longer supported persistence window
- **THEN** older exact points remain visible until they age out of that selected window and then fade away cleanly

### Requirement: Timing page removes the shelved waterfall panel
The Timing page SHALL remove the shelved waterfall experiment so it remains focused on timing and observability panels.

#### Scenario: Operator opens the Timing page after the dedicated message-field page ships
- **WHEN** the operator views the Timing page
- **THEN** the page no longer includes the shelved waterfall and instead leaves exact-point experimental high-speed visualisation to the dedicated message-field page
