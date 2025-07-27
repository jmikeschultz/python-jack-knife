## [0.5.0] - 2024-04-02
### Changed
- All pipeline components now use generator-style `__iter__()`
- All pipeline components do __init__(ParsedToken, Usage), Pipe, Sink use add_source
- Removed all `.next()` methods
- Refactored sinks and sources to be streaming and consistent

