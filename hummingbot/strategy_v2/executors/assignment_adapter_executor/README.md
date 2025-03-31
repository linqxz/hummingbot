# Assignment Adapter Executor

## Overview
The AssignmentAdapterExecutor is a specialized adapter that allows the PositionExecutor to handle exchange assignments efficiently. It bridges the gap between exchange assignments and the existing position management logic in the PositionExecutor.

## Purpose
This adapter serves as a minimally invasive solution to reuse PositionExecutor's robust position management capabilities for assignments, without having to maintain and synchronize two parallel implementations (AssignmentExecutor and PositionExecutor).

## Implementation Design
The AssignmentAdapterExecutor subclasses PositionExecutor but overrides key methods to:

1. Skip the position opening phase (since assignments arrive as already-open positions)
2. Initialize with a "fake" filled open order that represents the assignment
3. Focus solely on position management and closure
4. Track assignment-specific information for reporting and analytics

This approach offers several advantages:
- Reuses the battle-tested PositionExecutor code
- Maintains consistent position management behavior across both regular positions and assignments
- Reduces code duplication and maintenance overhead
- Automatically inherits future improvements to PositionExecutor

## Usage
The AssignmentManagerController automatically creates AssignmentAdapterExecutors when it detects assignments from the exchange. These executors then manage the assigned positions according to the controller's configuration. 