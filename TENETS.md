# Development Tenets

These are the tenets that this project tries to adhere to. Tenets are principles that should guide any decision making when developing on this project. I highly recommend watching [this talk by Steve Klabnik](https://www.infoq.com/presentations/rust-tradeoffs/) on Rust's principles and how that helps them approach tradeoffs.

These are not necessarily set-in-stone, but hopefully not too fluid either as these should guide our implementation, and inform our decisions when tradeoffs need to be made.

## User interaction latency is sacred

The system should try its hardest to return control to users as quickly as possible, and the users should receive feedback that their actions are acknowledged and persisted in the system.

## Core architectural solutions over ad-hoc features and performance enhancements

Whenever we face a difficult problem to solve, we must strive to solve it for the _general_ case instead of solving for the _specific_ case. This may involve deep, architectural changes, but doing this is preferably to patching specific solutions due to any reasons (like 'This is the most common use-case').

## Extensibility and flexibility in user interactions over infinite scaling

Solutions should always allow for the variety of user interactions possible in a spreadsheet. Infinite scaling to more data and more concurrent users and more performance should not come at the expense of removing the flexible interactions within a reasonable performance expectation.
