# Contributing to `dataframe`

Thank you for your interest in contributing to **hsky**, a Haskell dataframe library built for speed, clarity, and expressive data analysis. Contributions are welcome and encouraged! This guide will walk you through how to get started.

## Table of Contents

- [How to Contribute](#how-to-contribute)
- [Getting Started](#getting-started)
- [Documentation](#documentation)
- [Filing Issues](#filing-issues)
- [Submitting Pull Requests](#submitting-pull-requests)
- [Community Expectations](#community-expectations)

---

## How to Contribute

You can contribute in many ways:

- Filing bug reports
- Suggesting new features or APIs
- Improving documentation
- Submitting code improvements or new functionality
- Writing benchmarks or performance test cases
- Helping triage issues or reviewing PRs

---

## Getting Started

1. **Clone the repo**:

   ```bash
   git clone https://github.com/mchav/dataframe.git
   cd dataframe


2. **Install dependencies**:

   We keep the library intentionally light on dependencies to make installation less painful. All you need to do is install Haskell via GHCup.

3. **Code standards**
   * Prefer total functions and avoid partial ones unless necessary (with clear documentation).
   * Use strict folds when applicable to avoid space leaks.
   * Maintain performance discipline: avoid unnecessary allocations or intermediate structures.
   * Please run `./scripts/format.sh` and `./scripts/lint.sh` (or `./scripts/lint.sh --fix`) before submitting your code! 
   * Code is formatted with Fourmolu. Both HLint and Fourmolu run in CI and must pass.
4. **Testing**
   We use HUnit and QuickCheck for unit and property-based tests. Add tests for new features under `test/`. If you're fixing a bug, add a test that fails without your fix.

## Documentation

All public functions should have Haddock comments. If you're adding user-facing features, consider updating the README and examples/.

## Filing Issues

Please include:
  * A short, clear title
  * A minimal reproducible example (if applicable)
  * What you expected to happen vs. what actually happened
  * Your environment (OS, GHC version, etc.)

Feature suggestions are welcome too! Bonus points for a motivating use case.

## Submitting Pull Requests

  * Fork the repo.
  * Create a branch: `git checkout -b feature/my-awesome-feature`
  * Make your changes and commit with a clear message.
  * Push to your fork and open a PR.

Please include:
* A description of the problem and solution.
* Benchmarks or reasoning if the change impacts performance.
* Tests and documentation where applicable.

All PRs should pass the CI and be reviewed before merging.

## Community Expectations
We strive to maintain a respectful, inclusive, and collaborative environment. Be kind, curious, and constructive in your discussions.
