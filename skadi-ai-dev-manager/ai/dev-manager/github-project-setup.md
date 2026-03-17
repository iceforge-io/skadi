# Skadi AI Development Manager - GitHub Project Setup Guide

## Purpose
Define GitHub Issues/Projects/labels so an AI manager can orchestrate Claude workers safely.

## Core Principles
- GitHub = source of truth
- Stories = schedulable unit
- Project fields = operational state
- Labels = machine-readable hints
- Concurrency is first-class

## Project
Use: Skadi - Platform Roadmap (single board for planning + execution)

## Issue Types
- Epic, Story, Task, Bug

## Required Labels (families)
type:*, priority:*, lane:*, module:*, risk:*, ai:*, cg:*

## Required Project Fields
Status, Type, Lane, Module, Priority, AI Eligibility, Risk Level, Concurrency Group,
Parent Issue, Blocked By, PR Link, Worker ID, Branch Name, Retry Count, Last Manager Update

## Status Values
Backlog, Ready, Scheduled, Assigned to Worker, In Progress, Blocked, PR Open, In Review,
Ready to Merge, Merged, Done, Cancelled

## AI Runnable Criteria
- Status=Ready, AI Eligibility=Eligible
- Module, Risk, Priority, Lane, Concurrency Group set
- Acceptance criteria present
- No blockers

## Concurrency Rules
- Serialize: parent-build, sql-protocol, metadata-contracts
- Allow: docs-only, test-harness
- Avoid overlapping hot zones

## Hot Zones
- pom.xml
- skadi-sql-gateway/**/pgwire/**
- skadi-sql-gateway/**/metadata/**
- skadi-core/**/cache/**

## Definition of Ready
Clear AC, modules, files, risk, concurrency group, dependencies

## Definition of Done
PR merged, tests pass, cleanup complete
