# Skadi

Skadi is a Java / Spring Boot–based backend service designed to provide a clean, extensible foundation for data-centric services and APIs.  
It emphasizes clear domain boundaries, testability, and production-ready architecture, making it suitable both for standalone use and as part of a larger enterprise platform.

---

## Key Goals

- Provide a **clean Spring Boot service skeleton** with well-defined layers
- Enable **rapid feature development** without sacrificing structure
- Support **enterprise-grade concerns** such as configuration management, observability, and testability
- Remain **technology-agnostic** at the domain level (easy to evolve storage, messaging, or integrations)

---

## Core Functionality

Skadi currently provides:

- A Spring Boot application entry point
- REST API layer for external interaction
- Service layer encapsulating business logic
- Clear separation between API, domain, and infrastructure concerns
- Configuration via Spring profiles and externalized properties
- Structured logging and error handling
- Unit and integration testing support

The project is intentionally designed so that additional capabilities—such as persistence, messaging, or external system integration—can be added incrementally without refactoring the core structure.

---

## High-Level Architecture

Skadi follows a layered architecture aligned with standard Spring Boot best practices:
```
┌────────────────────────────┐
│            API Layer       │
│    (Controllers / DTOs)    │
└─────────────▲──────────────┘
              │
┌─────────────┴──────────────┐
│ Service Layer              │
│ (Business Logic)           │
└─────────────▲──────────────┘
              │
┌─────────────┴──────────────┐
│ Domain / Model │
│ (Core concepts & rules) │
└─────────────▲──────────────┘
              │
┌─────────────┴──────────────┐
│   Infrastructure Layer     │
│    (Config, persistence,   │
│     external integrations) │
└────────────────────────────┘
```
### Architectural Principles

- **Thin controllers**: HTTP logic only, no business rules
- **Service-centric logic**: Business behavior lives in services
- **Domain-first thinking**: Core concepts are not coupled to frameworks
- **Replaceable infrastructure**: Databases, messaging, and integrations can evolve independently

---

## Project Structure

```
skadi/
├── src/main/java
│ └── com.example.skadi
│ ├── SkadiApplication.java
│ ├── api/ # REST controllers & request/response DTOs
│ ├── service/ # Business logic
│ ├── domain/ # Core domain models
│ ├── config/ # Spring configuration
│ └── infrastructure/ # Persistence, clients, adapters
│
├── src/test/java
│ └── com.example.skadi
│ ├── unit/
│ └── integration/
│
├── pom.xml
└── README.md
```
---

## Technology Stack

- **Java** (17+ recommended)
- **Spring Boot**
    - Spring Web
    - Spring Context / Configuration
- **Maven** for build and dependency management
- **JUnit 5** for testing
- **Mockito** for mocking (where appropriate)

---

## Configuration & Profiles

Skadi uses standard Spring Boot configuration mechanisms:

- `application.yml` / `application.properties`
- Profile-specific overrides (e.g. `application-dev.yml`, `application-prod.yml`)
- Environment variable support for deployment environments

This allows the same artifact to be promoted across environments with no code changes.

---

## Testing Strategy

- **Unit tests**
    - Service-level tests with mocked dependencies
    - Fast feedback, no Spring context when possible
- **Integration tests**
    - Spring context loaded
    - End-to-end validation of wiring and configuration

The project structure encourages keeping tests close to the code they validate.

---

## Running Locally

```bash
mvn clean spring-boot:run
