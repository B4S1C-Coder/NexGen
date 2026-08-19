# RECOVERY.md: Strategic Blueprint for the NexGen Observability Framework

## Table of Contents
1.  **Prologue: The Fallacy of "Done"**
2.  **Core Philosophy: "Proper" vs "Working"**
3.  **Architectural Renaissance: Re-imagining the Multi-Agent Framework**
4.  **The Data Substrate: Rethinking Storage and Retrieval**
5.  **Quality Assurance: A Manifesto for Reliability**
6.  **Security Posture and Compliance**
7.  **Observability: Observing the Observability Tool**
8.  **Infrastructure, CI/CD, and Deployment Logistics**
9.  **Human Factors: Team Structure and Cognitive Load**
10. **The Road Ahead: Future-Proofing the AI Layer**

---

## 1. Prologue: The Fallacy of "Done"

The current state of the NexGen project serves as a stark reminder of the dangers inherent in checklist-driven development, particularly when applied to complex, non-deterministic systems like multi-agent AI frameworks. The `TASKS.md` document presents a facade of completion—dozens of checkboxes meticulously ticked, suggesting a linear progression from conception to production-readiness. However, as the detailed `STATUS.md` audit revealed, this apparent progress is a dangerous illusion. 

We have achieved what might be termed "superficial completeness." Code has been written, endpoints have been defined, and dependencies have been declared. Yet, the connective tissue—the critical integration points that allow data and intent to flow between the Orchestrator, the Query Pipeline, and the RAG Pipeline—is completely absent. We possess the organs of a system, but lack the circulatory system required to sustain life. 

Furthermore, the internal implementation of these individual organs is brittle. Relying on regular expressions for complex KQL parsing, failing to handle critical consensus errors in multi-agent debates, and maintaining duplicate, conflicting implementations within the same file are symptoms of a deeper malaise. 

This `RECOVERY.md` document is not a tactical patch list. It is not a sequence of specific bug fixes or minor refactors. Instead, it is a strategic blueprint designed to elevate NexGen from a fragile, disjointed prototype to a robust, enterprise-grade observability framework. It represents a fundamental shift in our approach—a move away from simply "getting it to work" towards building a system that is resilient, maintainable, scalable, and fundamentally "proper."

---

## 2. Core Philosophy: "Proper" vs "Working"

The distinction between a "working" project and a "proper" project is profound and often misunderstood. A "working" project satisfies the immediate functional requirements under ideal conditions. It can handle the "happy path" and demonstrate basic capability. A "proper" project, however, is built with the understanding that ideal conditions are a myth. 

### 2.1 The "Working" Project Reality
*   **Focus:** Immediate feature delivery.
*   **Architecture:** Ad-hoc, often tightly coupled, prioritizing speed over structure.
*   **Testing:** Minimal, often confined to unit tests for isolated functions, if present at all.
*   **Error Handling:** Reactive, relying on generic catch-all blocks that swallow critical context.
*   **Maintainability:** Low. The system is a black box, understandable only to its original creators (and sometimes not even them).
*   **Failure Mode:** Catastrophic. Unexpected inputs or environmental changes lead to ungraceful degradation or complete systemic collapse.

### 2.2 The "Proper" Project Imperative
*   **Focus:** Long-term sustainability, reliability, and clear systemic boundaries.
*   **Architecture:** Intentional, modular, with well-defined interfaces and strict adherence to separation of concerns.
*   **Testing:** Comprehensive, encompassing unit, integration, end-to-end, and adversarial testing (especially critical for LLM-driven components).
*   **Error Handling:** Proactive, structured, and informative. Errors are treated as first-class entities that guide system recovery and inform operators.
*   **Maintainability:** High. The system is transparent, extensively documented, and self-describing through robust observability.
*   **Failure Mode:** Graceful degradation. The system anticipates failure, isolates faults, and degrades predictably, prioritizing core functionality and user communication.

Our goal for NexGen is to transition it entirely into the realm of the "proper" project. This requires a cultural shift within the development process. Every line of code, every architectural decision, and every integration point must be evaluated against these principles. We must optimize for the system's ability to survive reality, not just its ability to pass a superficial demonstration.

---

## 3. Architectural Renaissance: Re-imagining the Multi-Agent Framework

The current architecture, as conceptualized, is sound: a central Orchestrator managing specialized pipelines (NL-to-KQL for structured logs, RAG for unstructured knowledge). However, the implementation has failed to respect the boundaries and responsibilities inherent in this design. We must re-establish these boundaries with absolute rigidity.

### 3.1 The Principle of Strict Isolation
The Orchestrator, Query Pipeline, and RAG Pipeline must be treated as truly independent services, even if deployed concurrently. They must share nothing but the data defined in their explicit JSON contracts.
*   **No Shared State:** The services must not rely on shared databases (like a common Redis instance) for internal state management unless explicitly required for cross-service coordination (which should be minimized).
*   **Contract-Driven Development:** The Pydantic models in `nexgen_shared` are the ultimate source of truth. Any change to a service's input or output must begin with a formalized update to these schemas.
*   **Network Boundaries as a Feature:** Even in local development, the services must communicate via HTTP (or gRPC). This forces developers to confront latency, serialization, and network failure modes early in the development cycle.

### 3.2 The Orchestrator: From Stub to Conductor
The `MasterOrchestrator` must be elevated from a disconnected class to the active, coordinating brain of the system.
*   **Lifecycle Management:** The Orchestrator's initialization must be robust. It must gracefully handle the absence of optional dependencies (like Redis) and fail fast if critical dependencies are unreachable.
*   **DAG Execution:** The execution of the Directed Acyclic Graph must be resilient. It must implement sensible timeouts, retry mechanisms with exponential backoff, and circuit breakers to prevent cascading failures when downstream services are struggling.
*   **Contextual Awareness:** The Orchestrator must not blindly trust the output of the pipelines. It must evaluate the confidence scores and error codes returned by the Query and RAG services, adjusting its synthesis strategy accordingly.

### 3.3 The Query Pipeline: Replacing Brittle Parsing with Formal Syntax
The current reliance on regular expressions for parsing KQL is a fatal flaw. It is fundamentally impossible to reliably parse a recursive language using regular expressions.
*   **Formal Parsing:** The pipeline must adopt a formal parser generator (like ANTLR, Lark, or PyParsing) to construct a true Abstract Syntax Tree (AST) from the natural language generated KQL.
*   **AST Validation:** Validation must occur on the AST, not the raw string. This allows for rigorous checks against schema validity, operator precedence, and malicious query injection.
*   **Deterministic Transpilation:** The conversion from the validated KQL AST to the Elasticsearch Query DSL must be deterministic and fully covered by unit tests representing complex nested queries.

### 3.4 The RAG Pipeline: Enforcing Consensus and Resolving Conflicts
The RAG pipeline's current approach to conflict resolution—silently dropping chunks when the debate agent fails—is unacceptable.
*   **Strict Error Propagation:** When multi-agent debate fails to reach a consensus, the system must loudly report this failure (e.g., via the `E007` code) back to the Orchestrator. The Orchestrator can then decide how to present this ambiguity to the user.
*   **Deterministic Fallbacks:** While LLM-driven compaction (LLMLingua-2) is powerful, the system must implement robust, deterministic fallbacks (like simple extractive summarization or truncation) that guarantee execution even when the LLM is unavailable or times out.

---

## 4. The Data Substrate: Rethinking Storage and Retrieval

NexGen relies on complex data stores: Elasticsearch for logs, Qdrant for vector embeddings, and Redis for session state. The current setup is heavyweight and difficult to manage locally. We must introduce flexibility and robustness into our data strategy.

### 4.1 The Necessity of the "Mock Pathway"
The inability to run the system without the entire Docker Compose stack is a massive impediment to development velocity.
*   **Interface-Driven Storage:** All interactions with Elasticsearch, Qdrant, and Redis must be abstracted behind clean, asynchronous interfaces.
*   **In-Memory Implementations:** We must provide fully functional, in-memory implementations of these interfaces for development and unit testing. For example, a simple Python dictionary can mock Redis; a naive string-matching implementation can mock Elasticsearch for basic tests.
*   **Fixture-Driven Integration:** The mock pathway must be capable of loading predefined JSON fixtures representing complex log scenarios or conflicting runbook entries, allowing developers to test the Orchestrator's reasoning logic in isolation.

### 4.2 Data Integrity and PII
The current approach to PII masking (regex replacement) is inadequate for a system designed to handle sensitive organizational logs.
*   **Schema-Aware Masking:** PII masking should leverage the schema context. If a field is known to contain sensitive data (e.g., `user.email`), the entire field should be hashed or redacted at the point of ingestion or retrieval.
*   **Audit Logging:** Every interaction with the data layer, particularly queries involving user data, must be rigorously audit-logged, recording the query intent, the generated KQL, and the specific records retrieved.

---

## 5. Quality Assurance: A Manifesto for Reliability

The absence of a functional test suite is perhaps the most glaring indicator of NexGen's current state. A "proper" project is defined by its testing culture.

### 5.1 The Testing Pyramid
We must implement a rigorous testing pyramid, shifting the balance from heavy end-to-end tests to fast, deterministic unit tests.
*   **Unit Tests:** Every function, parsing rule, and schema validation must be covered. These tests must run in milliseconds and require zero external dependencies (no Docker, no network calls).
*   **Integration Tests:** These tests must verify the interaction between components (e.g., the Query service generating KQL and successfully querying a localized, mocked Elasticsearch instance).
*   **Contract Tests:** We must introduce contract testing (e.g., using Pact) to ensure that the Orchestrator's expectations of the Query and RAG APIs match the actual implementations.

### 5.2 Adversarial LLM Testing
Standard testing methodologies are insufficient for non-deterministic LLM components.
*   **Evaluation Datasets:** We must construct rigorous evaluation datasets (golden sets) containing complex natural language queries and their expected KQL or RCA outputs.
*   **Automated Evaluation:** We must implement automated evaluation pipelines (like RAGAS for the RAG pipeline) that continuously measure metrics like Context Precision, Faithfulness, and Schema Hallucination Rate against the golden sets.
*   **Fuzzing the Prompt:** The reasoning agents must be subjected to prompt fuzzing—injecting adversarial inputs, contradictory context, and nonsensical queries—to ensure they degrade gracefully and do not confidently hallucinate incorrect RCAs.

---

## 6. Security Posture and Compliance

As an observability tool, NexGen will have access to highly sensitive organizational data. Security cannot be an afterthought.

### 6.1 Principle of Least Privilege
*   **Service Accounts:** Each service (Orchestrator, Query, RAG) must operate under dedicated service accounts with the absolute minimum permissions required. The Query service, for example, should only have read access to specific, non-sensitive Elasticsearch indices.
*   **Index-Level Access Control:** The system must enforce strict index-level access control based on the user's session context. The LLM must never be allowed to generate a query that bypasses these restrictions.

### 6.2 Injection Prevention
*   **KQL Injection:** The translation from natural language to KQL is a massive attack vector. The formal AST parser discussed in Section 3.3 must strictly sanitize all inputs and ensure that users cannot inject arbitrary KQL clauses (e.g., attempting to drop indices or extract unauthorized fields).
*   **Prompt Injection:** The Master Orchestrator must be resilient against prompt injection attacks embedded within the retrieved logs or runbooks. System prompts must be structured to prioritize their own instructions over the retrieved context.

---

## 7. Observability: Observing the Observability Tool

It is ironic that an observability tool lacks internal observability. We must instrument NexGen comprehensively.

### 7.1 Distributed Tracing
*   **OpenTelemetry:** The existing OpenTelemetry setup must be expanded. Every request must be assigned a unique `trace_id` that propagates across the Orchestrator, Query, and RAG boundaries.
*   **LLM Span Tracking:** Every call to an LLM must be tracked as a distinct span, recording the prompt payload, the response, token usage, and latency. This is critical for debugging hallucinations and optimizing costs.

### 7.2 Actionable Metrics
*   **Beyond Latency:** While latency is important, we must track business-level metrics: the rate of KQL syntax errors, the frequency of unresolved RAG conflicts, the average confidence score of generated RCAs, and the ratio of successful vs. failed LLM reasoning cycles.
*   **Alerting Thresholds:** Meaningful alerting thresholds must be defined. A sudden spike in KQL generation failures indicates a systemic issue with the LLM or the schema cache, requiring immediate operator intervention.

---

## 8. Infrastructure, CI/CD, and Deployment Logistics

The current deployment strategy (a massive `docker-compose.yml`) is unsuitable for production or efficient development.

### 8.1 Infrastructure as Code (IaC)
*   **Declarative Infrastructure:** All infrastructure (Elasticsearch clusters, Qdrant instances, Redis caches) must be defined declaratively using tools like Terraform or Pulumi.
*   **Environment Parity:** We must strive for absolute parity between development, staging, and production environments. The "Mock Pathway" is for local logic testing; staging must use the exact infrastructure configurations as production.

### 8.2 Continuous Integration / Continuous Deployment (CI/CD)
*   **Automated Gates:** Code must not be merged into the main branch unless it passes all unit tests, integration tests, linting rules (Ruff, MyPy), and does not decrease code coverage.
*   **LLM Evaluation in CI:** The automated evaluation suites (RAGAS, KQL generation accuracy) must run as part of the CI pipeline. A degradation in model accuracy must break the build just as surely as a compilation error.

---

## 9. Human Factors: Team Structure and Cognitive Load

Building a "proper" project requires acknowledging the limitations of human cognition. The system must be designed for maintainability.

### 9.1 Documentation as Code
*   **Living Architecture:** Documents like `AGENTS.md` and this `RECOVERY.md` must be treated as living artifacts. They must evolve alongside the codebase.
*   **Decision Records:** Every significant architectural decision (e.g., choosing PyParsing over ANTLR, changing the debate resolution strategy) must be documented in an Architecture Decision Record (ADR), detailing the context, options considered, and the rationale for the choice.

### 9.2 Modularity and Onboarding
*   **Clear Boundaries:** A new developer should be able to understand and contribute to the Query pipeline without needing to understand the intricacies of the Master Orchestrator's Tree-of-Thoughts logic.
*   **Standardized Environments:** Setting up the development environment must be frictionless. The "nuked environment" scenario must be impossible. A single command (e.g., `make dev`) should spin up the necessary mocks and launch the services natively.

---

## 10. The Road Ahead: Future-Proofing the AI Layer

The landscape of Large Language Models is evolving rapidly. NexGen must be built to adapt to these changes without requiring massive rewrites.

### 10.1 Model Agnosticism
*   **Abstracted Inference:** The system must not be tightly coupled to OpenAI, Ollama, or any specific provider. The inference layer must be abstracted, allowing us to swap models (e.g., moving from Llama 3 to a specialized, fine-tuned local model) via simple configuration changes.
*   **Prompt Management:** Prompts are code. They must be version-controlled, tested, and managed independently of the business logic. We must consider adopting a prompt management system to facilitate A/B testing of different reasoning strategies.

### 10.2 Continuous Learning
*   **Feedback Loops:** The system must incorporate user feedback. When an operator corrects a generated RCA, that correction must be captured and fed back into the evaluation datasets or used for future fine-tuning of the intent classification and KQL generation models.

---

## Conclusion: The Commitment to "Proper"

Recovering NexGen from its current state is not a trivial undertaking. It requires discarding the illusion of progress provided by checked boxes and confronting the reality of the codebase. By embracing the principles outlined in this blueprint—strict architectural boundaries, rigorous testing, deterministic logic, and comprehensive observability—we can transform NexGen from a fragile prototype into the robust, enterprise-grade observability framework it was designed to be. The focus must shift from "getting it done" to "building it right."
