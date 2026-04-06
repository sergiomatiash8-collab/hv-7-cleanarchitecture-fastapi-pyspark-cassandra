┌─────────────────────────────────────────────────────────┐
│  PRESENTATION LAYER (HTTP)                              │
│  ├─ routes/ (FastAPI endpoints)                         │
│  ├─ middleware/ (HTTP concerns)                         │
│  ├─ schemas.py (Pydantic validation)                    │
│  └─ api.py (FastAPI app factory)                        │
└───────────────────┬───────────────────────────────────────┘
                    │ depends on
┌───────────────────▼───────────────────────────────────────┐
│  APPLICATION LAYER (Use Cases)                          │
│  ├─ handlers/ (One handler = One endpoint)              │
│  └─ dto.py (Data Transfer Objects)                      │
└───────────────────┬───────────────────────────────────────┘
                    │ depends on
┌───────────────────▼───────────────────────────────────────┐
│  DOMAIN LAYER (Business Logic)                          │
│  ├─ entities.py (Data classes: Review, Product)         │
│  ├─ repositories.py (Abstract interfaces)               │
│  └─ services.py (Use case logic)                        │
└───────────────────┬───────────────────────────────────────┘
      ↑ implemented by   │
      │                  │ implemented by
│ INFRASTRUCTURE LAYER (External Services)               │
│  ├─ cassandra/ (DB read/write)                         │
│  ├─ redis/ (Caching)                                   │
│  └─ spark/ (ETL)                                       │
└─────────────────────────────────────────────────────────┘

CONFIG LAYER (Settings & DI)
├─ settings.py (Environment config)
├─ container.py (Dependency injection)
└─ logging.py (Structured logging)


Layer 1: PRESENTATION (REST API)
Responsibility: HTTP request/response handling
Dependencies: application + FastAPI

Layer 2: APPLICATION (Use Case Handlers)
Responsibility: Orchestrate domain + infrastructure
Dependencies: domain + infrastructure


Layer 3: DOMAIN (Business Logic)
Responsibility: Define WHAT the system does

Layer 4: INFRASTRUCTURE (How to Access Data)
Responsibility: Implement repositories, manage external services
Dependencies: cassandra-driver, redis, pyspark
Changes when: Technology choices change (Cassandra → Another DB)

