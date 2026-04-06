Architecture Pattern: Clean Architecture (Hexagonal)
Core Principle: One Class = One Responsibility (SOLID)

1. PRESENTATION (HTTP Entry)
   Client → GET /products/123/reviews
   ↓
   FastAPI Route Handler (reviews.py)
   ├─ Validate input (Pydantic)
   ├─ Dependency Injection (Container.get_product_reviews_handler())
   ↓

2. APPLICATION (Use Case Orchestration)
   GetProductReviewsHandler.handle(product_id="123")
   ├─ Check Redis cache (CacheKeyBuilder.product_reviews("123"))
   ├─ If MISS:
   │  ├─ Call domain service
   │  │  ↓
   │  └─ Call infrastructure repo
   │     ↓

3. INFRASTRUCTURE (Data Access)
   CassandraReviewRepository.get_by_product_id("123")
   ├─ Use CassandraConnection (get session from pool)
   ├─ Execute ReviewQueries.GET_BY_PRODUCT with params
   ├─ Convert rows → Review entities
   ├─ Return List[Review]
   ↓

4. APPLICATION (Response Assembly)
   GetProductReviewsHandler
   ├─ Convert Review entities → ReviewDTO
   ├─ Cache result in Redis (5 min TTL)
   ├─ Return List[dict]
   ↓

5. PRESENTATION (HTTP Response)
   FastAPI
   ├─ Serialize response (JSON)
   ├─ Add headers (Cache-Control, X-Process-Time)
   ├─ Compress with gzip
   ↓
   Client ← 200 OK + JSON array

   
   SOLID Principles
   Single Responsibility Principle
   Open/Closed Principle
   Liskov Substitution Principle
   Interface Segregation Principle
   Dependency Inversion Principle



   Dependencies Between Layers
   Presentation (routes, middleware, schemas)
    ↓ depends on
Application (handlers, DTOs)
    ↓ depends on
Domain (entities, repositories, services) + Infrastructure (cassandra, redis, spark)
    ↓ depends on
External Libraries (cassandra-driver, redis, pyspark)