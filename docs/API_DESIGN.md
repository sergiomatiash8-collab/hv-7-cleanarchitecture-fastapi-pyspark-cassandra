Content-Type: application/json
Caching: Redis (TTL: 5 minutes)

Endpoints Overview
Reviews Endpoints
1. GET /products/{product_id}/reviews
2. GET /products/{product_id}/reviews?star_rating={1-5}
3. GET /customers/{customer_id}/reviews

Analytics Endpoints
4. GET /analytics/top-products?period={YYYY-MM}&limit={N}
5. GET /analytics/top-haters?period={YYYY-MM}&limit={N}
6. GET /analytics/top-backers?period={YYYY-MM}&limit={N}

Health Endpoints

7. GET /health/live
8. GET /health/ready


Response Formats
Success (200-299)
Client Error (400-499)
Server Error (500-599)



Error Cases:
StatusErrorReason

404NOT_FOUNDProduct has no reviews
400BAD_REQUESTInvalid product_id format
429RATE_LIMITED>100 requests/minute
503SERVICE_UNAVAILABLECassandra down

