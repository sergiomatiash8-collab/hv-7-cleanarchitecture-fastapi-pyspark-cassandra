from gevent import monkey
monkey.patch_all()

from cassandra.cluster import Cluster
from cassandra.io.geventreactor import GeventConnection

def test_fetch_product_reviews():
    try:
       
        cluster = Cluster(['127.0.0.1'], port=9042)
        cluster.connection_class = GeventConnection
        session = cluster.connect('amazon_reviews')
        
        
        sample = session.execute("SELECT product_id FROM reviews_by_product LIMIT 1").one()
        
        if not sample:
            print(" Table reviews_by_product is empty!")
            return

        test_id = sample.product_id
        print(f"🔎 Testing search for product ID: {test_id}")

        
        query = f"SELECT * FROM reviews_by_product WHERE product_id = '{test_id}'"
        rows = session.execute(query)

        print(f"\n Search results:")
        for i, row in enumerate(rows):
            print(f"{i+1}. Rating: {row.star_rating} | Date: {row.review_date} | ID: {row.review_id}")
        
        cluster.shutdown()
        
    except Exception as e:
        print(f" Error during query: {e}")

if __name__ == "__main__":
    test_fetch_product_reviews()