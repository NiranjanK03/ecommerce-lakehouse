# LinkedIn Post

---

We were designing an architecture to separate compute and storage for faster analytics. Everyone wanted Iceberg for ACID guarantees. But Iceberg doesn't make queries fast — it makes data reliable. Those are two different problems.

So I built a simulation to find out what solving both actually looks like.

Same Silver Iceberg tables. 5 million rows. Same 5 analytical queries. Three scenarios:

— Trino 435 reading Iceberg directly: **12,938ms**
— StarRocks reading the same Iceberg files: **1,318ms → 9.8× faster**
— StarRocks pre-materialised Gold views: **47ms → 273× faster**

This runs on a single MacBook — StarRocks and Trino competing for the same CPU and memory. In a real deployment with dedicated nodes, the gap is wider.

The benchmark isn't the point. The point is that ACID correctness and query speed are not a tradeoff. They're two separate layers solving two separate problems. Iceberg gives your analysts data they can trust. StarRocks gives them answers before they switch tabs.

The engineering challenge nobody talks about: pre-materialisation only works if you know your query patterns. At a startup those patterns change every quarter. Getting that feedback loop right — between the analysts who consume data and the engineers who build the pipeline — is harder than any of the infrastructure decisions.

Full architecture, decisions, and methodology:
github.com/NiranjanK03/ecommerce-lakehouse

---

*Tags to add when posting: #DataEngineering #ApacheIceberg #StarRocks #DataLakehouse #Analytics #OpenSource*
