# Amazon Athena Cost & Performance Analysis: CSV vs. Partitioned Parquet

In Amazon Athena, billing is directly based on **the amount of data scanned** by queries at **$5.00 per TB ($0.005 per GB)** scanned (with a 10 MB minimum per query).

This benchmark compares querying the uncompressed raw CSV table versus the partitioned Snappy-compressed Parquet table produced by our PySpark pipeline.

---

## 1. Storage Comparison

| Storage Metric | Raw CSV (`raw/`) | Curated Parquet (`curated/`) | Efficiency Gain |
|---|---|---|---|
| **File Format** | Plaintext uncompressed CSV | Columnar Apache Parquet + Snappy | Columnar encoding + Snappy compression |
| **Total Size on S3** | ~4.7 MB (for 30,000 rows) | ~0.9 MB | **~80.8% storage reduction** |
| **Directory Layout** | Single flat file | Hive-partitioned (`order_year=YYYY/order_month=M/`) | Fine-grained partition pruning |

---

## 2. Query Scan Comparison

Consider a query analyzing a single month's sales trends:
```sql
SELECT category, SUM(sales), SUM(profit)
FROM sales_curated
WHERE order_year = 2024 AND order_month = 6
GROUP BY category;
```

### Scenario A: Querying Raw CSV
- **Data Scanned:** Because CSV is a row-oriented format, Athena **must scan the entire 4.7 MB file** from beginning to end, parsing every single row and unused column (customer names, postal codes, shipping dates, product IDs, etc.).
- **Partition Pruning:** None. Entire dataset scanned.
- **Estimated Data Scanned:** 4.7 MB (billed at 10 MB minimum).

### Scenario B: Querying Partitioned Parquet
1. **Partition Pruning:** Athena's query engine inspects S3 directory prefixes and **only reads the specific partition** (`order_year=2024/order_month=6/`). All other 47 months are skipped completely.
2. **Column Projection (Columnar Format):** Within the partition, Athena reads **only the byte ranges of the selected columns** (`category`, `sales`, `profit`). Unreferenced text columns are never touched.
3. **Snappy Compression:** The columnar data is compressed, further reducing bytes transferred over the network from S3 to Athena workers.
- **Estimated Data Scanned:** ~18 KB - 40 KB (billed at 10 MB Athena query floor).

---

## 3. Scale Projection: What happens at 1 TB of Raw Data?

At enterprise scale (e.g., 1 TB raw sales data per year, 100 analytical dashboard queries per day):

| Metric | Raw CSV Approach | Partitioned Parquet Approach | Savings |
|---|---|---|---|
| **Storage Cost (S3 Standard)** | \$23.00 / month | \$4.60 / month (80% compression) | **\$18.40 / month (80%)** |
| **Avg Data Scanned / Query** | 1,000 GB (1 TB) | ~15 - 30 GB (partition pruning + column projection) | **97 - 98.5% less data** |
| **Query Cost per Query** | \$5.00 | \$0.075 - \$0.15 | **~98% cost reduction** |
| **Monthly Query Cost (3,000 queries)** | \$15,000.00 | \$225.00 - \$450.00 | **Over \$14,500 / month saved** |
| **Query Runtime** | 45 - 90 seconds | 1.5 - 3 seconds | **~20x faster execution** |

---

## 4. Key Takeaways

1. **Partition Pruning** reduces I/O by skipping unneeded partitions.
2. **Columnar Projection** reads only queried fields, making analytical aggregations (`SUM`, `AVG`) lightweight.
3. **Snappy Compression** reduces S3 storage costs and network latency while maintaining fast decompression speeds in distributed query engines.
