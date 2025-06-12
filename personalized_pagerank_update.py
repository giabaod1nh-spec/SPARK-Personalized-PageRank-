from pyspark.sql import SparkSession
import time
import math

# === 1. Khởi tạo SparkSession ===
spark = SparkSession.builder \
    .appName("PersonalizedPageRank") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# === 2. Đọc dữ liệu từ file ===
input_path = "/mnt/c/Users/Admin/Personalized-Pagerank-SPARK/graph.txt"
edges_rdd = sc.textFile(input_path) \
    .map(lambda line: line.strip().split()) \
    .filter(lambda x: len(x) == 2) \
    .map(lambda x: (x[0], x[1]))

# Kiểm tra dữ liệu đầu vào
sample_edges = edges_rdd.take(10)
if not sample_edges:
    print("Error: Graph.txt is empty or not formatted correctly.")
    spark.stop()
    exit(1)

# === 3. Khởi tạo danh sách đỉnh và đồ thị ===
vertices = edges_rdd.flatMap(lambda edge: [edge[0], edge[1]]).distinct().persist()
graph = edges_rdd.groupByKey().mapValues(list).persist()

# In danh sách đỉnh và đồ thị
print(f"\n Vertices: {vertices.collect()}")
print(f" Adjacency list:")
for node, neighbors in graph.collect():
    print(f"  {node} -> {neighbors}")

# === 4. Khởi tạo Personalized PageRank ===
source_page = "P1"
alpha = 0.15
tolerance = 1e-6  # Sai số hội tụ

# Kiểm tra source_page có trong đồ thị
if source_page not in vertices.collect():
    print(f"Error: Source page {source_page} not in graph.")
    spark.stop()
    exit(1)

# PR ban đầu: 1.0 cho source, 0.0 cho các đỉnh khác
ranks = vertices.map(lambda v: (v, 1.0 if v == source_page else 0.0))

# === 5. Xác định Dangling Nodes ===
nodes_with_outbound = graph.keys().collect()
dangling_nodes = vertices.filter(lambda v: v not in nodes_with_outbound).collect()
dangling_broadcast = sc.broadcast(set(dangling_nodes))
print(f"\n Dangling nodes: {dangling_nodes}")

# === 6. Vòng lặp Personalized PageRank với kiểm tra hội tụ ===
start_time = time.time()
i = 0

while True:
    i += 1
    print(f"\n Iteration {i}")

    # Lưu ranks hiện tại
    old_ranks_dict = dict(ranks.collect())

    # 6.1 Đóng góp từ các đỉnh có outbound links
    contribs = graph.join(ranks).flatMap(
        lambda x: [(dest, x[1][1] / len(x[1][0])) for dest in x[1][0]]
    )

    # 6.2 Tổng rank từ dangling nodes
    dangling_rank_sum = ranks.filter(lambda x: x[0] in dangling_broadcast.value) \
                            .map(lambda x: x[1]) \
                            .fold(0.0, lambda x, y: x + y)

    # 6.3 Cập nhật Personalized PageRank
    updated_ranks = contribs.reduceByKey(lambda x, y: x + y).map(
        lambda x: (
            x[0],
            alpha * (1.0 if x[0] == source_page else 0.0) + (1 - alpha) * x[1]
        )
    )

    # 6.4 Thêm đóng góp từ dangling vào source_page
    updated_ranks = updated_ranks.map(
        lambda x: (
            x[0],
            x[1] + (1 - alpha) * dangling_rank_sum if x[0] == source_page else x[1]
        )
    )

    # 6.5 Gộp lại với các node không được cập nhật
    updated_ranks_dict = dict(updated_ranks.collect())
    ranks = vertices.map(
        lambda x: (x, updated_ranks_dict.get(x, old_ranks_dict.get(x, 0.0)))
    )

    # 6.6 Kiểm tra hội tụ
    max_diff = ranks.join(ranks.map(lambda x: (x[0], old_ranks_dict.get(x[0], 0.0)))).map(
        lambda x: abs(x[1][0] - x[1][1])).reduce(max)
    print(f"  Max difference in iteration {i}: {max_diff:.10f}")

    if max_diff < tolerance:
        print(f"Converged after {i} iterations with max difference {max_diff:.10f}")
        break

    # In giá trị mỗi vòng
    for page, rank in sorted(ranks.collect(), key=lambda x: -x[1]):
        print(f"  Page {page}: {rank:.6f}")

end_time = time.time()

# === 7. Kết quả cuối cùng (chuẩn hoá) ===
print(f"\n Personalized PageRank after {i} iterations (normalized):")
result = ranks.collect()
total = sum(r for _, r in result) or 1.0
for page, rank in sorted(result, key=lambda x: -x[1]):
    print(f"Page {page}: {rank / total:.6f}")

print(f"\n Elapsed time: {end_time - start_time:.2f} seconds")

spark.stop()
