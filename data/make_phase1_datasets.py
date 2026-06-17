import argparse
import os
import random


def write_points(path, points):
    with open(path, "w", encoding="utf-8") as f:
        for x, y in points:
            f.write(f"{x},{y}\n")


def build_edge_points():
    points = []

    # 20 points with identical y.
    for i in range(20):
        points.append((float(i), 10.0))

    # 10 exact duplicates.
    for _ in range(10):
        points.append((5.0, 5.0))

    # Rectangle corners for boundary checks.
    points.extend([
        (0.0, 0.0),
        (0.0, 10.0),
        (10.0, 0.0),
        (10.0, 10.0),
    ])

    # Small group that can represent a <=100 partition case.
    for i in range(50):
        x = 100.0 + (i % 10) * 0.01
        y = 100.0 + (i // 10) * 0.01
        points.append((x, y))

    # Large group to ensure >200 points are available.
    for i in range(240):
        x = 20.0 + (i % 40) * 0.05
        y = 30.0 + (i // 40) * 0.03
        points.append((x, y))

    # Very close neighbors for kNN sensitivity.
    base_x, base_y = 200.0, 200.0
    for i in range(20):
        delta = i * 1e-4
        points.append((base_x + delta, base_y + delta))

    return points


def build_syn_subset(n, seed):
    rng = random.Random(seed)
    points = []
    for _ in range(n):
        x = rng.uniform(-88.0, -87.0)
        y = rng.uniform(41.0, 42.0)
        points.append((x, y))
    return points


def main():
    parser = argparse.ArgumentParser(description="Generate Phase 1 datasets.")
    parser.add_argument("--syn-size", type=int, default=100000, help="Number of points for syn_subset.csv")
    parser.add_argument("--seed", type=int, default=20260422, help="Random seed for reproducible synthetic data")
    args = parser.parse_args()

    data_dir = os.path.dirname(os.path.abspath(__file__))

    edge_path = os.path.join(data_dir, "edge_points.csv")
    syn_path = os.path.join(data_dir, "syn_subset.csv")

    edge_points = build_edge_points()
    syn_points = build_syn_subset(args.syn_size, args.seed)

    write_points(edge_path, edge_points)
    write_points(syn_path, syn_points)

    print(f"Wrote {len(edge_points)} rows to {edge_path}")
    print(f"Wrote {len(syn_points)} rows to {syn_path}")


if __name__ == "__main__":
    main()
