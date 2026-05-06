# LiLIS 项目完整文档

## Lightweight Distributed Learned Index for Spatial Data (轻量级分布式学习型空间数据索引系统)

---

## 1. 项目概述

LiLIS (Lightweight Learned Index for Spark) 是一个基于 Apache Spark 的分布式学习型空间数据索引系统。它将学习型搜索技术（Learned Index）与分布式计算相结合，通过样条插值（Spline Interpolation）拟合空间数据的累积分布函数（CDF），实现对空间数据的高效索引和查询。相比传统空间索引方法，性能提升可达 2-3 个数量级。

### 1.1 核心思想

传统空间索引（R-Tree、KD-Tree 等）依赖树形结构进行搜索，而 LiLIS 的核心思想是：
1. **将索引视为一个函数**：索引本质上是从 key 到 position 的映射函数
2. **用样条曲线拟合 CDF**：通过 Taut String 算法将数据的 CDF 压缩为一条分段线性的样条曲线
3. **用样条进行位置预测**：查询时，通过样条曲线快速估算目标数据的位置，然后在估算位置附近进行微调

### 1.2 技术栈

| 组件 | 版本 |
|------|------|
| Java | 8+ |
| Apache Spark | 3.0.0 |
| Scala | 2.12 |
| Apache Sedona | 1.2.0-incubating |
| JTS (Java Topology Suite) | 1.19.0 |
| Maven | 4.0 |

---

## 2. 项目结构

```
learned-index-spark/
├── pom.xml                          # Maven 构建配置
├── README.md                        # 项目简介
├── docs/                            # GitHub Pages 项目主页
│   ├── index.html
│   └── static/
│       ├── css/                     # 样式文件
│       ├── images/                  # 论文图片 (框架图、实验结果等)
│       └── js/                      # 前端脚本
└── src/
    ├── main/java/
    │   ├── main.java                # 程序入口 (示例)
    │   ├── datatypes/               # 空间数据类型
    │   │   ├── Point.java
    │   │   ├── Rectangle.java
    │   │   ├── Circle.java
    │   │   └── Edge.java
    │   ├── spline/                  # 核心: 学习型索引 (样条曲线)
    │   │   ├── Spline.java
    │   │   ├── Coord.java
    │   │   ├── CdfOnTheFlyInterfaceY.java
    │   │   └── Errors.java
    │   ├── index/                   # 索引构建 (Spark mapPartitions)
    │   │   ├── BuildIndex.java
    │   │   └── IndexBuilder.java
    │   ├── partitions/              # 空间分区策略
    │   │   ├── SpatialPartition.java
    │   │   ├── FixGridPartitioner.java
    │   │   ├── AdaptiveGridPartitioner.java
    │   │   ├── KDBTreePartitioner.java
    │   │   ├── QuadTreePartitioner.java
    │   │   └── RtreePatitioner.java
    │   ├── innerPartition/          # 分区器内部实现
    │   │   ├── spatialPartitioner.java
    │   │   ├── KDBPartitioner.java
    │   │   └── QuadPartition.java
    │   ├── quadtree/                # 四叉树实现
    │   │   ├── QuadTree.java
    │   │   ├── QuadNode.java
    │   │   ├── QuadRectangle.java
    │   │   └── QuadtreePartitioning.java
    │   ├── query/                   # 查询处理
    │   │   ├── RangeQuery.java
    │   │   ├── PointQuery.java
    │   │   ├── KNNQuery.java
    │   │   ├── DistanceQuery.java
    │   │   ├── JoinQuery.java
    │   │   ├── RangeFilterUsingIndex.java
    │   │   ├── PointsFilterUsingIndex.java
    │   │   ├── DistanceFilterUsingIndex.java
    │   │   └── JoinQueryUsingIndex.java
    │   ├── pointrdd/                # RDD 工具
    │   │   └── PointRDDUtils.java
    │   ├── idnexbuild/              # 批量索引构建入口
    │   │   ├── BuildAll.java
    │   │   ├── QPIndexBuild.java
    │   │   ├── KPIndexBuild.java
    │   │   ├── RPIndexBuild.java
    │   │   ├── FPIndexBuild.java
    │   │   └── APIndexBuild.java
    │   ├── timeseries/              # 时序扩展
    │   │   └── Timeseries.java
    │   └── utils/                   # 工具类
    │       ├── Utils.java
    │       ├── SplineUtil.java
    │       ├── ReadPoints.java
    │       ├── ReadPolygon.java
    │       ├── ReadRectangles.java
    │       ├── Result.java
    │       ├── StatCalculator.java
    │       └── PointDistanceComparator.java
    └── test/java/
        └── Test.java                # 单元测试
```

---

## 3. 核心模块详解

### 3.1 数据类型层 (`datatypes/`)

| 类 | 说明 |
|----|------|
| `Point` | 二维空间点 (x, y)，实现了 `Serializable`。支持欧氏距离计算、坐标比较、JTS Envelope 转换 |
| `Rectangle` | 矩形区域，由左下角 `from` 和右上角 `to` 两个 Point 定义。支持相交判断、包含判断、面积计算 |
| `Circle` | 圆形区域（中心点 + 半径），可转换为外接矩形 (BoundingBox) |
| `Edge` | 边/线段，由起点和终点定义 |

### 3.2 学习型索引核心 (`spline/`)

这是整个系统的核心创新所在。

#### 3.2.1 `Spline.java` — 学习型索引结构

**核心数据结构：**
- `points: List<Point>` — 分区内的所有空间点（按 Y 坐标排序）
- `spline: List<Coord>` — 压缩后的样条控制点
- `radixHint: int[]` — Radix 加速表，用于快速定位样条段
- `min_x, max_x, min_y, max_y` — 分区内数据的边界框
- `factor` — Radix 变换因子

**关键参数：**
- `SPLINE_SIZE = 32` — 样条压缩的误差容忍度（epsilon）
- `RADIX_SIZE = 10` — Radix 表位数 (2^10 = 1024 个桶)
- `FALLBACK_TO_LINEAR_SCAN_THRESHOLD = 100` — 数据量 ≤100 时退化为线性扫描

**构建流程 (`build()`)：**
1. 计算分区内所有点的边界框
2. 若点数 ≤ 100，标记为 `linear_scan` 模式，直接返回
3. 按 Y 坐标排序所有点
4. 通过 `CdfOnTheFlyInterfaceY` 计算数据的 CDF
5. 使用 `tautString` 算法将 CDF 压缩为样条曲线
6. 构建 Radix 加速表 (`buildRadix`)

**查询操作：**
- `pointLookUp(Point)` — 点查找：通过样条估算位置，然后双向扫描精确匹配
- `lookUp(Rectangle, result)` — 范围查找：估算范围边界位置，提取范围内所有点
- `count(Rectangle)` — 范围计数：同上但只计数不返回数据
- `estimatePosition(val)` — 位置估算：通过 Radix 表定位样条段，再用线性插值计算位置

**位置估算原理：**
```
输入: 目标 Y 值
    ↓
Radix 表快速定位 → 找到所属的样条段 [segment, segment+1]
    ↓
线性插值: position = down.y + (val - down.x) * slope
    ↓
输出: 估算的数组索引位置 (可能有少量偏差，需要微调)
```

#### 3.2.2 `CdfOnTheFlyInterfaceY.java` — CDF 在线计算

将已排序的点序列转换为 CDF 迭代器：
- 对于相同 Y 值的连续点，只输出最后一个的位置
- 输出格式: `Coord(y_value, array_position)`
- 用于 `tautString` 算法的输入

#### 3.2.3 `Coord.java` — 样条控制点

简单的 (x, y) 坐标对，用于表示样条曲线上的控制点：
- `x` = 数据的 Y 坐标值（作为查找键）
- `y` = 对应的数组位置（作为预测目标）

#### 3.2.4 `Errors.java` — 误差统计

记录样条拟合的平均误差和最大误差。

### 3.3 样条工具 (`utils/SplineUtil.java`)

| 方法 | 说明 |
|------|------|
| `tautString(CdfOnTheFlyInterfaceY, epsilon)` | **核心算法**: Taut String 方法，将 CDF 压缩为分段线性函数。epsilon 控制允许的最大误差 |
| `tautString(List<Coord>, maxValue, epsilon)` | 通用版本，对任意 Coord 列表进行 Taut String 压缩 |
| `compressFunc(func, desiredSize)` | 通过二分搜索 epsilon，将函数压缩到指定大小 |
| `interpolate(spline, pos)` | 在样条上进行线性插值 |
| `computeErrors(cdf, spline)` | 计算样条拟合误差 |
| `computeSlopes(spline)` | 计算各样条段的斜率 |
| `buildCdf(points)` | 从已排序点列表构建 CDF |
| `cmpDevs(a, b, c)` | 计算三点的偏差方向（用于 Taut String） |

**Taut String 算法原理：**

Taut String（紧弦）算法是一种曲线简化方法，类似于在 CDF 曲线两侧保持 ±epsilon 的"管道"，然后在管道中拉一根"紧弦"，弦的折点就是样条的控制点。这样可以用最少的线段近似原始 CDF，同时保证误差不超过 epsilon。

### 3.4 索引构建 (`index/`)

#### `BuildIndex.java`
```java
public static JavaRDD<Spline> indexBuild(JavaRDD<Point> pointRDD) {
    return pointRDD.mapPartitions(new IndexBuilder());
}
```
在 Spark 的每个分区上独立构建 Spline 索引。

#### `IndexBuilder.java`
实现 `FlatMapFunction<Iterator<Point>, Spline>`：
1. 遍历分区内所有点，加入 Spline
2. 调用 `spline.build()` 构建索引
3. 返回包含一个 Spline 的迭代器

### 3.5 空间分区策略 (`partitions/`)

`SpatialPartition.java` 是统一入口，支持 5 种分区策略：

| 分区器 | 类 | 策略描述 |
|--------|-----|---------|
| 固定网格 | `FixGridPartitioner` | 将空间按 X 轴均匀切分为 1000 个格子，按格子 ID 分配分区 |
| 自适应网格 | `AdaptiveGridPartitioner` | 根据数据总量和分区数自适应计算格子宽度 |
| 四叉树 | `QuadTreePartitioner` | 采样数据构建四叉树，按叶节点分配分区 |
| KDB-树 | `KDBTreePartitioner` | 采样数据构建 KDB-Tree（k-d-B 树），按叶节点分配分区 |
| R-树 | `RtreePatitioner` | 采样数据构建 R-Tree，按 MBR 分配分区 |

**通用流程：**
1. `analyze()` — 通过 Spark aggregate 计算数据的边界框和总数
2. 采样（QuadTree/KDB/RTree 使用采样）或直接计算
3. 构建空间索引结构划分空间
4. `partitionPoints()` — 每个点根据空间位置分配到对应分区

### 3.6 查询处理 (`query/`)

#### 3.6.1 范围查询 `RangeQuery`

```
SpatialRangeQuery(indexRDD, rectangle):
  1. filter: 过滤掉边界框与查询矩形不相交的分区
  2. mapPartitions: 对每个相关分区用 Spline.lookUp() 查找范围内的点
```

#### 3.6.2 点查询 `PointQuery`

```
SpatialPointQuery(indexRDD, queryPoint):
  1. filter: 过滤掉不包含查询点的分区 (通过边界框判断)
  2. mapPartitions: 对每个相关分区用 Spline.pointLookUp() 查找精确匹配
```

#### 3.6.3 KNN 查询 `KNNQuery`

```
SpatialKNNQuery(indexRDD, k, queryPoint, maxArea, N):
  1. 基于数据密度估算初始搜索半径 r = sqrt(k / (π × density))
  2. 以查询点为中心，r 为半径构造搜索矩形
  3. 执行范围查询
  4. 若结果数 < k:
     - 若结果为 0: r = 2r
     - 否则: 根据实际密度重新估算 r
  5. 重复直到结果数 >= k
  6. 用最大堆取 Top-K 最近邻
```

#### 3.6.4 距离查询 `DistanceQuery`

```
SpatialDistanceQuery(indexRDD, queryPoint, distance):
  1. filter: 过滤掉与距离范围不相交的分区
  2. mapPartitions (DistanceFilterUsingIndex):
     a. 先用矩形范围查询获取候选点
     b. 对候选点用 Haversine 公式计算球面距离进行精确过滤
```

#### 3.6.5 空间连接 `JoinQuery`

```
SpatialJoinQuery(indexRDD, polygons):
  对每个多边形:
    1. 获取多边形的外接矩形
    2. 执行范围查询获取候选点
    3. (可选) 精确的多边形包含判断
```

### 3.7 内部分区器 (`innerPartition/`)

| 类 | 说明 |
|----|------|
| `spatialPartitioner` | 抽象基类，继承 Spark `Partitioner`。定义了 `placeObject()` 接口 |
| `KDBPartitioner` | KDB-Tree 分区器实现。遍历树找到包含点的叶节点 |
| `QuadPartition` | 四叉树分区器实现。查找点所属的叶节点区域 |

### 3.8 批量索引构建 (`idnexbuild/`)

| 类 | 说明 |
|----|------|
| `BuildAll` | 批量执行所有分区策略的索引构建 |
| `QPIndexBuild` | 基于 QuadTree 分区构建索引 |
| `KPIndexBuild` | 基于 KDB-Tree 分区构建索引 |
| `RPIndexBuild` | 基于 R-Tree 分区构建索引 |
| `FPIndexBuild` | 基于 FixGrid 分区构建索引 |
| `APIndexBuild` | 基于 AdaptiveGrid 分区构建索引 |

### 3.9 数据读取与工具 (`utils/`, `pointrdd/`)

| 类 | 说明 |
|----|------|
| `ReadPoints` | 从 JSON 文件（GeoJSON 格式 `"g":"POINT (x y)"`）或 CSV 文件读取点数据 |
| `ReadPolygon` | 读取多边形数据 |
| `ReadRectangles` | 读取矩形数据 |
| `PointRDDUtils` | 从 HDFS/本地 CSV 创建 Point RDD 和 Polygon RDD |
| `StatCalculator` | 通过 Spark aggregate 并行计算数据的边界框和计数 |
| `Utils` | 计算 BoundingBox、排序、分区 ID 计算 |
| `PointDistanceComparator` | 点距离比较器 |
| `Result` | 结果封装 |

### 3.10 时序扩展 (`timeseries/`)

`Timeseries<T>` 是一个泛型类，将空间点与时间属性 `T` 关联，支持时空联合查询。

---

## 4. 系统架构与数据流

```
┌─────────────────────────────────────────────────────────────────────┐
│                        LiLIS 系统架构                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────┐     ┌──────────────┐     ┌─────────────────────────┐ │
│  │ 数据读取  │────▶│  空间分区     │────▶│  学习型索引构建           │ │
│  │ (JSON/CSV)│     │  (5种策略)    │     │  (每分区独立 Spline)     │ │
│  └──────────┘     └──────────────┘     └─────────────────────────┘ │
│                                                   │                 │
│                                                   ▼                 │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                     查询处理层                                │   │
│  │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────────┐ ┌──────────┐     │   │
│  │  │Range │ │Point │ │ KNN  │ │ Distance │ │  Join    │     │   │
│  │  │Query │ │Query │ │Query │ │  Query   │ │  Query   │     │   │
│  │  └──┬───┘ └──┬───┘ └──┬───┘ └────┬─────┘ └────┬─────┘     │   │
│  │     │        │        │           │             │           │   │
│  │     └────────┴────────┴───────────┴─────────────┘           │   │
│  │                        │                                     │   │
│  │                        ▼                                     │   │
│  │              ┌─────────────────────┐                         │   │
│  │              │  Spline 位置估算     │                         │   │
│  │              │  + 局部扫描修正      │                         │   │
│  │              └─────────────────────┘                         │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    Apache Spark                               │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐             │   │
│  │  │ Partition 0 │  │ Partition 1 │  │ Partition N │  ...       │   │
│  │  │ [Spline]   │  │ [Spline]   │  │ [Spline]   │             │   │
│  │  └────────────┘  └────────────┘  └────────────┘             │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 5. 执行流程

### 5.1 完整流程示例 (以 RangeQuery 为例)

```
1. 数据加载
   ReadPoints.readPointsToJson("data.json", points)
        ↓
2. 计算全局边界框
   Utils.getBoundingBox(points)
        ↓
3. 创建 Spark RDD
   JavaRDD<Point> pointRDD = jsc.parallelize(points)
        ↓
4. 空间分区 (选择一种策略)
   JavaRDD<Point> partitionRDD = SpatialPartition.QuadtreePartitioner(pointRDD)
   内部流程:
     a. analyze() → 计算边界框 + 数据总量
     b. 采样 → 构建 QuadTree
     c. 每个点 → 通过 QuadTree 找到所属分区
     d. partitionBy() → Spark 重新分区
        ↓
5. 构建学习型索引
   JavaRDD<Spline> splineRDD = BuildIndex.indexBuild(partitionRDD)
   内部流程 (每个分区独立):
     a. 收集分区内所有点
     b. 按 Y 坐标排序
     c. 计算 CDF
     d. Taut String 压缩 → 样条控制点
     e. 构建 Radix 加速表
        ↓
6. 执行范围查询
   JavaRDD<Point> result = RangeQuery.SpatialRangeQuery(splineRDD, queryRect)
   内部流程:
     a. filter: 通过边界框快速排除无关分区
     b. mapPartitions: 对每个相关分区:
        - estimateFrom: 通过样条估算范围起始位置
        - estimateTo: 通过样条估算范围结束位置
        - 提取 [from, to] 区间内的点
        - 对于边界情况: 扫描修正
```

### 5.2 样条查询的位置估算细节

```
estimatePosition(y_value):
  ┌────────────────────────────┐
  │ 1. transform(y_value)      │  → 计算 radix 索引 p
  │ 2. radixHint[p]            │  → 得到样条段搜索起点 begin
  │    radixHint[p+1]          │  → 得到样条段搜索终点 end
  │ 3. 在 [begin, end] 中查找  │  → 定位包含 y_value 的样条段 segment
  │ 4. interpolateSegment()    │  → 在 segment 上线性插值
  └────────────────────────────┘
               ↓
  返回: 估算的数组索引位置
```

---

## 6. 构建与运行

### 6.1 构建

```bash
mvn clean package
```

### 6.2 运行环境

- 需要配置 `hadoop.home.dir`
- 支持 `local[*]` 本地模式和 Spark 集群模式
- 数据格式: JSON (GeoJSON POINT 格式) 或 CSV

### 6.3 数据格式

**JSON 格式 (每行一条记录):**
```json
{"g":"POINT (-87.697249 41.822730)", ...}
```

**CSV 格式:**
```csv
x,y
-87.697249,41.822730
-87.686513,41.830143
```

---

## 7. 依赖说明

| 依赖 | 用途 |
|------|------|
| `spark-core_2.12:3.0.0` | Spark 分布式计算框架核心 |
| `spark-sql_2.12:3.0.0` | Spark SQL (可选) |
| `sedona-core-3.0_2.12:1.2.0` | Apache Sedona 空间分区工具 (KDB-Tree, R-Tree, QuadTree 分区) |
| `jts-core:1.19.0` | JTS 几何计算库 (Envelope, Polygon, Coordinate) |
| `opencsv:5.5` | CSV 文件读取 |
| `junit:4.13.2` / `junit-jupiter:5.10.1` | 单元测试 |

---

## 8. 性能特点

### 8.1 为什么快？

1. **O(1) 位置估算**: Radix 表 + 样条插值，查询复杂度接近 O(1)，远快于树形索引的 O(log n)
2. **分区剪枝**: 通过边界框快速排除大量无关分区，减少实际计算量
3. **分布式并行**: 每个 Spark 分区的索引独立构建和查询，天然支持并行
4. **内存友好**: 样条压缩后只需存储少量控制点（通常 << 原始数据量）

### 8.2 适用场景

- 大规模空间点数据（百万至亿级）
- 以范围查询和点查询为主
- 数据分布相对稳定（索引一次构建，多次查询）
- 运行在 Spark 集群上

---

## 9. 关键算法总结

| 算法 | 位置 | 说明 |
|------|------|------|
| Taut String | `SplineUtil.tautString()` | CDF 曲线压缩为分段线性函数 |
| Radix 加速 | `Spline.buildRadix()` | 用 hash 表加速样条段定位 |
| CDF 在线计算 | `CdfOnTheFlyInterfaceY` | 从排序数据流式构建 CDF |
| 密度估算 KNN | `KNNQuery` | 基于数据密度动态调整搜索半径 |
| Haversine 距离 | `DistanceFilterUsingIndex` | 球面距离计算（地理坐标系） |
| 空间采样分区 | 各 Partitioner | 先采样再构建索引结构划分空间 |

---

## 10. 扩展性设计

- **新分区策略**: 实现 `spatialPartitioner` 抽象类即可接入新的空间分区方法
- **新查询类型**: 基于 `Spline.lookUp()` 和 `Spline.estimatePosition()` 可实现各种变体
- **时序扩展**: `Timeseries<T>` 泛型类支持任意时间类型的时空联合索引
- **数据源扩展**: `PointRDDUtils` 支持从 HDFS/本地文件系统读取多种格式
