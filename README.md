# Google Cluster Trace Analysis - Spark Project

This project analyzes the **Google Cluster Trace Dataset (GCAT)** using Apache Spark to answer key questions about cluster resource utilization, task scheduling, and eviction patterns.

## Overview

The project consists of 12 analytical queries that examine various aspects of Google's cluster data, including:
- CPU and resource distribution across machines
- Task scheduling and eviction behaviors
- Machine maintenance events
- Resource over-commitment and utilization patterns
- Task locality and execution patterns

## Project Structure

```
project_spark/
├── data/
│   ├── job_events/          # Job lifecycle events (creation, submission, completion)
│   ├── machine_events/      # Machine state changes (add, remove, update)
│   ├── task_events/         # Task lifecycle events (schedule, evict, finish, etc.)
│   └── task_usage/          # Task resource utilization metrics
├── scripts/
│   ├── config.py            # Configuration and schema definitions
│   ├── run_all.py           # Main orchestrator script
│   └──  q1.py - q12.py       # Individual analysis scripts
├── results/                 # Generated plots and results
└── schema.csv               # Dataset schema documentation
```

## Installation

### Prerequisites
- Python 3.8+
- Apache Spark 3.0+
- PySpark
- pandas, numpy, matplotlib

### Setup

1. **Clone and navigate to project:**
   ```bash
   cd /home/dell/project_spark
   ```

2. **Create virtual environment (optional but recommended):**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install pyspark pandas numpy matplotlib
   ```

## Usage

### Run All Analyses
To execute all 12 questions:
```bash
cd scripts/
python run_all.py
```

The script will prompt you to confirm before starting. Results and plots are saved to the `output/` folder.

### Run Specific Questions
To run only specific analyses:
```bash
cd scripts/
python run_all.py 1 2 6 9
```

This would run questions 1, 2, 6, and 9.

### Run Individual Analysis
To run a single analysis script:
```bash
cd scripts/
python q6.py
```

## Questions Analyzed

| # | Question | Focus |
|---|----------|-------|
| 1 | CPU Distribution | How are CPU resources distributed across machines? |
| 2 | Maintenance Loss | How much capacity is lost due to machine maintenance? |
| 3 | Maintenance by Class | Which task classes are most affected by maintenance? |
| 4-5 | Eviction Distribution | How are killed/evicted tasks distributed across priority classes? |
| 6 | Eviction Probability | Do low-priority tasks have higher eviction probability? |
| 7 | Task Locality | How often are tasks scheduled locally vs. remotely? |
| 8 | Resource Comparison | How do task resource requests compare to actual usage? |
| 9 | Peaks & Evictions | When do resource peaks occur and how do they relate to evictions? |
| 10 | Over-commitment | To what extent are resources over-committed? |
| 11 | Advanced Analysis | Additional insights (varies) |
| 12 | Advanced Analysis | Additional insights (varies) |

## Key Concepts

### Scheduling Classes
Tasks are categorized by priority/latency sensitivity:
- **Class 0**: Batch jobs (low priority, interruptible)
- **Class 1-2**: Standard jobs
- **Class 3**: Latency-sensitive services (high priority)

### Event Types
- **0**: Submit
- **1**: Schedule
- **2**: Evict
- **3**: Finish
- **4**: Fail
- **5**: Update
- **6**: Kill
- **7**: Lost
- **8**: Wait for resources

### Dataset Characteristics
- **Time range**: Large-scale cluster operations over multiple days
- **Machines**: Thousands of servers with varying specifications
- **Tasks**: Millions of tasks with diverse resource requirements
- **Format**: Compressed CSV files (`.csv.gz`)

## Configuration

Edit `scripts/config.py` to customize:
- **Data paths**: Location of input data files
- **Schema definitions**: Column mappings for different event types
- **Spark settings**: Memory, executor configuration, etc.

Example:
```python
BASE_PATH = "../data"
MACHINE_EVENTS_PATH = os.path.join(BASE_PATH, "machine_events", "*.csv.gz")
TASK_EVENTS_PATH = os.path.join(BASE_PATH, "task_events", "*.csv.gz")
```

## Output

Results are saved in:
- **Plots**: `scripts/output/` (PNG format)
- **Console**: Summary statistics and analysis results
- **Optional**: CSV exports of aggregated data

Example output files:
```
output/
├── q1_cpu_distribution.png
├── q2_maintenance_loss.png
├── q6_eviction_by_class.png
└── ...
```

## Code Structure

### Main Flow
1. **Load & Parse**: Read CSV data from compressed files
2. **Transform**: Parse and map data into Spark RDDs/DataFrames
3. **Analyze**: Apply Spark transformations (map, filter, groupBy, etc.)
4. **Aggregate**: Use reduceByKey or SQL for summation
5. **Visualize**: Generate plots with matplotlib/numpy
6. **Output**: Display results and save artifacts

### Helper Functions (config.py)
- `create_spark_context()`: Initialize Spark session
- `parse_csv_line()`: Parse CSV records
- `safe_int()`: Safe integer conversion with defaults

## Performance Notes

- Scripts use `.cache()` to optimize repeated RDD access
- Large datasets benefit from Spark distributed processing
- Typical execution time: 5-30 minutes depending on data size
- Adjust Spark executor memory for large datasets:
  ```bash
  export SPARK_LOCAL_IP=127.0.0.1
  export SPARK_DRIVER_MEMORY=4g
  export SPARK_EXECUTOR_MEMORY=4g
  ```

## Troubleshooting

### Memory Issues
- Increase Spark memory allocation
- Process data in smaller batches
- Use DataFrames instead of RDDs for better optimization

### File Not Found
- Verify data files are in correct location (`../data/`)
- Check file compression format (`.csv.gz`)
- Ensure read permissions on data directories

### Slow Execution
- Verify Spark parallelization: `sc.defaultParallelism`
- Check for data skew in grouping operations
- Use `cache()` strategically for reused RDDs

## Development

### Adding New Analyses
1. Create new script `qN.py` in `scripts/`
2. Define `main()` function with Spark context
3. Register in `run_all.py`: `elif num == N: from qN import main`

### Refactoring
Each analysis script can be modularized with helper functions:
```python
def load_data(sc):
    """Load and parse dataset"""
    
def analyze(rdd):
    """Core analysis logic"""
    
def save_results(data, path):
    """Export results and plots"""
```

## Citation

This project analyzes data from the **Google Cluster Trace Dataset**, published by Google for research purposes. Reference: Reiss et al., "Heterogeneity and Dynamicity of Clouds at Scale" (NSDI 2012).

## License

Project-specific code is provided as-is for educational and research purposes.

## Contact

For questions or issues, refer to individual script documentation or analysis notes in the source code.

---

**Last Updated**: January 2026
