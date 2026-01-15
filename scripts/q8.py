"""
question 8 : Do tasks that request more resources actually consume more?

we compare the "expectation" (resource request in task_events) vs the "reality" (actual resource usage in task_usage)
this requires joining two RDDs on the unique task identifier (job_id, task_index)
"""
from config import *
import matplotlib.pyplot as plt
import numpy as np

def main():
    print("Question 8")

    sc = create_spark_context("question 8")

    try:
        # step 1 : expectation = requested resources from task_events
        task_events = sc.textFile(TASK_EVENTS_PATH)

        # we need to identify info + request info
        # we filter for event types 0 (submit) or 1 (schedule) because that's where the resource request numbers are stored
        requests_rdd = task_events \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 10) \
            .filter(lambda x: x[TASK_EVENTS_SCHEMA["event_type"]] in ["0", "1"]) \
            .map(lambda x: (
                # key : unique task ID (job_id + task_index)
                (x[TASK_EVENTS_SCHEMA["job_id"]], x[TASK_EVENTS_SCHEMA["task_index"]]),
                # value : (cpu_request, memory_request)
                (safe_float(x[TASK_EVENTS_SCHEMA["cpu_request"]]),
                 safe_float(x[TASK_EVENTS_SCHEMA["memory_request"]]))
            )) \
            .reduceByKey(lambda a, b: a)

            # a task can have multiple events (example : rescheduled)

        requests_rdd.cache()
        print(f"Found {requests_rdd.count()} task requests")
        
        # step 2 : reality = resource usage from task_usage

        task_usage = sc.textFile(TASK_USAGE_PATH)
        
        # usage is reported in 5 minute windows, a long task has 1000s of lines
        # We need to calculate the avg usage for each task :
        # map : output (cpu, mem, 1)
        # reduce : sum(total_cpu, total_mem, total_count)
        usage_rdd = task_usage \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 6) \
            .map(lambda x: (
                (x[TASK_USAGE_SCHEMA["job_id"]], x[TASK_USAGE_SCHEMA["task_index"]]),
                (safe_float(x[TASK_USAGE_SCHEMA["cpu_rate"]]),
                 safe_float(x[TASK_USAGE_SCHEMA["canonical_memory_usage"]]),
                 1) # The counter for the average
            )) \
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])) \
            .mapValues(lambda x: (x[0] / x[2], x[1] / x[2])) # Calculate avg: Sum / Count
        
        usage_rdd.cache()
        print(f"Found usage data for {usage_rdd.count()} tasks")
        

        # step 3 : join on (job_id, task_index)
        
        # like an INNER JOIN in SQL : we only keep tasks that exist in "both" lists
        # format after join : (key, ((cpu_req, mem_req), (cpu_used, mem_used)))
        joined_data = requests_rdd.join(usage_rdd)
        
        joined_data.cache()
        total_pairs = joined_data.count()
        print(f"total successful matches : {total_pairs}")
        
        # step 4 : display results and plots
        
        local_data = joined_data \
            .map(lambda x: (x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1])) \
            .collect()
            
        cpu_req = [row[0] for row in local_data]
        mem_req = [row[1] for row in local_data]
        cpu_use = [row[2] for row in local_data]
        mem_use = [row[3] for row in local_data]
        
        # we calculate correlations : how well does the request predict the usage ?
        cpu_corr = np.corrcoef(cpu_req, cpu_use)[0, 1] if len(cpu_req) > 1 else 0
        mem_corr = np.corrcoef(mem_req, mem_use)[0, 1] if len(mem_req) > 1 else 0
        
        print("\ncorrelation report :")
        print(f"CPU correlation:    {cpu_corr:.4f}")
        print(f"memory correlation: {mem_corr:.4f}")
        
        # we calculate efficiency ratios (used/requested)
        # ratio=1 means perfect estimation
        # ratio<1 means user asked for too much (waste)
        # ratio>1 means user asked for too little (over-use)

        cpu_ratios = [u/r if r > 0 else 0 for u, r in zip(cpu_use, cpu_req)]
        mem_ratios = [u/r if r > 0 else 0 for u, r in zip(mem_use, mem_req)]
        
        # filter outliers for clean stats (keep ratios between 0 and 10)
        cpu_clean = [r for r in cpu_ratios if 0 < r < 10]
        mem_clean = [r for r in mem_ratios if 0 < r < 10]
        
        # we do some categorization for summary stats
        def categorize(ratios):
            if not ratios: return (0,0,0)
            under = sum(1 for r in ratios if r < 0.5)  # Wasted > 50%
            good  = sum(1 for r in ratios if 0.5 <= r <= 1.5)
            over  = sum(1 for r in ratios if r > 1.5)  # Using > 150% of request
            total = len(ratios)
            return (under/total*100, good/total*100, over/total*100)

        cpu_stats = categorize(cpu_clean)
        mem_stats = categorize(mem_clean)

        print("\nefficiency stats :")
        print(f"CPU -> wasted: {cpu_stats[0]:.1f}% | good : {cpu_stats[1]:.1f}% | over-used : {cpu_stats[2]:.1f}%")
        print(f"memory -> wasted: {mem_stats[0]:.1f}% | good : {mem_stats[1]:.1f}% | over-used : {mem_stats[2]:.1f}%")

        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        indices = np.random.choice(len(cpu_req), min(5000, len(cpu_req)), replace=False)
        
        # CPU
        axes[0, 0].scatter([cpu_req[i] for i in indices], [cpu_use[i] for i in indices], alpha=0.3, s=5)
        axes[0, 0].set_xlabel('CPU requested')
        axes[0, 0].set_ylabel('CPU used')
        axes[0, 0].set_title(f'CPU correlation : {cpu_corr:.2f}')
        
        # memory
        axes[0, 1].scatter([mem_req[i] for i in indices], [mem_use[i] for i in indices], alpha=0.3, s=5, color='green')
        axes[0, 1].set_xlabel('Memory requested')
        axes[0, 1].set_ylabel('Memory used')
        axes[0, 1].set_title(f'Memory correlation : {mem_corr:.2f}')
        
        # CPU
        axes[1, 0].hist(cpu_clean, bins=50, color='skyblue', edgecolor='black')
        axes[1, 0].axvline(1, color='red', linestyle='dashed', label='ideal')
        axes[1, 0].set_title('CPU efficiency distribution (used/requested)')
        axes[1, 0].legend()
        
        # memory
        axes[1, 1].hist(mem_clean, bins=50, color='lightgreen', edgecolor='black')
        axes[1, 1].axvline(1, color='red', linestyle='dashed', label='ideal')
        axes[1, 1].set_title('Memory efficiency distribution (used/requested)')
        axes[1, 1].legend()
        
        plt.tight_layout()
        plt.savefig("./output/q8.png", dpi=100)
        
        wait_for_user()
        
    finally:
        sc.stop()

if __name__ == "__main__":
    main()