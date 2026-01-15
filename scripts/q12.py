"""
question 12 : resource efficiency by priority

the goal of this analysis is to determine if high priority tasks are better
our approach is to compare (CPU used/CPU requested) for each priority level
"""
from config import *
import matplotlib.pyplot as plt
import numpy as np

def main():
    print("question 12")
    
    sc = create_spark_context("question 12")
    
    try:
        task_events = sc.textFile(TASK_EVENTS_PATH)
        
        # key : (job, task)
        # value : (priority, CPU_requested)
        requests = task_events \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 9) \
            .filter(lambda x: x[TASK_EVENTS_SCHEMA["event_type"]] in ["0", "1"]) \
            .map(lambda x: (
                (x[TASK_EVENTS_SCHEMA["job_id"]], x[TASK_EVENTS_SCHEMA["task_index"]]),
                (
                    safe_int(x[TASK_EVENTS_SCHEMA["priority"]]),
                    safe_float(x[TASK_EVENTS_SCHEMA["cpu_request"]])
                )
            )) \
            .reduceByKey(lambda a, b: a)
            
        task_usage = sc.textFile(TASK_USAGE_PATH)
        
        # we calculate average usage per task
        usage = task_usage \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 5) \
            .map(lambda x: (
                (x[TASK_USAGE_SCHEMA["job_id"]], x[TASK_USAGE_SCHEMA["task_index"]]),
                (safe_float(x[TASK_USAGE_SCHEMA["cpu_rate"]]), 1)
            )) \
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
            .mapValues(lambda x: x[0] / x[1])

        # step 3 : join on (job_id, task_index)
        
        # join : ((job_id, task_index), ((priority, cpu_req), cpu_used))
        joined = requests.join(usage)
        
        def calculate_efficiency_stats(data):
            (priority, cpu_req), cpu_used = data
            
            efficiency = 0
            if cpu_req > 0:
                efficiency = cpu_used / cpu_req
            
            if efficiency > 5: efficiency = 5
            
            return (priority, (efficiency, 1))

        # we groupby priority and calculate average efficiency
        efficiency_by_prio = joined \
            .map(lambda x: calculate_efficiency_stats(x[1])) \
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
            .mapValues(lambda x: x[0] / x[1]) \
            .sortByKey() \
            .collect()
            
        # viz
        print(f"{'priority':<10} {'avg Efficiency':<15}")
        
        priorities = []
        eff_values = []
        
        for prio, eff in efficiency_by_prio:
            eff_pct = eff * 100
            priorities.append(prio)
            eff_values.append(eff_pct)
            
            # using ascii bars, NOTE: we used AI help for this part of visualization
            bar_len = int(eff_pct / 5)
            bar = "█" * min(bar_len, 20)
            print(f"{prio:<10} {eff_pct:6.2f}%    {bar}")
            
        plt.figure(figsize=(12, 6))
        
        # we do a color code : green > 70%, yellow > 40%, red <= 40%
        colors = ['#2ecc71' if e > 70 else '#f1c40f' if e > 40 else '#e74c3c' for e in eff_values]
        
        bars = plt.bar([str(p) for p in priorities], eff_values, color=colors, edgecolor='black', alpha=0.8)
        
        plt.axhline(100, color='blue', linestyle='--', label='100% (perfect sizing)')
        plt.axhline(50, color='gray', linestyle=':', label='50% (under-utilized)')
        
        plt.xlabel('task priority (0 = lowest, 11 = highest)')
        plt.ylabel('average efficiency (%)')
        plt.title('question 12')
        plt.legend()
        
        out_path = "./output/original_q2_efficiency.png"
        plt.savefig(out_path, dpi=150)
        
        wait_for_user()
        
    finally:
        sc.stop()

if __name__ == "__main__":
    main()