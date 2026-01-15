"""
question 9 : can we observe correlations between resource peaks and task evictions ?

we want to see if machines that experience high resource usage (peaks) are the same machines that trigger many evictions
"""
from config import *
import matplotlib.pyplot as plt
import numpy as np

def main():
    print("question 9")
    
    sc = create_spark_context("question 9")
    
    try:
        task_events = sc.textFile(TASK_EVENTS_PATH)
        
        # for this, we need event type 2 = evictions
        # map -> (machine_id, 1) -> ReduceByKey
        machine_evictions = task_events \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 5) \
            .filter(lambda x: x[TASK_EVENTS_SCHEMA["event_type"]] == "2") \
            .filter(lambda x: x[TASK_EVENTS_SCHEMA["machine_id"]] != "") \
            .map(lambda x: (x[TASK_EVENTS_SCHEMA["machine_id"]], 1)) \
            .reduceByKey(lambda a, b: a + b)
        
        
        machine_evictions.cache()
        total_evicted_machines = machine_evictions.count()
        print(f"found {total_evicted_machines} machines with evictions")
        
        # step 2 : we get resource usage peaks from task_usage
        task_usage = sc.textFile(TASK_USAGE_PATH)
        
        # we do sampling because it is enough to find the peak behavior of the machine
        # since if it is overloaded it will show up in 10 % of its logs
        
        sampled_usage = task_usage.sample(withReplacement=False, fraction=0.1, seed=42)
        
        # we extract : (machine_id, (maximum_cpu_rate, maximum_memory_usage))
        # reducec : we keep the max of each machine
        machine_peaks = sampled_usage \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 13) \
            .filter(lambda x: x[TASK_USAGE_SCHEMA["machine_id"]] != "") \
            .map(lambda x: (
                x[TASK_USAGE_SCHEMA["machine_id"]],
                (
                    safe_float(x[TASK_USAGE_SCHEMA["maximum_cpu_rate"]]),
                    safe_float(x[TASK_USAGE_SCHEMA["canonical_memory_usage"]])
                )
            )) \
            .reduceByKey(lambda a, b: (max(a[0], b[0]), max(a[1], b[1])))
            
        # step 3 : we correlate peaks with evictions
        # we do left join because we want all machines that have usage data
        # format : (machine_id, ((max_cpu, max_mem), eviction_count))
        joined_data = machine_peaks.leftOuterJoin(machine_evictions)
        
        final_stats = joined_data \
            .map(lambda x: (
                x[1][0][0],
                x[1][0][1],
                x[1][1] if x[1][1] is not None else 0
            )) \
            .collect()
            
        print(f"analyzed {len(final_stats)} machines")

        # viz and corr calculation
        max_cpus = [x[0] for x in final_stats]
        max_mems = [x[1] for x in final_stats]
        evicts = [x[2] for x in final_stats]
        
        corr_cpu = np.corrcoef(max_cpus, evicts)[0, 1] if len(max_cpus) > 1 else 0
        corr_mem = np.corrcoef(max_mems, evicts)[0, 1] if len(max_mems) > 1 else 0
        
        print(f"peak CPU vs evictions : {corr_cpu:.4f}")
        print(f"\npeak RAM vs evictions : {corr_mem:.4f}")
        
        fig, axes = plt.subplots(1, 2, figsize=(12, 6))
        
        indices = np.random.choice(len(evicts), min(2000, len(evicts)), replace=False)
        # cpu
        axes[0].scatter([max_cpus[i] for i in indices], 
                        [evicts[i] for i in indices], 
                        alpha=0.3, s=15, color='#e74c3c')
        axes[0].set_xlabel('peak CPU usage (cores)')
        axes[0].set_ylabel('total evictions')
        axes[0].set_title(f'CPU peaks vs evictions (r={corr_cpu:.2f})')
        axes[0].grid(True, alpha=0.3)
        
        # memory
        axes[1].scatter([max_mems[i] for i in indices], 
                        [evicts[i] for i in indices], 
                        alpha=0.3, s=15, color='#3498db')
        axes[1].set_xlabel('peak memory usage (normalized)')
        axes[1].set_ylabel('total evictions')
        axes[1].set_title(f'memory peaks vs evictions (r={corr_mem:.2f})')
        axes[1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig("output/q9.png", dpi=150)
            
        wait_for_user()
        
    finally:
        sc.stop()

if __name__ == "__main__":
    main()