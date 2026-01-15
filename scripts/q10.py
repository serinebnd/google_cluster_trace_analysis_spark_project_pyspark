"""
question 10 : How often are machine resources over-committed?

since tasks start and stop constantly, we must reconstruct the timeline of each machine to find its PEAK usage.
"""
from config import *
import matplotlib.pyplot as plt
import numpy as np

def main():
    print("question 10")

    sc = create_spark_context("question 10")
    
    try:
        # step 1 : get machine capacities (limits)
        machine_events = sc.textFile(MACHINE_EVENTS_PATH)
        
        # we look for event type 0 (add) to get the machine specs
        # format: (machine_id, (CPU_cap, mem_cap))
        machine_capacities = machine_events \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 5) \
            .filter(lambda x: x[MACHINE_EVENTS_SCHEMA["event_type"]] == "0") \
            .map(lambda x: (
                x[MACHINE_EVENTS_SCHEMA["machine_id"]],
                (safe_float(x[MACHINE_EVENTS_SCHEMA["cpu"]]),
                 safe_float(x[MACHINE_EVENTS_SCHEMA["memory"]]))
            )) \
            .reduceByKey(lambda a, b: a) # if we have duplicates we keep the first one found
            
        machine_capacities.cache()
        print(f"Found capacity data for {machine_capacities.count()} machines")
        
        # step 2 : we get all task events that affect machine load
        task_events = sc.textFile(TASK_EVENTS_PATH)
        
        # we need every event that affects a machine : SCHEDULE (starts consuming), FINISH/KILL/EVICT (stops consuming)
        # we filter out events with no machine_id (pending tasks)
        task_timeline_events = task_events \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 10) \
            .filter(lambda x: x[TASK_EVENTS_SCHEMA["machine_id"]] != "") \
            .map(lambda x: (
                x[TASK_EVENTS_SCHEMA["machine_id"]], # key: machine_id
                (
                    safe_long(x[TASK_EVENTS_SCHEMA["timestamp"]]),
                    safe_int(x[TASK_EVENTS_SCHEMA["event_type"]]),
                    safe_float(x[TASK_EVENTS_SCHEMA["cpu_request"]]),
                    safe_float(x[TASK_EVENTS_SCHEMA["memory_request"]]),
                    # unique task ID (job_id, task_index)
                    (x[TASK_EVENTS_SCHEMA["job_id"]], x[TASK_EVENTS_SCHEMA["task_index"]])
                )
            ))
        
        # step 3 : simulate each machine's load over time

        # we group all events by machine (machine_id, [event 1, event 2, event 3, etc...])
        grouped_events = task_timeline_events.groupByKey()
        
        def simulate_machine_load(machine_data):
            """
            we receive all events for 1 machine, sorted by time and calculate the running sum of usage
            we return the peak (max) usage ever reached
            """
            machine_id, events_iter = machine_data
            
            sorted_events = sorted(list(events_iter), key=lambda x: x[0])
            
            active_tasks = {} # map : task_id -> (CPU_req, mem_req)
            
            max_cpu_seen = 0
            max_mem_seen = 0

            current_cpu = 0
            current_mem = 0

            for ts, etype, cpu_req, mem_req, task_id in sorted_events:
                
                # event 1 : schedule -> task starts using resources
                if etype == 1:
                    # note : if task is already tracked (duplicate event), update it, otherwise add it
                    if task_id not in active_tasks:
                        active_tasks[task_id] = (cpu_req, mem_req)
                        current_cpu += cpu_req
                        current_mem += mem_req

                # events 2-6 : task stops (evict, fail, finish, kill, lost)
                elif etype in {2, 3, 4, 5, 6}:
                    if task_id in active_tasks:
                        old_cpu, old_mem = active_tasks[task_id]
                        current_cpu -= old_cpu
                        current_mem -= old_mem
                        del active_tasks[task_id]
                
                # the peak
                if current_cpu > max_cpu_seen: max_cpu_seen = current_cpu
                if current_mem > max_mem_seen: max_mem_seen = current_mem
            
            return (machine_id, (max_cpu_seen, max_mem_seen))
        
        # we apply the simulation
        peak_usage = grouped_events.map(simulate_machine_load)
        
        # step 4 : compare peak usage vs capacity to find over-commitment
        
        # we apply join : (machine_id, ((peak_cpu, peak_mem), (cap_CPU, cap_mem)))
        comparison = peak_usage.join(machine_capacities)
        
        def analyze_overcommit(data):
            mid, ((peak_c, peak_m), (cap_c, cap_m)) = data
            
            ratio_c = peak_c / cap_c if cap_c > 0 else 0
            ratio_m = peak_m / cap_m if cap_m > 0 else 0
            
            is_cpu_over = peak_c > cap_c
            is_mem_over = peak_m > cap_m
            
            return (is_cpu_over, is_mem_over, ratio_c, ratio_m)
        
        final_stats = comparison.map(analyze_overcommit).collect()
        

        # step 5 : stats and plots
        total_machines = len(final_stats)
        
        # we calculate the totals
        cpu_over_count = sum(1 for x in final_stats if x[0])
        mem_over_count = sum(1 for x in final_stats if x[1])
        any_over_count = sum(1 for x in final_stats if x[0] or x[1])
        
        cpu_ratios = [x[2] for x in final_stats]
        mem_ratios = [x[3] for x in final_stats]
        
        print(f"\nAnalysis ({total_machines} machines analyzed)")
        print(f"Machines with CPU over-commit: {cpu_over_count:<5} ({cpu_over_count/total_machines*100:.2f}%)")
        print(f"Machines with RAM over-commit: {mem_over_count:<5} ({mem_over_count/total_machines*100:.2f}%)")
        print(f"Total machines over-committed: {any_over_count:<5} ({any_over_count/total_machines*100:.2f}%)")
        
        print(f"Max CPU ratio observed: {max(cpu_ratios):.2f}x capacity")
        print(f"Max RAM ratio observed: {max(mem_ratios):.2f}x capacity")
        

        fig, axes = plt.subplots(2, 2, figsize=(12, 10))


        labels = ['safe', 'CPU over', 'memory over', 'both over']
        safe = total_machines - any_over_count
        both = sum(1 for x in final_stats if x[0] and x[1])
        c_only = cpu_over_count - both
        m_only = mem_over_count - both
        
        sizes = [safe, c_only, m_only, both]
        colors = ['#27ae60', '#3498db', "#ffcc00", '#c0392b']
        
        axes[0,0].pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors, startangle=140)
        axes[0,0].set_title('Global Over-commitment Status')
        
        # CPU
        axes[0,1].hist([r for r in cpu_ratios if r < 2], bins=50, color='#3498db', edgecolor='black')
        axes[0,1].axvline(1.0, color='red', linestyle='dashed', linewidth=2, label='Limit (100%)')
        axes[0,1].set_title('Distribution of CPU Peak Commitment')
        axes[0,1].set_xlabel('Ratio (Requested / Capacity)')
        axes[0,1].legend()
        
        # MEMORY
        axes[1,0].hist([r for r in mem_ratios if r < 2], bins=50, color='#f1c40f', edgecolor='black')
        axes[1,0].axvline(1.0, color='red', linestyle='dashed', linewidth=2, label='Limit (100%)')
        axes[1,0].set_title('Distribution of RAM Peak Commitment')
        axes[1,0].set_xlabel('Ratio (Requested / Capacity)')
        axes[1,0].legend()
        
        
        axes[1,1].scatter(cpu_ratios, mem_ratios, alpha=0.4, s=10, color='purple')
        axes[1,1].axhline(1.0, color='red', linestyle='--')
        axes[1,1].axvline(1.0, color='red', linestyle='--')
        axes[1,1].set_xlabel('CPU ratio')
        axes[1,1].set_ylabel('Memory ratio')
        axes[1,1].set_title('correlation : CPU vs RAM over-commit')
        axes[1,1].set_xlim(0, 2)
        axes[1,1].set_ylim(0, 2)
        
        plt.tight_layout()
        output_path = "./output/q10.png"
        plt.savefig(output_path, dpi=150)

        wait_for_user()
        
    finally:
        sc.stop()

if __name__ == "__main__":
    main()