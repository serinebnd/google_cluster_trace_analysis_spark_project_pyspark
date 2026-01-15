"""
question 11 : temporal patterns in job submissions

our approach : we analyse the timestamps of job submissions to find patterns in :
- daily cycle : is there a peak hour ? (example : day vs night)
- monthly trend : how do the workload evolve over the 29 days ?
"""
from config import *
import matplotlib.pyplot as plt

def main():
    print("question 11")
    
    sc = create_spark_context("quesition 11")
    
    try:
        job_events = sc.textFile(JOB_EVENTS_PATH)
        
        # we assume timestamps are in micro seconds and we care about the "submit" event (type = 0)
        submits = job_events \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 3) \
            .filter(lambda x: safe_int(x[JOB_EVENTS_SCHEMA["event_type"]]) == 0) \
            .map(lambda x: safe_long(x[JOB_EVENTS_SCHEMA["timestamp"]]))

        # cache it because we will use it 2 times (for hours and for days)
        submits.cache()
        total_jobs = submits.count()
        print(f"we analyzed {total_jobs} job submissions")

        
        # step 2 : hourly analysis (daily pattern)
        
        # we know that 1s = 1000000 micro seconds
        MICROSECONDS_PER_HOUR = 60 * 60 * 1000000
        MICROSECONDS_PER_DAY = 24 * MICROSECONDS_PER_HOUR
        
        # map : we convert timestamp to number of hours of day (0 to 23)
        # reduce : we count jobs/hour
        hourly_distribution = submits \
            .map(lambda ts: ((ts % MICROSECONDS_PER_DAY) // MICROSECONDS_PER_HOUR, 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .sortByKey() \
            .collect()
            
        # step 3 : daily analysis (monthly trend)

        # map : we convert timestamp to number of days (0 to 28)
        daily_distribution = submits \
            .map(lambda ts: (ts // MICROSECONDS_PER_DAY, 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .sortByKey() \
            .collect()

        # viz and stats
        if hourly_distribution:
            peak_hour, peak_count = max(hourly_distribution, key=lambda x: x[1])
            print(f"peak activity is at hour {int(peak_hour)}:00 ({peak_count} jobs)")
            
        if daily_distribution:
            busy_day, busy_count = max(daily_distribution, key=lambda x: x[1])
            print(f"the most busy day is day {int(busy_day)} ({busy_count} jobs)")

        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        hours = [h for h, _ in hourly_distribution]
        h_counts = [c for _, c in hourly_distribution]
        
        # daily
        ax1.bar(hours, h_counts, color='#3498db', alpha=0.8, edgecolor='black')
        ax1.set_xlabel('hour of the day (0-23)')
        ax1.set_ylabel('total jobs submitted')
        ax1.set_title('global daily pattern : when do users submit jobs ?')
        ax1.set_xticks(range(0, 24))
        ax1.grid(axis='y', alpha=0.3)
        
        days = [d for d, _ in daily_distribution]
        d_counts = [c for _, c in daily_distribution]
        
        # monthly
        ax2.plot(days, d_counts, marker='o', linestyle='-', color='#e74c3c', linewidth=2)
        ax2.set_xlabel('day of the month (0-28)')
        ax2.set_ylabel('total jobs submitted')
        ax2.set_title('cluster workload evolution over the month')
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        out_path = "./output/q11.png"
        plt.savefig(out_path, dpi=150)
        
        wait_for_user()
        
    finally:
        sc.stop()

if __name__ == "__main__":
    main()