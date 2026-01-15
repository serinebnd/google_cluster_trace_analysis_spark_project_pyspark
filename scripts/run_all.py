import sys
import os
import time
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def run_question(num):

    if num == 1:
        from q1 import main
        main()
    elif num == 2:
        from q2 import main
        main()
    elif num == 3:
        from q3 import main
        main()
    elif num == 4:
        from q4_q5 import main
        main()
    elif num == 5:
        from q4_q5 import main
        main()
    elif num == 6:
        from q6 import main
        main()
    elif num == 7:
        from q7 import main
        main()
    elif num == 8:
        from q8 import main
        main()
    elif num == 9:
        from q9 import main
        main()
    elif num == 10:
        from q10 import main
        main()
    elif num == 11:
        from q11 import main
        main()
    elif num == 12:
        from q12 import main
        main()
    else:
        print("error")
        return


if __name__ == "__main__":
    if len(sys.argv) > 1:
        questions = sys.argv[1:]
    else:
        questions = ["1", "2", "3", "4-5", "6", "7", "8", "9", "10", "11", "12"]
    
    print(f"you choosed questions : {questions}. Press enter to start")
    input()
    
    total_start = time.time()
    
    for q in questions:
        run_question(q)

    total_elapsed = time.time() - total_start
    print(f"\nit took {timedelta(seconds=int(total_elapsed))} seconds")