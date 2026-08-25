import subprocess
import sys
import time

def run_stage(script_name):

    print(f'Stage {script_name}')

    start_time = time.time()

    try:
        subprocess.run([sys.executable, script_name], check=True)

        duration = time.time() - start_time

        print(f'Stage {script_name} done in {duration} s')

    except subprocess.CalledProcessError as e:
        print(f'Error in {script_name}! Error {e.returncode}')
        print('ETL fail!')

        sys.exit(1)


def main():

    pipeline_start = time.time()

    print('ETL start')

    run_stage("extract.py")
    run_stage("transform.py")
    run_stage("load.py")

    total_duration = time.time() - pipeline_start

    print(f'Whole proces done in {total_duration} s')

if __name__ == "__main__":
    main()
