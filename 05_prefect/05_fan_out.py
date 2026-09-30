from prefect import flow, task, get_run_logger
import random
import time

@task
def print_and_sleep(value: int):
  delay = random.randint(3, 18)

@task
def range_task(start: int = 1, end: int = 20):
  futures = print_and_sleep.map(range(start, end + 1))
  results = futures.result()
  print(f"Completed (len(results)) subtasks")
  return results 

@task(log_prints=True)
def print_and_sleep(value: int):
  # Subtask: wait a random 5-15 seconds so output is staggered,
  # then print the value it was handed and sleep 5 seconds
  delay = random.uniform(5, 15)
  time.sleep(delay)
  print(f"Value: {value} (delayed {delay:.1f}s)")
  time.sleep(5)
  return value

@task(log_prints=True)
def range_task(start: int = 1, end: int = 20):
  # Parent task: fan out one subtask per value in the range (inclusive).
  # .map() submits them concurrently; .result() waits for all to finish.
  futures = print_and_sleep.map(range(start, end + 1))
  results = futures.result()
  print(f"Completed {len(results)} subtasks.")
  return results

@flow
def fan_out_flow():
  range_task()

# This initiates the whole thing--calls the function inside of itself if the script is nested in functions
if __name__ == "__main__":
  fan_out_flow()
