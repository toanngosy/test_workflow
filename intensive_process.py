import random
import os
import sys

def run_data_intensive_process():
    output_message = ''
    error_message = ''
    try:
        output_message = random.randint(1, 100)
        output_message = str(output_message)
    except Exception as e:
        error_message = str(e)
    return output_message, error_message

if __name__ == '__main__':
    run_id = sys.argv[1]
    output_message, error_message = run_data_intensive_process()
    os.makedirs('./result', exist_ok=True)
    with open(f'./result/output_{run_id}.txt', 'w') as f:
        f.write(output_message)
    with open(f'./result/error_{run_id}.txt', 'w') as f:
        f.write(error_message)
        