import random
import os

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
    output_message, error_message = run_data_intensive_process()
    os.makedirs('./result', exist_ok=True)
    with open(f'./result/output_{os.getpid()}.txt', 'w') as f:
        f.write(output_message)
    with open(f'./result/error_{os.getpid()}.txt', 'w') as f:
        f.write(error_message)
        