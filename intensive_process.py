import random

def run_data_intensive_process():
    output_message = ''
    error_message = ''
    try:
        output_message = random.randint(1, 100)
    except Exception as e:
        error_message = str(e)
    return output_message, error_message

if __name__ == '__main__':
    output_message, error_message = run_data_intensive_process()
    with open('./output.txt') as f:
        f.write(output_message)
    with open('./error.txt') as f:
        f.write(error_message)
        