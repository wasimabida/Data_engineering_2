import json
import socket 
import time 
import pandas as pd
import logging   
import signal
import sys

# Flag to handle interruptions
interrupted = False

""""
This part of the code handles interruptions, specifically the SIGINT signal, which is the interrupt signal 
(usually generated by pressing Ctrl+C in the terminal)
"""

def signal_handler(signum, frame):
    """
    Signal handler for SIGINT (Ctrl+C). Sets the interrupted flag and exits gracefully.
    """
    global interrupted
    logging.info("Received interrupt signal. Stopping gracefully.")
    interrupted = True
    logging.info("See you next time!! \n")
    sys.exit(0)

# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)  


def handle_date(obj):
    """
    Custom JSON serializer for pandas timestamp.
    """
    if isinstance(obj,pd.Timestamp):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    raise TypeError(f"object of type '{type(obj).__name__}'' is not json serialzable")

def send_data_over_socket(file_path, host='spark-master',port=9999,chunk_size=2):

    """
    Sends data over a socket to a specified host and port.

    Parameters:
    - file_path (str): Path to the file containing data to be sent.
    - host (str): Hostname or IP address to bind the socket. Default is 'spark-master'.
    - port (int): Port to bind the socket. Default is 9999.
    - chunk_size (int): Number of lines to send in each chunk. Default is 2.
    """

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host,port))
    s.listen(1)
    logging.info(f"listening for connections on {host}:{port}")

   
    last_sent_index =0

    while not interrupted : 
        conn,add=s.accept()
        logging.info(f"Connection from {add}")

        try:
            with open(file_path, 'r')as file :
                # skip the lines that were already sent 
                for _ in range (last_sent_index):
                    next(file)

                records= []
                for line in file:
                    records.append(json.loads(line))
                    if(len(records)) == chunk_size:
                        chunk = pd.DataFrame(records)
                        logging.info(f"Sending chunk:\n{chunk}")
                        for record in chunk.to_dict(orient='records'):
                            serialize_data =json.dumps(record, default=handle_date).encode('utf-8')
                            conn.send(serialize_data + b'\n')
                            time.sleep(5)
                            last_sent_index +=1
                        records= []
        except (BrokenPipeError,ConnectionResetError):  
            logging.info("Client Disconnected.")
        except FileNotFoundError as e:
            logging.error(f"File not found: {e.filename}")
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}",exc_info=True) #This will log the full traceback along with the error message.

        finally:
            conn.close()
            logging.info("Connection closed")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO) #log messages with severity level INFO, WARNING, ERROR, and CRITICAL will be captured and displayed
    send_data_over_socket("datasets/yelp_academic_dataset_review.json")