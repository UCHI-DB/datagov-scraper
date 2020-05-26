import multiprocessing
from multiprocessing import Pool
from multiprocessing import Process, Queue
from collections import namedtuple
import os
import csv

import config as C
from download import download as D

# Work queue
queue = Queue()
total_el_push_q = 0
last_el_push_q = None

# Task definition
Task = namedtuple('Task', 'name, id, format, url')

def push_work(task, queue):
    '''
        Pushes new work task to the queue
    '''
    queue.put(task)
    global total_el_push_q
    total_el_push_q  = total_el_push_q  + 1
    global last_el_push_q
    last_el_push_q = task

def get_work(queue):
    '''
        Retrieves work task from the queue
    '''
    task = queue.get()
    return task

def start_workers(target_path):
    '''
    Start a collection of workers with the process_task
    callback and pass the work queue to the workers
    '''
    workers = []
    print("Starting " + str(C.num_pool_workers) + " workers")
    for i in range(C.num_pool_workers):
        w = Process(target=process_task, args=(queue, target_path,))
        workers.append(w)
    # Start workers
    for w in workers:
        w.start()
    # Wait until they are all done
    for w in workers:
        w.join()
        
def process_task(queue, target_path):
    '''
    This is the callback passed to the workers
    '''
    pid = multiprocessing.current_process()
    while not queue.empty():
        task = get_work(queue)
        print("T: " + str(task.name) + " P: " +  str(pid))
        # download URL to path
        fname = task.name + ".csv"
        D.download_file_to(task.url, fname, target_path)     
    if queue.empty():
        print("Done: " + str(pid))

def populate_q_with_work(file_descriptor):
    p = Process(target=read_urls_to_queue, args=(file_descriptor,))
    p.start()

def read_urls_to_queue(csvfile):
    '''
    Reads URLs of a given type from a file and push
    them to the queue to generate work
    '''
    with open(csvfile, 'r') as f:
        csvreader = csv.reader(f, delimiter=",")
        for (dname, did, dformat, durl) in csvreader:
            # Filter by format
            if dformat in C.ok_formats:
                t = Task(dname, did, dformat, durl)
                push_work(t, queue)
        
def test():
    for i in range(10):
        push_work(i, queue)
    start_workers("./")


if __name__ == "__main__":
    print("TODO")
    push_work(["1", "raul"])
    push_work(["2", "jonathan"])
    push_work(["3", "holger"])
    init_and_start_worker_pool()
    
    
