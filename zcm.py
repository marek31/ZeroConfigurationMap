import Pyro4 #distributed messaging system
#import pickle #serializer
import dill   #serializer
import socket, threading, queue, time, collections, os, sys, random, datetime, subprocess, argparse #helpers
import ctypes, tempfile, psutil #windows specific

import gc


serializer = dill
#serializer = pickle
prefix = 'workerpool'
priority = 'idle'
comm_timeout=0

def _set_task_priority(priority):
    ''' Helper function. Set current task priority on windows '''
    psutil.Process(os.getpid()).set_nice(
    {   'idle':    psutil.IDLE_PRIORITY_CLASS,
        'below':   psutil.BELOW_NORMAL_PRIORITY_CLASS,
        'normal':  psutil.NORMAL_PRIORITY_CLASS,
        'above':   psutil.ABOVE_NORMAL_PRIORITY_CLASS,
        'high':    psutil.HIGH_PRIORITY_CLASS,
        'realtime':psutil.REALTIME_PRIORITY_CLASS}
        [priority])     

def _set_console_title(title):
    ''' Helper function . Set the title of the current console window '''
    ctypes.windll.kernel32.SetConsoleTitleA(str(title).encode('latin-1'))

def list(workerpool_prefix = None):
    ''' Lists all reachable workers. Any worker that's not reachable will be removed from the name serer '''
    name_server = Pyro4.locateNS()
    discovered_worker_names = name_server.list(workerpool_prefix or prefix).keys()
    def is_reachable(worker_name):
        try:
            Pyro4.Proxy('PYRONAME:' + worker_name).ping()
            return True
        except Exception as e:       
            return False         
    return [w for w in discovered_worker_names if is_reachable(w) or not name_server.remove(w)]


class worker:
    ''' worker(...) The remote worker. 

        worker(workerpool_prefix = worker.prefix, default 'workerpool'
               verbose = False, 
               log = True, 
               log_folder = local user temp folder
               task_priority = 'idle')

        Optional arguments:
        workerpool_prefix: choose your workerpool prefix
        verbose = prints the status of executed functions
        log = logs the status of the exacuted functions
        log_folder = specify the log folder
        task_priority = specify worker task priority: 'idle', 'below', 'normal', 'above', 'high', 'realtime' '''

    def __init__(self, 
                 workerpool_prefix = None, 
                 verbose = False, 
                 log = True, 
                 log_folder = None, 
                 task_priority = None):
        _set_task_priority(task_priority or priority)
        suffix = str(random.randint(0, 2**32)).zfill(len(str(2**32)))
        self.hostname = socket.gethostname().lower()
        self.worker_name = '{}.{}.{}'.format(workerpool_prefix or prefix, self.hostname,suffix)
        _set_console_title(self.worker_name)
        Pyro4.config.SERIALIZERS_ACCEPTED = set(['pickle', 'json', 'marshal', 'serpent'])          
        Pyro4.config.SERIALIZER = 'pickle' 
        Pyro4.config.SERVERTYPE = "multiplex"
        Pyro4.config.COMMTIMEOUT = comm_timeout        
        self.signal_shutdown = False
        self.verbose = verbose
        if log:
            self.logfilename = os.path.join(log_folder if log_folder else tempfile.gettempdir(), datetime.datetime.now().strftime('%Y.%m.%d %H.%M.%S') + ' ' + self.worker_name + '.log')
        else:
            self.logfilename = None

    def run(self):
        self._print_and_log("Started up as {}".format(self.worker_name))
        while not self.signal_shutdown:
            try:
                self.daemon = Pyro4.Daemon(self.hostname)         
                Pyro4.locateNS().register(self.worker_name, self.daemon.register(self))
                self._print_and_log ('Connected to Pyro Name Server')                
                self.daemon.requestLoop()
            except TypeError as e:
                #todo - probably it needs to call daemon shutdown from another thread
                #calling close on daemon causes this error
                pass
            except Pyro4.errors.DaemonError as e:
                #keyboard interrput when idling causes this error Pyro4.errors.DaemonError
                self._print_and_log ('Terminated by keyboard interrupt')
                self.signal_shutdown = True
            except KeyboardInterrupt as e:
                self._print_and_log ('Terminated by keyboard interrupt')
                self.signal_shutdown = True
            except Exception as e:
                #self._print_and_log ('Worker error - check if Pyro Name Server is reachable. Will retry in 10s. Exception type:{}, exception:'.format(type(e), e))
                self._print_and_log ('Worker error - probably disconnected from Pyro name server. Will retry in 10s. type:{}, e:{}'.format(type(e),e))
                time.sleep(10)
        self._print_and_log ('Worker has exited')                

    def ping(self): 
        pass

    def _print_and_log(self, message):
        message_timstamped = datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S.%f ') + message
        if self.verbose: print (message_timstamped)
        if self.logfilename: open(self.logfilename, 'a').write(message_timstamped+'\n')

    def execute_serialized_func(self, function_serialized, arg_serialized):
        try:
            tic = time.time()
            #deserialize the function
            function = serializer.loads(function_serialized)

            #find out function name if available (e.g it doesn't work for functools.partial)
            try:            
                function_name = function.__name__
            except:
                function_name = '(no function name)'
            self._print_and_log("{} received function: '{}'".format(self.worker_name, function_name))

            #deserialize the arguments
            arg = serializer.loads(arg_serialized)

            #execute the function
            result = function(arg)          
            self._print_and_log("{} finished function: '{}' elapsed:{:3}s".format(self.worker_name, function_name, time.time() - tic))

            #clean up - useful when processing large data sets
            del arg, arg_serialized, function, function_serialized, function_name
            gc.collect()
        except KeyboardInterrupt as e:
            self._print_and_log('KeyboardInterrupt')
            self.signal_shutdown = True
            #todo - it needs to call daemon shutdown from another thread othrewise it causes TypeError in run()
            self.daemon.close()
            raise #workerpool will see it as Pyro4.errors.ConnectionClosedError
        return result



class _ProcessPullDataWrapper(threading.Thread):
    def __init__(self, function_serialized, iterable_input, results_queue, current_workers, input_queue, temp_function_name):
        super().__init__()
        self.function_serialized = function_serialized
        self.iterable_input = iterable_input
        self.results_queue = results_queue
        self.signal_shutdown = False
        self.signal_input_exhausted = False
        self.current_workers = current_workers
        self.counter_input_pulled = 0
        self.input_queue = input_queue
        self.start()
        self.temp_function_name = temp_function_name

    def run(self):
        '''Thread responsible for pulling in the data from the input iterator and putting it on an internal queue'''
        #wait for any available workers 
        while not self.current_workers and not self.signal_shutdown: 
            time.sleep(0.002)

        while not self.signal_shutdown and not self.signal_input_exhausted:
            if self.iterable_input: #TODO is it necessary?
                for id, input_value in enumerate(self.iterable_input): 
                    #pauses when the input queue is overpopulated
                    while (len(self.input_queue) >= max(2, len(self.current_workers) // 4) and not self.signal_shutdown):
                        time.sleep(0.002)
                    if self.signal_shutdown: break
                    self.input_queue.appendleft((id, self.function_serialized, serializer.dumps(input_value), self.results_queue))
                    self.counter_input_pulled += 1
                self.signal_input_exhausted = True
            else:
                time.sleep(0.002)


class executor:
    class _WorkerProxy(threading.Thread):
        def __init__(self, executor, worker_name, verbose):
            super().__init__()
            self.name = worker_name
            self.executor = executor
            self.verbose = verbose
            self.start()
            self.executor._print_and_log('Adding worker: ' + worker_name)

        def run(self):
            try:
                workerproxy = Pyro4.Proxy('PYRONAME:' + self.name)
                #test if the worker is reachable
                workerproxy.ping()

                commit_suicide = False 
                while not self.executor.signal_shutdown and not commit_suicide:
                    try:
                        item = self.executor.input_queue.pop()
                        id, function_serialized, value, results_queue = item

                        if results_queue.qsize() < len(self.executor.current_workers):
                            result = workerproxy.execute_serialized_func(function_serialized, value)
                            results_queue.put((id, result))
                        else:
                            self.executor.input_queue.append(item)
                            time.sleep(0.01)

                    except IndexError:
                        time.sleep(0.01)
                    except (Pyro4.errors.TimeoutError) as e:
                        #it does happen to have a timeout when processing tons of data with timeout=360, temporarily increased to 3600
                        self.executor._print_and_log('Pyro4.errors.TimeoutError. Removing from workerpool: {}'.format(self.name))
                        self.executor.input_queue.append(item)
                        commit_suicide = True
                        self.executor._remove_dead_worker(self.name)
                    except (Pyro4.errors.ConnectionClosedError) as e:
                        #keyboardinterrupt or worker probably died
                        self.executor._print_and_log('Pyro4.errors.ConnectionClosedError. Removing from workerpool: {}'.format(self.name)) 
                        self.executor.input_queue.append(item)
                        commit_suicide = True
                        self.executor._remove_dead_worker(self.name)
                    except (Pyro4.errors.NamingError) as e:
                        self.executor._print_and_log('Pyro4.errors.NamingError. Removing from workerpool: {}'.format(self.name)) 
                        self.executor.input_queue.append(item)
                        commit_suicide = True
                        self.executor._remove_dead_worker(self.name)
                    except (Pyro4.errors.CommunicationError) as e:
                        self.executor._print_and_log("Pyro4.errors.CommunicationError. Removing from workerpool: {}".format(self.name))
                        self.executor.input_queue.append(item)
                        commit_suicide = True
                        self.executor._remove_dead_worker(self.name)
                    except (ConnectionError) as e:
                        self.executor._print_and_log("ConnectionError. Removing from workerpool: {}".format(self.name)) 
                        self.executor.input_queue.append(item)
                        commit_suicide = True
                        self.executor._remove_dead_worker(self.name)
                    except Exception as e:                
                        self.executor.input_queue.append(item)
                        self.executor._print_and_log('Remote error. Exception type:{}, e:{}'.format(type(e), e))
                        self.executor.exception = e
                        # commit_suicide = True
                        self.executor._print_and_log('Remote traceback:\n'+"".join(Pyro4.util.getPyroTraceback()))
                        # self.executor._remove_dead_worker(self.name)

            except (Pyro4.errors.CommunicationError, ConnectionError) as e:
                self.executor._print_and_log('Worker unreachable. Removing from workerpool: {}'.format(self.name))
                self.executor._remove_dead_worker(self.name)
            except Exception as e:                
                self.executor._print_and_log('Other error when pinging the worker. Removing from workerpool: {}. Exception type:{}, e:{}'.format(self.name, type(e), e))
                self.executor._remove_dead_worker(self.name)


    def __init__(self, workerpool_prefix= None, log= False, log_folder= None, verbose= False):
        self.workerpool_prefix = workerpool_prefix or prefix
        Pyro4.config.SERIALIZERS_ACCEPTED = set(['pickle', 'json', 'marshal', 'serpent'])          
        Pyro4.config.SERIALIZER = 'pickle' 
        Pyro4.config.COMMTIMEOUT = comm_timeout
        self.current_workers = set()
        self.threads = set()
        self.signal_shutdown = False        
        self.exception = None
        self.verbose = verbose
        self.input_queue = collections.deque()        
        self.pyro_name_server = Pyro4.locateNS()
        if log:
            self.logfilename = os.path.join(tempfile.gettempdir() if not log_folder else log_folder, '{} executor.log'.format(datetime.datetime.now().strftime('%Y.%m.%d %H.%M.%S')))
        else:
            self.logfilename = None

        workers_discovery = threading.Thread(target=self._workers_discovery, name= 'workers_discovery')            
        workers_discovery.start()
        self.threads.add(workers_discovery)

        while not self.current_workers and not self.exception: 
            time.sleep(0.005)

    def __del__(self):
        #wait for all threads to shut down
        self.signal_shutdown = True
        for i in self.threads: 
            i.is_alive() and i.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.signal_shutdown = True
        for i in self.threads: 
            i.is_alive() and i.join()

    def _print_and_log(self, message):
        message_timstamped = datetime.datetime.now().strftime('%Y.%m.%d %H.%M.%S.%f') + ' ' + message
        if self.verbose: print (message_timstamped)
        if self.logfilename: open(self.logfilename, 'a').write(message_timstamped+'\n')

    def _workers_discovery(self):
        ''' Thread responsible work discovering new online workers. Runs in 3s loop '''
        while not self.signal_shutdown:            
            try:
                workers_now = set(Pyro4.locateNS().list(self.workerpool_prefix or prefix))
            except:
                #nothing to do if name server is dead or not reachable, we'll retry
                workers_now = set()

            new_workers = workers_now - self.current_workers
            for worker in new_workers:
                self.threads.add(self._WorkerProxy(self, worker, self.verbose))
                self.current_workers.add(worker)

            if not self.current_workers:  
                self.signal_shutdown = True
                self.exception = RuntimeError('No workers to do the job')
            else:
                time.sleep(3)


    def _remove_dead_worker(self, worker):
        '''Removes an unreachable worker from the list of workers and the name server'''
        #TODO may need locking here
        try:
            self.current_workers.remove(worker)
            self.pyro_name_server.remove(worker)
        except: #can throw if name server not found (or got killed)            
            pass        


    def _map(self, function, iterable_input):
        '''Internal implementation of an unsorted distributed yielding map'''
        # print('_mape function name:', function.__name__)
        function_serialized = serializer.dumps(function)
        # print('serialized function:', function_serialized)
        results_queue = queue.Queue()
        counter_results_produced = 0      

        pull_data_thread = _ProcessPullDataWrapper(function_serialized, iterable_input, results_queue, self.current_workers, self.input_queue, 'pull_data_thread')

        sleep = False
        while not self.signal_shutdown and not self.exception:
            try:
                if sleep: 
                    time.sleep(0.001)
                    sleep = False
                try:
                    yield results_queue.get(False) #--------------------------
                    counter_results_produced += 1
                except queue.Empty:
                    sleep = True
                self.signal_shutdown = self.signal_shutdown or ((counter_results_produced == pull_data_thread.counter_input_pulled) and pull_data_thread.signal_input_exhausted)
            except (Exception, KeyboardInterrupt) as e:
                self._print_and_log('Exception caught. Waiting for the threads to finish processing')
                self.signal_shutdown = True
                self.exception = e

        pull_data_thread.signal_shutdown = True
        pull_data_thread.join()

        #raises the last exception that was caught
        if self.exception: 
            raise self.exception

    def map_ordered(self, function, iterable_input):
        '''Distributed sorted map as generator. 
           If you don't need the results to be ordered then use faster map()'''
        current_id = 0
        result_queue = queue.PriorityQueue()
        for item in self._map(function, iterable_input):
            result_queue.put(item)
            while result_queue.empty() is False and current_id == result_queue.queue[0][0]:
                    current_id += 1
                    yield result_queue.get()[1]

    def map(self, function, iterable_input):
        '''Distributed unordered map as generator.'''
        for id, value in self._map(function, iterable_input):  
            yield value

    # def submit(self, results_queue, function, *args):
    #     function_serialized = serializer.dumps(function)
    #     self.input_queue.appendleft((id, function_serialized, serializer.dumps(*args), results_queue))

    # def reduce(self, function, args, init):
    #     #todo
    #     pass


def launch_workers(processes = None, create_new_console = False, verbose=True):
    '''launch_workers(n = None)
       Launch a bunch of workers. If not specified, the number of workers depend on the physical processor cores.
      
       Optional arguments:
       processes - override for a number of workers to launch'''

    number_of_physical_processors = psutil.cpu_count() // 2
    number_of_processes = processes if processes else number_of_physical_processors

    print('Launching {} subprocesses'.format(number_of_processes))
    for i in range(number_of_processes):
        args = ['python', 'zcm.py', '--processes', '1']
        if not verbose:
            args.append('--noverbose')

        subprocess.Popen(args, creationflags=subprocess.CREATE_NEW_CONSOLE if create_new_console else 0)


def worker_run(verbose=False):
    '''Runs a single instance of a worker '''
    try:
        worker(verbose=verbose).run()
    except (Pyro4.errors.TimeoutError, Pyro4.errors.ConnectionClosedError, Pyro4.errors.CommunicationError):
        print("Name Server hasn't been found")
    except Exception as e:
        print("worker_run() exception caught type(e):{} e:{}".format(type(e), e))
    input('\npress enter to exit')


if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--processes', type=int,  help='specifies the number of workers to run')
    parser.add_argument('-s', '--separate',  help='launch workers in separate windows', action="store_true")
    parser.add_argument('-m', '--mute', '--noverbose', help='turn verbosity off', action="store_false")
    args = parser.parse_args()
    if args.processes == 1 and not args.separate:
        worker_run(args.mute)
    else:
        launch_workers(processes = args.processes, create_new_console = args.separate, verbose=args.mute)
