import zcm, time, concurrent.futures, queue, Pyro4, numpy as np

def helper_create_array():
    rows=10000
    cols=100
    for array_number in range(10):
        array = np.reshape(np.arange(rows*cols),(rows,cols))*1.0/(array_number+1)
        yield array

def remote_function(array):
    import numpy as np
    number_of_columns = array.shape[1]
    for i in range(number_of_columns-1):
        #slow me down
        for j in range(5000):
            array[:,i] = (array[:,i] + array[:,i+1])/2
    return array.sum()

def remote_function_with_error(dummy):
    raise 1.0/0.0


def test_local_map():
    results_map_sum = 0.0
    tic_local = time.time()
    print("Calculating sum of arrays with Python's map (single threaded)")
    for array_number, array_sum in enumerate(map(remote_function, helper_create_array())):
        print('sum of array number {}:{}'.format(array_number, array_sum))
        results_map_sum += array_sum
    print('elapsed:',time.time() - tic_local,'\n')
    return results_map_sum

def test_process_pool_map():
    tic_process_pool = time.time()
    result_process_pool_map_sum = 0.0
    print("Calculating sum of arrays with concurrent.futures.ProcessPoolExecutor")
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for array_number, array_sum in enumerate(executor.map(remote_function, helper_create_array())):
            print('sum of array number {}:{}'.format(array_number, array_sum))
            result_process_pool_map_sum += array_sum
    print('elapsed:',time.time() - tic_process_pool,'\n')
    return result_process_pool_map_sum

def test_zcf_map_unordered():
    tic_remote_unordered = time.time()
    result_zcm_map_sum = 0.0
    with zcm.executor(verbose=True) as executor:
        print("Calculating sum of arrays with Zero Configuration Map map (unordered)")
        for array_number, array_sum in enumerate(executor.map(remote_function, helper_create_array())):
            print('sum of array number {}:{}'.format(array_number, array_sum))
            result_zcm_map_sum += array_sum
    print('elapsed:',time.time() - tic_remote_unordered,'\n')
    return result_zcm_map_sum

def test_zcf_map_ordered():
    tic_remote_ordered = time.time()
    result_zcm_map_ordered_sum = 0.0
    with zcm.executor(verbose=True) as executor:
        print("Calculating sum of arrays with Zero Configuration Map map (ordered)")
        for array_number, array_sum in enumerate(executor.map_ordered(remote_function, helper_create_array())):
            print('sum of array number {}:{}'.format(array_number, array_sum))
            result_zcm_map_ordered_sum += array_sum
    print('elapsed:',time.time() - tic_remote_ordered,'\n')
    return result_zcm_map_ordered_sum

# def test_zcf_submit():
#     tic_zcm_submit = time.time()
#     print("Calculating sum of array with Zero Configuration Map submit")
#     results_queue = queue.Queue()
#     with zcm.executor(verbose=True) as executor:
#         for array in helper_create_array():
#             executor.submit(results_queue, remote_function, array)

#         print('Waiting for all results to arrive')
#         while results_queue.qsize() != 10:
#             # time.sleep(0.01)
#             time.sleep(5)
#             print("results_queue.qsize():",results_queue.qsize())
#             for i in  range(results_queue.qsize()):
#                 print ('results_queue.queue[0]:',results_queue.queue[i][1])

#         result_zcm_submit_sum = 0.0
#         array_number = 0
#         while results_queue.qsize():
#             print('sum of array number {}:{}'.format(array_number, array_sum))
#             result_zcm_submit_sum += results_queue.get()         
#             array_number =+ 1
    
#     print('elapsed:',time.time() - tic_zcm_submit,'\n')
#     return result_zcm_submit_sum

def test_compare_map_functions():
    '''Compares execution time of several map functions - map, process pool executor, Zero Configuration Map unordered, Zero Configuration Map ordered'''
    try:
        result_zcm_map_sum = test_zcf_map_unordered()
        result_zcm_map_ordered_sum = test_zcf_map_ordered()
        # result_zcm_submit_sum = test_zcf_submit()
        results_map_sum = test_local_map()
        result_process_pool_map_sum = test_process_pool_map()

        compare_maps_result = all((results_map_sum == result_process_pool_map_sum,
                                  results_map_sum == result_zcm_map_sum, 
                                  results_map_sum == result_zcm_map_ordered_sum,
                                 # results_map_sum == result_zcm_submit_sum,
                                 ))
        print('Comparing results. Test passed?', compare_maps_result)
        return compare_maps_result
    except (RuntimeError, Pyro4.errors.NamingError):
        print("Error - can't find any Zero Configuration Map remote workers.\n"+
              "1. start pyro Name server with pyro__start_name_server.bat\n"+
              "2. launch one or more workers on local or remote pc(s) with launch_workers.bat or launch_workers_one_console.bat\n")


def test_remote_error(): 
    '''Tests passing exceptions from remote workers'''
    print('\nRemote exception test\n')
    remote_exception_thrown = False
    try:
        with zcm.executor(verbose=True) as executor:
            for result in executor.map(remote_function_with_error, range(20)):
                pass
    except ZeroDivisionError:
        remote_exception_thrown = True
    except (RuntimeError, Pyro4.errors.NamingError):
        print("Error - can't find any Zero Configuration Map remote workers.\n"+
              "1. start pyro Name server with pyro__start_name_server.bat\n"+
              "2. launch one or more workers on local or remote pc(s) with launch_workers.bat or launch_workers_one_console.bat\n")

    print('Remote error test passed?', remote_exception_thrown)
    return remote_exception_thrown

if __name__ == '__main__':
    print('\n\nAll tests passed?', 
        all((
            test_compare_map_functions(),
            test_remote_error(),
        )))
