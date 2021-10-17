# https://stackoverflow.com/a/59141032/1742064


import threading
import cProfile
from time import sleep
from pstats import Stats
import pstats
from time import time
import threading
import sys

# using different times to ensure the results reflect all threads
SHORT = 0.5
MED = 0.715874
T1_SLEEP = 1.37897
T2_SLEEP = 2.05746
ITER = 1
ITER_T = 4


class MyThreading(threading.Thread):
    """ Subclass to arrange for the profiler to run in the thread """
    def run(self):
        """
        Here we simply wrap the call to self._target (the callable passed as
        arg to MyThreading(target=....) so that cProfile runs it for us,
        and thus is able to profile it.

        Since we're in the current instance of each threading object at
        this point, we can run arbitrary number of threads & profile all of them
        """
        try:
            if self._target:
                # using the name attr. of our thread to ensure unique profile filenames
                cProfile.runctx(
                    'self._target(*self._args, **self._kwargs)',
                    globals=globals(),
                    locals=locals(),
                    filename= f'full_server_thread_{self.name}')
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs


def main(args):
    """ Main func. """
    thread1_done =threading.Event()
    thread1_done.clear()
    thread2_done =threading.Event()
    thread2_done.clear()

    print("Main thread start.... ")
    t1 = MyThreading(target=thread_1, args=(thread1_done,), name="T1" )
    t2 = MyThreading(target=thread_2, args=(thread2_done,), name="T2" )
    print("Subthreads instances.... launching.")

    t1.start()          # start will call our overrident threading.run() method
    t2.start()

    for i in range(0,ITER):
        print(f"MAIN iteration: {i}")
        main_func_SHORT()
        main_func_MED()

    if thread1_done.wait() and thread2_done.wait():
        print("Threads are done now... ")
        return True


def main_func_SHORT():
    """ Func. called by the main T """
    sleep(SHORT)
    return True


def main_func_MED():
    sleep(MED)
    return True


def thread_1(done_flag):
    print("subthread target func 1 ")
    for i in range(0,ITER_T):
        thread_func(T1_SLEEP)
    done_flag.set()


def thread_func(SLEEP):
    print(f"Thread func")
    sleep(SLEEP)


def thread_2(done_flag):
    print("subthread target func 2 ")
    for i in range(0,ITER_T):
        thread_func(T2_SLEEP)
    done_flag.set()


if __name__ == '__main__':

    import sys
    args = sys.argv[1:]
    cProfile.run('main(args)', f'full_server_profile')
    stats = Stats('full_server_profile')
    stats.add('full_server_thread_T1')
    stats.add('full_server_thread_T2')
    stats.sort_stats('filename').print_stats()
