import sys
import mpi4py
import threading
mpi4py.rc.initialize = False

from mpi4py import MPI

MPI.Init_thread(2)

from time import sleep, time
from ship import Ship
from constants import Data

def foo(*args):
    while True:
        sleep(3)
        print('XDDDDDD')


if __name__ == '__main__':
    data = Data(
        size=MPI.COMM_WORLD.Get_size(), 
        channels=4, 
        capacity=[1,1,2,2]
    )

    ship = Ship(data=data)
    #ship.run()

    x = threading.Thread(target=ship.run)
    y = threading.Thread(target=ship.second_thread_keep_communication)
    x.start()
    y.start()
