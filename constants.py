from enum import IntEnum, Enum


class Data:
    def __init__(self, size, channels, capacity):
        self.P = size
        self.K = channels
        self.capacity = capacity


class MessageTag(IntEnum):
    ACK = 1
    ACK_EXCEPT = 2
    ACK_ALL = 3
    BLOCK = 4
    UNBLOCK = 5
    WAIT = 6
    UNWAIT = 7
    REQ = 8


class State(Enum):
    LEFT_SIDE = 1
    RIGHT_SIDE = 2
    SAIL_TO_LEFT_SIDE = 3
    SAIL_TO_RIGHT_SIDE = 4
    GATHER_AGREEMENTS_LEFT_SIDE = 5
    GATHER_AGREEMENTS_RIGHT_SIDE = 6



