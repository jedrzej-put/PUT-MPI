import re
import numpy as np

#############
channel = 4
capacity = np.array([1,1,2,2], dtype=int)
############

def check_log():
    for i in range(channel):
        if ships[i] > capacity[i]:
            return True
    return False

def check_direction(channel_id, ship_direction):
    if ships[channel_id] == 1:
        direction[channel_id] = ship_direction
        return False
    elif direction[channel_id] == ship_direction:
        return False
    else:
        return True
    
    


with open('log.txt') as f:
    lines = f.readlines()



ships = np.zeros(channel,dtype=int)
direction = {}
for i, line in enumerate( lines):
    if re.search("Plyne w prawa", line):
        channel_num = int(line.split(' ')[-1])
        ships[channel_num] += 1
        if check_direction(channel_num, ship_direction="PRAWA"):
            print(f"ERROR DIRECTION ships:{ships}, capacity:{capacity}, direction:{direction}")
            print(f"line:{i+1}  {line}")
            exit(1)
    elif re.search("Plyne w lewa", line):
        channel_num = int(line.split(' ')[-1])
        ships[channel_num] += 1
        if check_direction(channel_num, ship_direction="LEWA"):
            print(f"ERROR DIRECTION ships:{ships}, capacity:{capacity}, direction:{direction}")
            print(f"line:{i+1}  {line}")
            exit(1)
    elif re.search("Opuszczam", line):
        channel_num = int(line.split(' ')[-1])
        ships[channel_num] -= 1

    if check_log():
        print(f"ERROR ships:{ships}, capacity:{capacity}")
        print(f"line:{i+1}  {line}")
        exit(1)

print(f"ships:{ships}, capacity:{capacity}")
