from re import L
from mpi4py import MPI
from constants import State, MessageTag
from time import sleep, time
from random import uniform, choice
import threading


class Ship:
    def __init__(self, data):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.status = MPI.Status()
        self.state = choice([State.LEFT_SIDE, State.RIGHT_SIDE])  # State.LEFT_SIDE if self.rank % 2 == 0 else State.RIGHT_SIDE
        print(f"Statek {self.rank}: Zaczynam po {self.state}")
        self.data = data
        self.lamport = 0
        self.wait_counter = 0
        self.answer_counter = 0
        self.permissions = [0 for _  in range (self.data.K)]
        self.stopped_ships_opposite_side = set()
        self.stopped_ships_same_side = set()
        self.denied_permissions = set()
        self.holded_permissions = set()
        self.got_res = set()
        self.last_res_tag = {}
        self.request_id = 0
        self.channel = -1
        self.sleep_time = 0
        self.cv = threading.Condition(lock=threading.Lock())
        

        
        open(f"./logs/{self.rank}_req.txt", 'w').close()
        open(f"./logs/{self.rank}_res.txt", 'w').close()
        open(f"log.txt", 'w').close()

    def write_log(self, msg):
        with open(f"log.txt", 'a') as the_file:
            the_file.write(f"{msg}\n")

    def write_res(self, msg):
        with open(f"./logs/{self.rank}_req.txt", 'a') as the_file:
            the_file.write(f"{msg}\n")
    
    def write_req(self, msg, second=False):
        with open(f"./logs/{self.rank}_res.txt", 'a') as the_file:
            if second:
                the_file.write(f"{msg} - 2 watek\n")
            else:
                the_file.write(f"{msg}\n")

    def agree_on_all(self):
        self.permissions = [p + 1 for p in self.permissions]
        self.answer_counter += 1
        
    def agree_on_all_except_one(self, channel_id):
        self.permissions = [p + 1 if i != channel_id else p for i, p in enumerate(self.permissions)]
        self.answer_counter += 1

    def agree_on_one(self, channel_id):
        self.permissions[channel_id] += 1

    def block_channel(self, channel_id):
        self.permissions[channel_id] -= self.data.capacity[channel_id]
        self.permissions = [p + 1 if i != channel_id else p for i, p in enumerate(self.permissions)]
        self.answer_counter += 1

    def unblock_channel(self, channel_id):
        self.permissions[channel_id] += self.data.capacity[channel_id] + 1


    def rollback_agree_on_all(self):
        self.permissions = [p - 1 for p in self.permissions]
        self.answer_counter -= 1
        
    def rollback_agree_on_all_except_one(self, channel_id):
        self.permissions = [p - 1 if i != channel_id else p for i, p in enumerate(self.permissions)]
        self.answer_counter -= 1
    
    def rollback_block_channel(self, channel_id):
        self.permissions[channel_id] += self.data.capacity[channel_id]
        self.permissions = [p - 1 if i != channel_id else p for i, p in enumerate(self.permissions)]
        self.answer_counter -= 1

    def rollback_agreement_resend_request(self, sender):
        if self.last_res_tag[sender] == MessageTag.ACK_ALL:
            self.rollback_agree_on_all()
        elif self.last_res_tag[sender] == MessageTag.ACK_EXCEPT:
            self.rollback_agree_on_all_except_one()
        elif self.last_res_tag[sender] == MessageTag.BLOCK:
            self.rollback_block_channel()

        self.send_request(
            message={
                "request_id": self.request_id - 1,#bo w send_requests zwikeszylismy o 1 a to jest do poprzedniego
                "direction": State.SAIL_TO_RIGHT_SIDE if self.state == State.GATHER_AGREEMENTS_LEFT_SIDE else State.SAIL_TO_LEFT_SIDE,
                "state": self.state, #State.GATHER_AGREEMENTS_LEFT_SIDE | State.GATHER_AGREEMENTS_RIGHT_SIDE
                "lamport": self.lamport,
            },
            destination=sender
        )
        
        
    def channel_is_available(self, channel_id, permission_counter):
        # print(f"STATEK {self.rank} permission:{self.permissions}")
        return permission_counter >= self.data.P - self.data.capacity[channel_id]
    
    def find_channel(self):
        for channel_id, permission_counter in enumerate(self.permissions):
            if self.channel_is_available(channel_id, permission_counter):
                self.channel = channel_id
                return True
        return False

    def stop_and_sleep(self):
        #self.sleep_time = uniform(1, 2)*uniform(1, 2)*uniform(1, 2)
        self.sleep_time = 1

        
        # if self.state in (State.SAIL_TO_LEFT_SIDE, State.SAIL_TO_RIGHT_SIDE):
        #     self.sleep_time = 1000000
        if self.state in  (State.LEFT_SIDE, State.RIGHT_SIDE):
            side = "lewej stronie" if self.state == State.LEFT_SIDE else "prawej stronie"
            print(f"Statek {self.rank}: Czekam po {side}.")
            self.write_log(f"Statek {self.rank}: Czekam po {side}.")

        elif self.state in  (State.SAIL_TO_LEFT_SIDE, State.SAIL_TO_RIGHT_SIDE):
            # sleep(uniform(0, 1))
            direction = "w lewa strone" if self.state == State.SAIL_TO_LEFT_SIDE else "w prawa strone"
            print(f"Statek {self.rank}: Plyne {direction} w kanale nr: {self.channel}")
            self.write_res(f"Statek {self.rank} req_id:{self.request_id - 1}: Plyne {direction} w kanale nr: {self.channel}")
            self.write_req(f"Statek {self.rank} req_id:{self.request_id - 1}: Plyne {direction} w kanale nr: {self.channel}")
            self.write_log(f"Statek {self.rank} req_id:{self.request_id - 1}: Plyne {direction} w kanale nr: {self.channel}")
  
        #powiadom drugi watek ze idzeisz spac i on ma odbierac wiadomosci
        #self.write_log(f"statek:{self.rank} zaraz powiadomie 2 watek")

        if self.cv.acquire(False):
            #self.write_log(f"statek:{self.rank} WITH 2 watek")
            self.cv.notify()
            self.cv.release()

        #self.write_log(f"statek:{self.rank} wlasnie powiadomilem")
        #self.write_log(f"statek:{self.rank} ide spac na {self.sleep_time}")
        sleep(self.sleep_time)
        #self.write_log(f"statek:{self.rank} skonczylem spac")


    def is_first_thread_asleep(self):
        return True if self.state in (State.LEFT_SIDE, State.RIGHT_SIDE, State.SAIL_TO_LEFT_SIDE, State.SAIL_TO_RIGHT_SIDE) else False
            

    def second_thread_keep_communication(self):
        with self.cv:
            # poczekaj az statek bedzie w stanie(...) cczyli tamten watek spi wiec tu sa odbierane komunikaty
            #self.write_log(f"Staek:{self.rank}: 2 watek czekam za stanem dobrym")
            self.cv.wait_for(self.is_first_thread_asleep)
            #self.write_log(f"Staek:{self.rank}: 2 watek mam dobry stan")
            self.recive_request_or_respond(second=True)
            #self.write_log(f"Staek:{self.rank}: 2 watek wyzej moja odp")

    def actions_before_entry_to_critical_section(self):
        self.permissions = [0 for _ in range (self.data.K)]
        self.stopped_ships_opposite_side.clear()
        self.stopped_ships_same_side.clear()
        self.denied_permissions.clear()
        self.holded_permissions.clear()
        self.got_res.clear()
        self.wait_counter = 0
        self.answer_counter = 0

    def actions_after_enter_to_critical_section(self):
        #Jeśli wejdziemy do i-tej sekcji krytycznej to dla wszystkich z stopped_ships_opposite_side:wysyłamy UNWAIT i BLOCK(i) - dajemy zgodę na wszystkie kanały oprócz tego co zajmiemy izapisujemy go na denied_permissions
        for ship_id, request_id in self.stopped_ships_opposite_side:
            

            self.comm.isend({
                "request_id": request_id,
                "channel_id": self.channel
            }, 
            dest=ship_id,
            tag=MessageTag.BLOCK)

            self.comm.isend({
                "request_id": request_id
            }, 
            dest=ship_id,
            tag=MessageTag.UNWAIT)
            self.write_req(f"Statek {self.rank}: stopped_ships_opposite_side: Wysyłam  na blokade na kanału {self.channel} i UNWAIT do Statka {ship_id} req_id:{request_id}")

            self.denied_permissions.add((ship_id, request_id))
        self.stopped_ships_opposite_side.clear()

        #Jeśli wejdziemy do i-tej sekcji krytycznej to dla wszystkich z stopped_ships_same_side: wysyłamy ACK_EXCEPT(i) - dajemy zgodę na wszystkie kanały oprócz tego co zajmiemy i zapisujemy go na holded_permissions
        for ship_id, request_id in self.stopped_ships_same_side:
            self.comm.isend({
                "request_id": request_id,
                "channel_id": self.channel
            }, 
            dest=ship_id,
            tag=MessageTag.ACK_EXCEPT)
            self.write_req(f"Statek {self.rank}: stopped_ships_SAME_side: Wysyłam  zgode na wszystko oprocz  kanał {self.channel}  do Statka {ship_id} req_id:{request_id}")

            self.holded_permissions.add((ship_id, request_id))
        self.stopped_ships_same_side.clear()



    
    def actions_after_leaving_to_critical_section(self):
        print(f"Statek {self.rank} req_id:{self.request_id - 1} : Opuszczam kanal nr: {self.channel}")
        self.write_req(f"Statek {self.rank}: Opuszczam kanal nr: {self.channel}")
        self.write_res(f"Statek {self.rank}: Opuszczam kanal nr: {self.channel}")
        self.write_log(f"Statek {self.rank}: Opuszczam kanal nr: {self.channel}")


        #Jeśli skończymy płynąć i-tym kanałem to wszystkim procesom z holded_permissions wysyłamy ACK(i).
        for ship_id, request_id in self.holded_permissions:
            self.comm.isend({
                "request_id": request_id,
                "channel_id": self.channel
            }, 
            dest=ship_id,
            tag=MessageTag.ACK)
            self.write_req(f"Statek {self.rank}: holded_permissions: Wysyłam  ACK na  kanał {self.channel}  do Statka {ship_id} req_id:{request_id}")

        self.holded_permissions.clear()

        #Jeśli skończymy płynąć i-tym kanałem to wszystkim procesom z denied_permissions wysyłamy UNBLOCK(i)
        for ship_id, request_id in self.denied_permissions:
            self.comm.isend({
                "request_id": request_id,
                "channel_id": self.channel
            }, 
            dest=ship_id,
            tag=MessageTag.UNBLOCK)
            self.write_req(f"Statek {self.rank}: denied_permissions: Wysyłam  UNBLOCK na  kanał {self.channel}  do Statka {ship_id} req_id:{request_id}")
        self.denied_permissions.clear()
        self.increment_lamport()

    def increment_lamport(self):
        self.lamport += 1

    def compare_priority(self, opponent_lamport, opponent_rank):
        if self.lamport < opponent_lamport: #mnijeszy zegar, wyzszy/lepszy priorytet
            #self.write_req(f"statek:{self.rank} MAM LEPSZYY PRIO bo lepszy lamport mylamport:{self.lamport} a opponnent_lanmport:{opponent_lamport} rank:{opponent_rank}")
            return True
        elif self.lamport > opponent_lamport: #wiekszy zegar, nizszy/slabszy priorytet
            return False
        elif self.rank < opponent_rank: #mnijeszy rank, wyzszy/lepszy priorytet
            #self.write_req(f"statek:{self.rank} MAM LEPSZYY PRIO bo lpeszy rank mylamport:{self.lamport} a opponnent_lanmport:{opponent_lamport} rank:{opponent_rank}")
            return True
        elif self.rank > opponent_rank: #wiekszy rank, nizszy/slabszy priorytet
            return False
    
    def send_request(self, message, destination):
        self.comm.isend(message, dest=destination, tag=MessageTag.REQ)
        

    def send_requests(self):
        for ship_id in range(self.data.P):
            if ship_id == self.rank: continue

            self.send_request(
                message={
                    "request_id": self.request_id,
                    "direction": State.SAIL_TO_RIGHT_SIDE if self.state == State.GATHER_AGREEMENTS_LEFT_SIDE else State.SAIL_TO_LEFT_SIDE,
                    "state": self.state, #State.GATHER_AGREEMENTS_LEFT_SIDE | State.GATHER_AGREEMENTS_RIGHT_SIDE
                    "lamport": self.lamport,
                },
                destination=ship_id
            )
        self.request_id += 1

    def handle_request(self, message, second=False):
        message_source = self.status.Get_source()
        #jeśli nie jesteśmy oraz nie ubiegamy się o dostęp do sekcji krytycznej to wysyłamy ACK_ALL
        #print(f"{self.rank}:: message: {message} tag: {message_tag}")
        if self.state in [State.LEFT_SIDE, State.RIGHT_SIDE]:
            self.comm.isend({
                "request_id": message["request_id"]
            }, 
            dest=message_source,
            tag=MessageTag.ACK_ALL)
            # print(f"Statek {self.rank}: Wysyłam zgode na wszystko do Statka {message_source}")
            self.write_req(f"Statek {self.rank}: Wysyłam zgode na wszystko do Statka {message_source} req_id:{ message['request_id']}",second=second)

        #jeśli jestesmy w i-tym kanale oraz płyniemy w tym samym kierunku
        elif self.state in [State.SAIL_TO_LEFT_SIDE, State.SAIL_TO_RIGHT_SIDE] \
            and self.state == message.get("direction"):
            self.comm.isend({
                "request_id": message["request_id"],
                "channel_id": self.channel
            }, 
            dest=message_source,
            tag=MessageTag.ACK_EXCEPT)
            self.holded_permissions.add((message_source, message["request_id"]))
            #print(f"Statek {self.rank}: Wysyłam zgode na wszystko prócz kanału {self.channel} do Statka {message_source}")
            self.write_req(f"Statek {self.rank}: Wysyłam zgode na wszystko prócz kanału {self.channel} do Statka {message_source} req_id:{ message['request_id']}",second=second)
        #jeśli jestesmy w i-tym kanale oraz płyniemy w przeciwnym kierunku to wysyłamy
        elif self.state in [State.SAIL_TO_LEFT_SIDE, State.SAIL_TO_RIGHT_SIDE] \
            and self.state != message["direction"]:
            self.comm.isend({
                "request_id": message["request_id"],
                "channel_id": self.channel
            }, 
            dest=message_source,
            tag=MessageTag.BLOCK)
            self.denied_permissions.add((message_source, message["request_id"]))
            #print(f"Statek {self.rank}: Wysyłam blokade na kanał {self.channel} do Statka {message_source}")
            self.write_req(f"Statek {self.rank}: Wysyłam blokade na kanał {self.channel} do Statka {message_source} req_id:{ message['request_id']}", second=second)

        #otrzymujemy REQ() - a sami też zbieramy zgody 
        elif self.state in [State.GATHER_AGREEMENTS_LEFT_SIDE, State.GATHER_AGREEMENTS_RIGHT_SIDE]:
            
            #i mamy niższy priorytet, ale nie dostalimy od niego ACK_ALL, ACK_EXCEPT, BLOCK TODO
            if not self.compare_priority(opponent_lamport=message["lamport"], opponent_rank=message_source) \
                and message_source not in self.got_res: # TODO
                self.comm.isend({
                    "request_id": message["request_id"]
                }, 
                dest=message_source,
                tag=MessageTag.ACK_ALL)
                # print(f"Statek {self.rank}: Wysyłam zgode na wszystko do Statka {message_source} bo mam mniejszy priorytet")
                self.write_req(f"Statek {self.rank}: state:{self.state} Wysyłam zgode na wszystko do Statka {message_source} bo mam mniejszy priorytet. my_lamport:{self.lamport}, przecwink:{message['lamport']} req_id:{ message['request_id']}", second=second)
            
            #gorszy prio i  dostalismy ACK_ALL | ACK_EXCEPT | BLOCK od niego
            elif not self.compare_priority(opponent_lamport=message["lamport"], opponent_rank=message_source) \
                and message_source  in self.got_res:
                #wycofaj zgode otrzymana
                self.got_res.discard(message_source)
                self.rollback_agreement_resend_request(sender=message_source)
                
                #daj zgode
                self.comm.isend({
                    "request_id": message["request_id"]
                }, 
                dest=message_source,
                tag=MessageTag.ACK_ALL)
                # print(f"Statek {self.rank}: Wysyłam zgode na wszystko do Statka {message_source} bo mam mniejszy priorytet")
                self.write_req(f"Statek {self.rank}: state:{self.state} Wycofuje otrzymana zgode i wysyłam moja zgode na wszystko do Statka {message_source} bo mam mniejszy priorytet. my_lamport:{self.lamport}, przecwink:{message['lamport']} req_id:{ message['request_id']}", second=second)
            
            #i mamy wyższy priorytet 
            else:
                #po tej samej stronie i mamy wyższy priorytet
                if  self.state == message["state"]:
                    self.stopped_ships_same_side.add((message_source, message["request_id"]))
                    # print(f"Statek {self.rank}: Nie wysyłam zgody do Statka {message_source} (po tej samej stronie) bo mam wiekszy priorytet")
                    self.write_req(f"Statek {self.rank}: Nie wysyłam zgody do Statka {message_source} (po tej samej stronie) bo mam wiekszy priorytet, my_lamport:{self.lamport}, przecwink:{message['lamport']} req_id:{ message['request_id']}", second=second)

                #zgody po przeciwnej stronie i mamy wyższy priorytet
                elif self.state != message["state"]:
                    self.comm.isend({
                        "request_id": message["request_id"]
                    }, 
                    dest=message_source,
                    tag=MessageTag.WAIT)
                    self.stopped_ships_opposite_side.add((message_source, message["request_id"]))
                    # print(f"Statek {self.rank}: Nie wysyłam zgody do Statka {message_source} (po innej stronie) bo mam wiekszy priorytet")
                    self.write_req(f"Statek {self.rank}: Nie wysyłam zgody do Statka {message_source} (po innej stronie) bo mam wiekszy priorytet my_lamport:{self.lamport}, przecwink:{message['lamport']} req_id:{ message['request_id']}", second=second)


    def recive_request_or_respond(self, second=False):
        req = self.comm.irecv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
        message = req.wait(status=self.status)
        message_tag = self.status.Get_tag()
        message_source = self.status.Get_source()
        #print(f"{self.rank}:: message.request_id:{message['request_id']}, source:{message_source}, tag:{ self.print_tag(message_tag)} ")

        channel_id = message.get("channel_id")
        #print(f"id mess:{message['request_id']} self.{ self.request_id}")
        if message_tag == MessageTag.REQ:
            self.handle_request(message=message, second=second)
        elif message['request_id'] == self.request_id - 1:

            #print(f"message_tag {message_tag}")
            #print(f"{self.rank}::  source:{message_source}, tag:{ self.print_tag(message_tag)} permission:{self.permissions} messsge:{message}")
            self.write_res(f"{self.rank}::  source:{message_source}, tag:{ self.print_tag(message_tag)} permission:{self.permissions} messsge:{message}")

            if message_tag == MessageTag.ACK_ALL:
                self.agree_on_all()
                self.got_res.add(message_source) # TODO
                self.last_res_tag[message_source] = MessageTag.ACK_ALL
                #print(f"{self.rank}:: message.request_id:{message['request_id']}, source:{message_source}, tag:{ self.print_tag(message_tag)} permission:{self.permissions}")

            elif message_tag == MessageTag.ACK_EXCEPT:
                self.agree_on_all_except_one(channel_id)
                self.got_res.add(message_source) # TODO
                self.last_res_tag[message_source] = MessageTag.ACK_EXCEPT


            elif message_tag == MessageTag.ACK:
                self.agree_on_one(channel_id)

            elif message_tag == MessageTag.BLOCK:
                self.block_channel(channel_id)
                self.got_res.add(message_source) # TODO
                self.last_res_tag[message_source] = MessageTag.BLOCK

            
            elif message_tag == MessageTag.UNBLOCK:
                self.unblock_channel(channel_id)

            elif message_tag == MessageTag.WAIT:
                self.wait_counter += 1

            elif message_tag == MessageTag.UNWAIT:
                self.wait_counter -= 1
        else:
            pass
            #print("invalid request_id")
        self.write_res(f"{self.rank}:: permission:{self.permissions} messsge:{message} answer:{self.answer_counter} wait:{self.wait_counter} req_id:{ message['request_id']}")
    
        

    def wait_for_responds(self):
        while True:
            # if self.data.P > 1:
            #     self.recive_request_or_respond()

            is_available_channel = self.find_channel()
            #print(f"{self.rank}find_channel: {self.channel}")
            if self.answer_counter == self.data.P - 1 and self.wait_counter == 0 and is_available_channel:
                break
            self.recive_request_or_respond()
    
    def next_state(self, new_state, flag_stopped=False):
        self.state = new_state
        #print(f"{self.rank}:: state:{self.state} ")
        if flag_stopped:
            self.stop_and_sleep()
        # TODO self.increment_lamport()
    
    def print_tag(self, tag):
        return next(name for name, value in vars(MessageTag).items() if value == tag)

    
    def run(self):
        while True:
            if self.state == State.LEFT_SIDE:
                self.next_state(new_state=State.GATHER_AGREEMENTS_LEFT_SIDE)
                #print(f"Statek {self.rank}: zacznam zbierac zgody po lewej stronie")
                self.write_log(f"Statek {self.rank}: zacznam zbierac zgody po lewej stronie")
                self.write_req(f"Statek {self.rank}: zacznam zbierac zgody po lewej stronie")
                self.write_res(f"Statek {self.rank}: zacznam zbierac zgody po lewej stronie")



                
                self.actions_before_entry_to_critical_section()
                self.send_requests()
                self.wait_for_responds()
                
                #otrzymal kanal i zaraz zajmie sekcje krytyczna
                self.actions_after_enter_to_critical_section()
                self.next_state(new_state=State.SAIL_TO_RIGHT_SIDE, flag_stopped=True)
                #wyjscie z sekcji
                self.actions_after_leaving_to_critical_section()
                self.next_state(new_state=State.RIGHT_SIDE, flag_stopped=True)

            
            
            ## DRUGA STRONA
            else:
                self.next_state(new_state=State.GATHER_AGREEMENTS_RIGHT_SIDE)
                #print(f"Statek {self.rank}: zacznam zbierac zgody po prawej stronie")
                self.write_log(f"Statek {self.rank}: zacznam zbierac zgody po prawej stronie")
                self.write_res(f"Statek {self.rank}: zacznam zbierac zgody po prawej stronie")
                self.write_req(f"Statek {self.rank}: zacznam zbierac zgody po prawej stronie")

                
                self.actions_before_entry_to_critical_section()
                self.send_requests()
                self.wait_for_responds()
                #otrzymal kanal i zaraz zajmie sekcje krytyczna
                self.actions_after_enter_to_critical_section()
                self.next_state(new_state=State.SAIL_TO_LEFT_SIDE, flag_stopped=True)
                #wyjscei z sekcji
                self.actions_after_leaving_to_critical_section()
                self.next_state(new_state=State.LEFT_SIDE, flag_stopped=True)


          