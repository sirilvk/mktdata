import argparse
import random
from datetime import datetime, date, time, timedelta
from multiprocessing import Process,Queue
import bisect


EXCH = ['NYSE', 'EDGX', 'ARCA']
TYPES = ['ASK', 'BID', 'TRADE']
NPROCS = 5 # 5 processes

def getType():
  return random.choice(TYPES)

def getExch():
  return random.choice(EXCH)

def getPx(mktPx):
  px = mktPx * 100
  return random.randint(int(px - px / 10), int(px + px / 10)) / 100

def getSz(sz):
  return random.randint(int(sz - sz / 5), int(sz + sz / 5))

def random_duration(st:time, dt:timedelta):
  duration = ((dt.seconds * 1000) + (dt.microseconds / 1000))
  return random.randint(0, duration)

def random_tm(st:time, dt:timedelta):
  random_offset = random_duration(st, dt)
  return (datetime.combine(date.min, st) + timedelta(seconds=(random_offset / 1000), milliseconds=(random_offset % 1000))).time()

def genMktData(sym:str, st:time, duration:timedelta, cnt:int):
  tmlst = []
  for i in range(cnt):
    bisect.insort(tmlst, random_tm(st, duration))
    with open('mkt/' + sym + '.txt', 'w') as f:
      print("#Timestamp,Price,Size,Exchange,Type", file=f)
      for i in tmlst:
        dt = datetime.combine(date.today(), i).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(dt + "," + str(getPx(45)) + "," + str(getSz(100)) + "," + getExch() + "," + getType(), file=f)


def process(queue, st:time, duration:timedelta, cnt:int):
  while sym := queue.get():
    genMktData(sym, st, duration, cnt)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(prog="mktgen", description="generates mkt data with random timestamps")
  parser.add_argument('start', help='start time', type=time.fromisoformat)
  parser.add_argument('end', help='end time', type=time.fromisoformat)
  parser.add_argument('-sf', help='symbol file', type=str)
  parser.add_argument('-count', help='number of entries (default=100)', type=int, default=100)
  args = parser.parse_args()
  duration = (datetime.combine(date.min, args.end) - datetime.combine(date.min, args.start))
  queue = Queue()
  procs = [Process(target=process, args=(queue, args.start, duration, args.count, )) for _ in range(NPROCS)]
  for p in procs:
    p.start()
  with open(args.sf, 'r') as sf:
    for sym in sf:
      queue.put(sym.strip())
  for _ in range(NPROCS):
    queue.put(None)
  for p in procs:
    p.join()
