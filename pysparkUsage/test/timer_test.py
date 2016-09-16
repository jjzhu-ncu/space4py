import time
import sched
import datetime
schedule = sched.scheduler(time.time, time.sleep)
schedule2 = sched.scheduler(time.time, time.sleep)


def perform_command(cmd, inc):
    schedule.enter(inc, 0, perform_command, (cmd, inc))
    print(datetime.datetime.now())


def timing_exe(cmd, inc):
    schedule.enter(inc, 0, timing_exe, (cmd, inc))
    print(datetime.datetime.now().__format__('%Y/%m/%d %H:%M:%S'))
now = datetime.datetime.now()
late = datetime.datetime(now.year, now.month, now.day, 11, 13, 0)
if late > now:
    time.sleep((late-now).seconds)

schedule.run()
