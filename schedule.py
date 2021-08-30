import subprocess
import sys
import time


def process(command):
    pro = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    pro.wait()
    if pro.returncode == 0:
        print("Monitoring Client folder")
    else:
        print("Run unsuccessfull")
        sys.exit(1)

try:
    while True:
        process("python3 /mnt/c/scripts_with_monitor/client_monitor.py")
        time.sleep(5)
except KeyboardInterrupt:
    print ('KeyboardInterrupt')
