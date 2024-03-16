import time

import gpiozero

pin_reset_n = gpiozero.DigitalOutputDevice("GPIO16")
pin_next_n = gpiozero.DigitalOutputDevice("GPIO20")
pin_data = gpiozero.DigitalInputDevice("GPIO21")

pin_reset_n.value = 1
pin_reset_n.value = 1


def reset():
    pin_reset_n.value = 0
    time.sleep(0.01)
    pin_reset_n.value = 1
    time.sleep(0.01)


def next():
    pin_next_n.value = 0
    time.sleep(0.01)
    pin_next_n.value = 1
    time.sleep(0.01)


def readout():
    reset()
    value = 0
    for i in range(10):
        next()
        if pin_data.value:
            value |= 1 << i
    return value


while True:
    print(readout())
    time.sleep(1)
