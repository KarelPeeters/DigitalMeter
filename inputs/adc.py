import time

import gpiozero


class ArduinoADC:
    def __init__(self, bit_delay: float = 0.1):
        self.pin_reset_n = gpiozero.DigitalOutputDevice("GPIO16")
        self.pin_next_n = gpiozero.DigitalOutputDevice("GPIO20")
        self.pin_data = gpiozero.DigitalInputDevice("GPIO21")

        self.pin_reset_n.value = 1
        self.pin_reset_n.value = 1

        self.bit_delay = bit_delay

    def reset(self):
        self.pin_reset_n.value = 0
        time.sleep(self.bit_delay)
        self.pin_reset_n.value = 1
        time.sleep(self.bit_delay)

    def next(self):
        self.pin_next_n.value = 0
        time.sleep(self.bit_delay)
        self.pin_next_n.value = 1
        time.sleep(self.bit_delay)
        return self.pin_data.value

    def readout(self):
        self.reset()
        value = 0
        for i in range(10):
            value |= self.next() << i
        return value


def main():
    adc = ArduinoADC(bit_delay=0.1)
    while True:
        print(adc.readout())


if __name__ == '__main__':
    main()
