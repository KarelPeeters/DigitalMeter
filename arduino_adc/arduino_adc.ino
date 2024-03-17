/*
Arduino ADC
===========

Reads the analog voltage on pin PIN_ADC and outputs it as a digital serial stream.
The output format is as follows:
* Pulling PIN_IN_RESET_N low will read in a new value from the ADC.
* Pulling PIN_IN_NEXT_N low will output the next bit of the ADC value on PIN_OUT_DATA.
* The bits are output in little-endian order.
*/

const int PIN_ADC = A5;
const int PIN_IN_RESET_N = A4;
const int PIN_IN_NEXT_N = A3;
const int PIN_OUT_DATA = A2;

void setup() {
  Serial.begin(9600);
  pinMode(9, OUTPUT);

  pinMode(PIN_ADC, INPUT_PULLUP);
  pinMode(PIN_IN_RESET_N, INPUT_PULLUP);
  pinMode(PIN_IN_NEXT_N, INPUT_PULLUP);
  pinMode(PIN_OUT_DATA, OUTPUT);

  digitalWrite(PIN_OUT_DATA, false);
}

int adc_value = 0;
int next_bit = 0;

bool prev_stable_next_n = 1;

bool prev_in_next_n = 1;
int in_next_stable_count = 0;
const int DEBOUNCE_COUNT = 1024;

void loop() {
  // read ADC on reset
  if (!digitalRead(PIN_IN_RESET_N)) {
    adc_value = analogRead(PIN_ADC);
    next_bit = 0;

    Serial.print("ADC in: ");
    Serial.println(adc_value);
  }

  // debounce curr_in_next_n into curr_stable_next_n
  bool curr_in_next_n = digitalRead(PIN_IN_NEXT_N);
  bool curr_stable_next_n = prev_stable_next_n;

  if (curr_in_next_n == prev_in_next_n) {
    if (in_next_stable_count == DEBOUNCE_COUNT) {
      curr_stable_next_n = curr_in_next_n;
    } else {
      in_next_stable_count++;
    }
  } else {
    prev_in_next_n = curr_in_next_n;
    in_next_stable_count = 0;
  }

  // check for falling edge between stqble signals
  if (prev_stable_next_n && !curr_stable_next_n) {
    digitalWrite(PIN_OUT_DATA, adc_value & 1);

    Serial.print("Write output bit: ");
    Serial.println(adc_value & 1);

    adc_value >>= 1;
  }
  prev_stable_next_n = curr_stable_next_n;
}
