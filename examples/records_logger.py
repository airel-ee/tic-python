#!/usr/bin/env python3

# A more advanced data logging program for the TIC
#
# It stores data received from the TIC in records files that can be used directly with the Retrospect program
# Device internal automatic zero mode running is disabled. The measurement cycle is aligned to the computer clock.

import argparse
import datetime
import logging
import math
import pathlib
import signal
import sys
import threading

import airel.tic
import pytz
import yaml

# Averaging period of the TIC records
AVERAGING_PERIOD = 5

# Measurement cycle as a list of operating modes and durations in seconds
MEASUREMENT_CYCLE = [("zero", 60), ("run", 60), ("run_swapped", 60)]

# Shift the measurement cycle by -15 seconds which is the settling time after operating mode switch of the TIC, so that
# correct data records will start at full minutes
CYCLE_SHIFT = -15

# Set to your local time zone database name (https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)
LOCAL_TZ = pytz.timezone("Europe/Tallinn")

FIELDS = ["opmode", "is_settling", "begin_time_ms", "end_time_ms", "pos_concentration_mean", "neg_concentration_mean",
          "pos_concentration_stddev", "neg_concentration_stddev", "a_cev_voltage_raw_mean", "a_cev_voltage_raw_stddev",
          "a_cev_voltage_mean", "a_cev_voltage_stddev", "a_cev_voltage_target_mean", "a_cev_voltage_target_stddev",
          "a_cev_voltage_control_mean", "a_cev_voltage_control_stddev", "a_flow_rate_raw_mean",
          "a_flow_rate_raw_stddev", "a_flow_rate_mean", "a_flow_rate_stddev", "a_flow_rate_target_mean",
          "a_flow_rate_target_stddev", "a_flow_rate_control_mean", "a_flow_rate_control_stddev",
          "a_flow_rate_tacho_mean", "a_flow_rate_tacho_stddev", "b_cev_voltage_raw_mean", "b_cev_voltage_raw_stddev",
          "b_cev_voltage_mean", "b_cev_voltage_stddev", "b_cev_voltage_target_mean", "b_cev_voltage_target_stddev",
          "b_cev_voltage_control_mean", "b_cev_voltage_control_stddev", "b_flow_rate_raw_mean",
          "b_flow_rate_raw_stddev", "b_flow_rate_mean", "b_flow_rate_stddev", "b_flow_rate_target_mean",
          "b_flow_rate_target_stddev", "b_flow_rate_control_mean", "b_flow_rate_tacho_mean", "b_flow_rate_tacho_stddev",
          "b_flow_rate_control_stddev", "temperature_mean", "temperature_stddev", "humidity_mean", "humidity_stddev",
          "pressure_mean", "pressure_stddev", "env_sensor_sample_counter", "env_sensor_error_counter",
          "a_cev_adc_sample_counter", "a_cev_voltage_correction_counter", "b_cev_adc_sample_counter",
          "b_cev_voltage_correction_counter", "a_electrometer_sample_counter", "a_electrometer_reset_counter",
          "a_electrometer_error_counter", "b_electrometer_sample_counter", "b_electrometer_reset_counter",
          "b_electrometer_error_counter", "a_electrometer_current_mean", "a_electrometer_current_stddev",
          "a_electrometer_current_raw_mean", "a_electrometer_voltage", "b_electrometer_current_mean",
          "b_electrometer_current_raw_mean", "b_electrometer_current_stddev", "b_electrometer_voltage",
          "a_flow_sensor_error_counter", "a_flow_sensor_sample_counter", "b_flow_sensor_error_counter",
          "b_flow_sensor_sample_counter", "a_concentration_mean", "b_concentration_mean"]

MONITORED_COUNTERS = [
    "env_sensor_error_counter",
    "a_flow_sensor_error_counter",
    "b_flow_sensor_error_counter",
    "a_electrometer_reset_counter",
    "b_electrometer_reset_counter",
    "a_electrometer_error_counter",
    "b_electrometer_error_counter",
]

UTC = datetime.timezone.utc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--connection", help="Connection string", default="")
    args = ap.parse_args()

    stop_event = threading.Event()

    def set_stop_event(sig, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, set_stop_event)
    signal.signal(signal.SIGTERM, set_stop_event)

    setup_logging()

    logging.info("Starting measurements")

    while not stop_event.is_set():
        try:
            device = airel.tic.Tic(args.connection)

            collect_data(device, stop_event)

            device.close()
        except airel.tic.TicError as e:
            logging.error(f"TIC error: {type(e).__name__} {e}")
            stop_event.wait(1.0)
        except KeyboardInterrupt:
            return

    logging.info("Measurements stopped")


def collect_data(device, stop_event):
    system_info = device.get_system_info()
    serial_number = system_info["serial_number"]

    logging.info(f"Connected to {serial_number}")
    logging.debug(f"System info: {system_info}")
    logging.debug(f"Debug info: {device.get_debug_info()}")

    device.reset_settings({
        "auto_zero_enabled": False,
        "averaging_period": AVERAGING_PERIOD,
        "run_at_start": True,
        "extended_record_fields_enabled": True,
        "non_run_records_hidden": False,
        # "allow_power_from_usb_data": True
        # "zero_settling_duration": 1.5,
        # "run_settling_duration": 17.5,
    })
    logging.info(f"Settings: {device.get_settings()}")

    flag_map = device.get_flag_descriptions()

    records_file = TimedFile(f"./{serial_number}/" + "{t:%Y%m%d}-block.records")

    cycle = MeasurementCycle(cycle_def=MEASUREMENT_CYCLE, shift=CYCLE_SHIFT)

    counter_values = {f: 0 for f in MONITORED_COUNTERS}

    while not stop_event.is_set():
        now = datetime.datetime.utcnow().replace(tzinfo=UTC).astimezone(LOCAL_TZ)
        ts = now.timestamp()
        mode = cycle.get_mode(ts)
        if mode is not None:
            logging.debug(
                f"{now:%H:%M:%S.%f} set opmode {mode} until {datetime.datetime.fromtimestamp(cycle.next_change)}")
            device.set_mode(mode)

        msg = device.receive_message(timeout=min(cycle.next_change - ts, 1.0))
        if msg is None:
            continue

        if msg.get("event", None) == "record":
            r = msg["params"]

            # Ignore record in case the setting to include extended record fields has not yet kicked in
            if "a_electrometer_current_mean" not in r:
                continue

            for f in FIELDS:
                if r.get(f, None) is None:
                    r[f] = math.nan

            r["is_settling"] = 1 if r["is_settling"] else 0

            now = datetime.datetime.utcnow().replace(tzinfo=UTC).astimezone(LOCAL_TZ)

            out_file, is_new_file = records_file.get(now)
            if is_new_file:
                write_records_file_header(out_file)

            begin_time = now - datetime.timedelta(milliseconds=r["end_time_ms"] - r["begin_time_ms"])
            cols = [
                       str(begin_time), str(now), r["opmode"],
                       r["a_electrometer_current_mean"],
                       r["b_electrometer_current_mean"],
                       r["a_electrometer_current_stddev"],
                       r["b_electrometer_current_stddev"],
                       r["a_electrometer_current_raw_mean"],
                       r["b_electrometer_current_raw_mean"],
                       r["a_electrometer_voltage"],
                       r["b_electrometer_voltage"],
                   ] + [r[f] for f in FIELDS] + [""]
            out_file.write("\t".join(format_field(x) for x in cols))
            out_file.write("\n")
            out_file.flush()

            print(
                f"{begin_time:%H:%M:%S.%f} {r['begin_time_ms'] / 1000:9.1f} {(r['end_time_ms'] - r['begin_time_ms']) / 1000:9.1f}"
                f" {r['opmode']:12} {'settl' if r['is_settling'] else 'ok   '}"
                f" pos_conc: {r['pos_concentration_mean']:+11.3f}"
                f" neg_conc: {r['neg_concentration_mean']:+11.3f}"
                f" a: {r['a_electrometer_current_mean']:+11.3f} {r['a_electrometer_current_raw_mean']:+11.3f} {r['a_electrometer_current_raw_mean'] - r['a_electrometer_current_mean']:+11.3f}"
                f" b: {r['b_electrometer_current_mean']:+11.3f} {r['b_electrometer_current_raw_mean']:+11.3f} {r['b_electrometer_current_raw_mean'] - r['b_electrometer_current_mean']:+11.3f}"
                f" flags:{[flag_map[f] for f in r['flags']]}"
            )

            for f in MONITORED_COUNTERS:
                if r[f] != counter_values[f]:
                    print(f"  {f}: {counter_values[f]} -> {r[f]}")
                    counter_values[f] = r[f]

        else:
            print("Other message:", msg)


def write_records_file_header(outfile):
    params = []

    for f in FIELDS:
        params.append({
            "humanname": f,
            "name": f,
            "unit": "",
        })

    doc = {
        "dataproc variant": "block",
        "electrometer groups": {"a_el": [0, 0], "b_el": [1, 1]},
        "electrometer names": ["A", "B"],
        "file type": "records",
        "instrument configuration": {},
        "opmodes": ["run", "zero", "run_swapped", "unknown"],
        "software": "tic_to_records",
        "total electrometers": 2,
        "parameters": params,
    }

    yamldata = yaml.safe_dump(doc)
    yamlrows = "\n".join(f"# {line}" for line in yamldata.split("\n"))

    outfile.write("# Spectops records\n")
    outfile.write(yamlrows)
    outfile.write("\n")

    colfields = ["begin_time", "end_time", "opmode", "cur_0", "cur_1", "curvar_0", "curvar_1", "rawcur_0", "rawcur_1",
                 "volt_0", "volt_1"] + FIELDS + ["flags"]

    outfile.write("\t".join(colfields) + "\n")


def format_field(x):
    if x is None:
        return ""
    else:
        return str(x)


def setup_logging():
    logger = logging.getLogger()

    hdlr = logging.StreamHandler(sys.stderr)
    hdlr.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    hdlr.setLevel(logging.DEBUG)
    logger.addHandler(hdlr)

    logger.setLevel(logging.DEBUG)


class TimedFile:
    def __init__(self, name_template: str):
        self.name_template = name_template
        self.file = None
        self.file_name = None

    def get(self, t):
        file_name = self.name_template.format(t=t)
        if self.file_name != file_name:
            if self.file:
                self.file.close()

            pathlib.Path(file_name).parent.mkdir(exist_ok=True, parents=True)
            self.file_name = file_name
            self.file = open(file_name, "a")
            return self.file, True
        else:
            return self.file, False


class MeasurementCycle:
    def __init__(self, cycle_def, shift):
        self.cycle_def = cycle_def
        self.shift = shift
        self.total_duration = sum(x[1] for x in cycle_def)
        self.next_change = None

    def get_mode(self, timestamp):
        if self.next_change is None or timestamp > self.next_change:
            cycles_since_epoch, rel_t = divmod(timestamp - self.shift, self.total_duration)
            self.next_change = cycles_since_epoch * self.total_duration + self.shift
            for mode, duration in self.cycle_def:
                if rel_t <= duration:
                    self.next_change += duration
                    return mode
                else:
                    self.next_change += duration
                    rel_t -= duration


if __name__ == '__main__':
    main()
