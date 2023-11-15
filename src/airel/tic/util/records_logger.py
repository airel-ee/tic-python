import dataclasses
import datetime
import logging
import math
import pathlib
import signal
import sys
import threading
import multiprocessing
import time
from typing import Union

import airel.tic
import yaml
from airel.tic.libusb_interface import find_all, DevUsbAddress, LibusbInterface
from pydantic import BaseModel, PositiveFloat, ConfigDict, ValidationError

FIELDS = [
    "is_settling",
    "begin_time_ms",
    "end_time_ms",
    "pos_concentration_mean",
    "neg_concentration_mean",
    "pos_concentration_stddev",
    "neg_concentration_stddev",
    "a_cev_voltage_raw_mean",
    "a_cev_voltage_raw_stddev",
    "a_cev_voltage_mean",
    "a_cev_voltage_stddev",
    "a_cev_voltage_target_mean",
    "a_cev_voltage_target_stddev",
    "a_cev_voltage_control_mean",
    "a_cev_voltage_control_stddev",
    "a_flow_rate_raw_mean",
    "a_flow_rate_raw_stddev",
    "a_flow_rate_mean",
    "a_flow_rate_stddev",
    "a_flow_rate_target_mean",
    "a_flow_rate_target_stddev",
    "a_flow_rate_control_mean",
    "a_flow_rate_control_stddev",
    "a_flow_rate_tacho_mean",
    "a_flow_rate_tacho_stddev",
    "b_cev_voltage_raw_mean",
    "b_cev_voltage_raw_stddev",
    "b_cev_voltage_mean",
    "b_cev_voltage_stddev",
    "b_cev_voltage_target_mean",
    "b_cev_voltage_target_stddev",
    "b_cev_voltage_control_mean",
    "b_cev_voltage_control_stddev",
    "b_flow_rate_raw_mean",
    "b_flow_rate_raw_stddev",
    "b_flow_rate_mean",
    "b_flow_rate_stddev",
    "b_flow_rate_target_mean",
    "b_flow_rate_target_stddev",
    "b_flow_rate_control_mean",
    "b_flow_rate_tacho_mean",
    "b_flow_rate_tacho_stddev",
    "b_flow_rate_control_stddev",
    "temperature_mean",
    "temperature_stddev",
    "humidity_mean",
    "humidity_stddev",
    "pressure_mean",
    "pressure_stddev",
    "env_sensor_sample_counter",
    "env_sensor_error_counter",
    "a_cev_adc_sample_counter",
    "a_cev_voltage_correction_counter",
    "b_cev_adc_sample_counter",
    "b_cev_voltage_correction_counter",
    "a_electrometer_sample_counter",
    "a_electrometer_reset_counter",
    "a_electrometer_error_counter",
    "b_electrometer_sample_counter",
    "b_electrometer_reset_counter",
    "b_electrometer_error_counter",
    "a_electrometer_current_mean",
    "a_electrometer_current_stddev",
    "a_electrometer_current_raw_mean",
    "a_electrometer_voltage",
    "b_electrometer_current_mean",
    "b_electrometer_current_raw_mean",
    "b_electrometer_current_stddev",
    "b_electrometer_voltage",
    "a_flow_sensor_error_counter",
    "a_flow_sensor_sample_counter",
    "b_flow_sensor_error_counter",
    "b_flow_sensor_sample_counter",
    "a_concentration_mean",
    "b_concentration_mean",
]

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


class Config(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    averaging_period: PositiveFloat = 10
    settling_time: PositiveFloat = 30
    measurement_cycle: list = [("zero", 60), ("run", 120)]
    cycle_shift: float
    local_tz: datetime.tzinfo = datetime.timezone.utc
    allow_power_from_usb_data: bool = True
    blowers_enabled_during_zero: bool = True
    custom_settings: dict = {}


def run(connection: Union[str, None], config: dict):
    setup_logging()

    try:
        config = Config(**config)
    except ValidationError as e:
        raise airel.tic.TicError(f"Invalid configuration: {str(e)}") from None

    stop_event = threading.Event()

    def set_stop_event(sig, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, set_stop_event)
    signal.signal(signal.SIGTERM, set_stop_event)

    logging.info("Starting measurements")
    logging.info(f"Using configuration: {config}")

    while not stop_event.is_set():
        try:
            device = airel.tic.Tic(connection)
            collect_data(device, stop_event, config=config)
            device.close()
        except airel.tic.TicError as e:
            logging.error(f"TIC error: {type(e).__name__} {e}")
            stop_event.wait(1.0)
        except KeyboardInterrupt:
            return

    logging.info("Measurements stopped")


@dataclasses.dataclass
class TicProcess:
    dev_address: DevUsbAddress
    process: multiprocessing.Process


def run_many(config):
    setup_logging()

    stop_event = multiprocessing.Event()

    stop_request = False

    def set_stop_event(sig, frame):
        nonlocal stop_request
        stop_request = True

    signal.signal(signal.SIGINT, set_stop_event)
    signal.signal(signal.SIGTERM, set_stop_event)

    exclude = set()
    processes = []

    logging.info("Starting logger manager")

    while not stop_request:
        dev_address_list = find_all(exclude_bus_address=exclude)
        for d in dev_address_list:
            logging.info(f"Found new device: {d.serial_number}")
            process = multiprocessing.Process(target=run_multiprocessing, args=(d, config, stop_event))
            process.daemon = True
            process.start()
            processes.append(TicProcess(dev_address=d, process=process))
            exclude.add((d.bus, d.address))

        dead_processes = [(i, p) for i, p in enumerate(processes) if not p.process.is_alive()]

        for i, p in dead_processes[::-1]:
            del processes[i]
            exclude.remove((p.dev_address.bus, p.dev_address.address))
            logging.info(f"Device {p.dev_address.serial_number} process died")

        time.sleep(1.0)

    stop_event.set()

    for p in processes:
        logging.info(f"Stopping {p.dev_address.serial_number}")
        p.process.join()

    pass


def run_multiprocessing(dev_address: DevUsbAddress, config: dict, stop_event: multiprocessing.Event):
    logger = logging.getLogger(dev_address.serial_number)

    try:
        config = Config(**config)
    except ValidationError as e:
        logger.error(f"Invalid configuration: {str(e)}")
        return

    logger.info("Starting logger")
    logger.info(f"Using configuration: {config}")

    while not stop_event.is_set():
        try:
            interface = LibusbInterface(
                serial_number=dev_address.serial_number, bus_address=(dev_address.bus, dev_address.address)
            )
            device = airel.tic.Tic(interface=interface)
            collect_data(device, stop_event, config=config)
            device.close()
        except airel.tic.TicError as e:
            logging.error(f"TIC error: {type(e).__name__} {e}")
            return

    logger.info("Logger stopped")


def collect_data(device: airel.tic.Tic, stop_event: threading.Event, config: Config):
    system_info = device.get_system_info()
    serial_number = system_info["serial_number"]

    logger = logging.getLogger(serial_number)
    logger.info(f"Connected to {serial_number}")
    logger.debug(f"System info: {system_info}")
    logger.debug(f"Debug info: {device.get_debug_info()}")

    settings = {
        "auto_zero_enabled": False,
        "averaging_period": config.averaging_period,
        "run_at_start": True,
        "extended_record_fields_enabled": True,
        "non_run_records_hidden": False,
        "allow_power_from_usb_data": config.allow_power_from_usb_data,
        "blowers_enabled_during_zero": config.blowers_enabled_during_zero,
        "zero_settling_duration": config.settling_time,
        "run_settling_duration": config.settling_time,
    }

    settings.update(config.custom_settings)

    device.reset_settings(settings)
    logger.info(f"Settings: {device.get_settings()}")

    flag_map = device.get_flag_descriptions()

    records_file = TimedFile(f"./{serial_number}/" + "{t:%Y%m%d}-block.records")
    raw_em_file = TimedFile(f"./{serial_number}/" + "{t:%Y%m%d}.rawem")

    cycle = MeasurementCycle(cycle_def=config.measurement_cycle, shift=config.cycle_shift)

    counter_values = {f: 0 for f in MONITORED_COUNTERS}

    while not stop_event.is_set():
        now = datetime.datetime.utcnow().replace(tzinfo=UTC).astimezone(config.local_tz)
        ts = now.timestamp()
        mode = cycle.get_mode(ts)
        if mode is not None:
            logger.info(
                f"{now:%H:%M:%S.%f} set opmode {mode} until {datetime.datetime.fromtimestamp(cycle.next_change)}"
            )
            if isinstance(mode, dict):
                device.set_custom_mode(mode)
            else:
                device.set_mode(mode)

        msg = device.receive_message(timeout=min(cycle.next_change - ts, 1.0))
        if msg is None:
            continue

        event_type = msg.get("event", None)

        if event_type == "record":
            r = msg["params"]

            # Ignore record in case the setting to include extended record fields has not yet kicked in
            if "a_electrometer_current_mean" not in r:
                continue

            for f in FIELDS:
                if r.get(f, None) is None:
                    r[f] = math.nan

            r["is_settling"] = 1 if r["is_settling"] else 0

            now = datetime.datetime.utcnow().replace(tzinfo=UTC).astimezone(config.local_tz)

            out_file, is_new_file = records_file.get(now)
            if is_new_file:
                write_records_file_header(out_file)

            begin_time = now - datetime.timedelta(milliseconds=r["end_time_ms"] - r["begin_time_ms"])
            cols = (
                [
                    str(begin_time),
                    str(now),
                    r["opmode"],
                    r["a_electrometer_current_mean"],
                    r["b_electrometer_current_mean"],
                    r["a_electrometer_current_stddev"],
                    r["b_electrometer_current_stddev"],
                    r["a_electrometer_current_raw_mean"],
                    r["b_electrometer_current_raw_mean"],
                    r["a_electrometer_voltage"],
                    r["b_electrometer_voltage"],
                ]
                + [r[f] for f in FIELDS]
                + [""]
            )
            out_file.write("\t".join(format_field(x) for x in cols))
            out_file.write("\n")
            out_file.flush()

            # fmt: off
            logger.info(
                f"{r['opmode'] + ('*' if r['is_settling'] else ' '):13}"
                f" pos_conc: {r['pos_concentration_mean']:10.3f} neg_conc: {r['neg_concentration_mean']:10.3f} "
                f" a: {r['a_electrometer_current_mean']:+9.2f} {r['a_electrometer_current_raw_mean'] - r['a_electrometer_current_mean']:+6.2f}"
                f" b: {r['b_electrometer_current_mean']:+9.2f} {r['b_electrometer_current_raw_mean'] - r['b_electrometer_current_mean']:+6.2f}"
                # f" acev: {r['a_cev_voltage_mean']:+6.2f} {r['a_cev_voltage_raw_mean']:+6.2f} {r['a_cev_voltage_control_mean']:+6.2f}",
                # f" flags:{[flag_map[f] for f in r['flags']]}"
            )
            # fmt: on

            for f in MONITORED_COUNTERS:
                if r[f] != counter_values[f]:
                    logger.info(f"  {f}: {counter_values[f]} -> {r[f]}")
                    counter_values[f] = r[f]

        elif event_type == "raw_em_record":
            now = datetime.datetime.utcnow().replace(tzinfo=UTC)
            params = msg.get("params", None)
            if params:
                ch = params.get("channel", None)
                t = params.get("time", None)
                data = params.get("data", None)
                if isinstance(data, dict):
                    value = data.get("value", None)
                else:
                    value = None
                if ch is not None and value is not None:
                    out_file, is_new_file = raw_em_file.get(now)
                    if is_new_file:
                        out_file.write("timestamp,mcutime,channel,value\n")
                    out_file.write(f"{now.timestamp()},{t},{ch},{value}\n")

        else:
            logger.debug(f"Other message: {msg}")


def write_records_file_header(outfile):
    params = []

    for f in FIELDS:
        params.append({"humanname": f, "name": f, "unit": ""})

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

    colfields = (
        [
            "begin_time",
            "end_time",
            "opmode",
            "cur_0",
            "cur_1",
            "curvar_0",
            "curvar_1",
            "rawcur_0",
            "rawcur_1",
            "volt_0",
            "volt_1",
        ]
        + FIELDS
        + ["flags"]
    )

    outfile.write("\t".join(colfields) + "\n")


def format_field(x):
    if x is None:
        return ""
    else:
        return str(x)


def setup_logging(connection=None):
    logger = logging.getLogger()

    hdlr = logging.StreamHandler(sys.stdout)
    hdlr.setFormatter(logging.Formatter(f"%(asctime)s %(name)10s %(levelname)s: %(message)s", datefmt="%H:%M:%S"))
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
