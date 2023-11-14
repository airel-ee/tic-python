import pytz

import airel.tic.util.records_logger as records_logger


def main():
    config = {
        "averaging_period": 10,
        "settling_time": 30,
        "measurement_cycle": [("zero", 60), ("run", 120)],
        "cycle_shift": 0,
        "local_tz": pytz.timezone("Europe/Tallinn"),
    }

    records_logger.run_many(config=config)


if __name__ == "__main__":
    main()
