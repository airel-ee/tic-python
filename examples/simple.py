import airel.tic

print("Connecting")
device = airel.tic.Tic()

print("Updating settings")
device.reset_settings({
    "auto_zero_interval": 300.0,
    "auto_zero_duration": 60.0,
    "auto_zero_enabled": True,
    "extended_record_fields_enabled": True,
    "non_run_records_hidden": False,
    "averaging_period": 30.0,
})

print("Acive settings:", device.get_settings())

print("Start measurements")
device.set_mode("run")

while True:
    msg = device.receive_message(1.0)
    if msg is not None:
        print(msg)
