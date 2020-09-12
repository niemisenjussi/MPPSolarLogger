from mppsolar.mpputils import *
import RPi.GPIO as GPIO
import psycopg2
import json
import time
import datetime
import os
import setproctitle

DBPASS = "inserter"
DBUSER = "inserter"

PACKET_OFFSET = 6

# Control SSR pack, 3 resistors VLM300
HEATER_PIN_1 = 17
HEATER_PIN_2 = 27
HEATER_PIN_3 = 22

# Control termostat on VLM300
servoPIN = 19
controlPIN = 26

def connect_db():
    host = "host='192.168.1.68'"
    db = "dbname='datacollection'"
    identity = "user='"+DBUSER+"' password='"+DBPASS+"'"
    conn = psycopg2.connect(host+" "+db+" "+identity)
    return conn


def disconnect_db(conn):
    conn.close()


def insert(cur, values, plan):
    cur.execute("execute "+plan+" (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", values)


def look_commands(filename, autodelete=True):
    cmdlist = []
    try:
        fh = open(filename, 'r')
        temp = fh.read()
        fh.close()
        cmdlist = temp.split('\n')
        if autodelete is True:
            print(f"Removing file {filename}")
            os.remove(filename)
        return cmdlist
    except FileNotFoundError:
        # print("Cannot find any new commands to execute")
        return []
    else:
        print("Other command read error")
        return []
        
        
def run_commands(cmdlist):
    print(f"Running {len(cmdlist)} commands")
    for cmd in cmdlist:
        if len(cmd) > 2:
            print(f"Exec command:{cmd}")
            res = inverter.getResponseDict(cmd)
            if isinstance(res, dict):
                print(json.dumps(res, indent=4))
                for rcmd, reslist in res.items():
                    if rcmd in cmd:
                        if "ACK" in reslist:
                            print("Command successfully executed")
                        else:
                            print("Command failed")
            else:
                print("Command failed")                    
                print(json.dumps(res, indent=4))

    print("All commands executed")


def get_inverter_mode():
    try:
        res = inverter.getResponseDict("QMOD")
        #print(json.dumps(res, indent=4))
        mode = res['device_mode'][0]
        return mode
    except:
        return "Unknown"


def activate_heater():
    print("Activate heater")
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(HEATER_PIN_1, GPIO.OUT)
    GPIO.setup(HEATER_PIN_2, GPIO.OUT)
    GPIO.setup(HEATER_PIN_3, GPIO.OUT)
    GPIO.output(HEATER_PIN_1, 1) # Activate only first resistor
    GPIO.output(HEATER_PIN_2, 0) # Deactivated
    GPIO.output(HEATER_PIN_3, 0) # Deactivated


def deactivate_heater():
    print("Deactivate heater")
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(HEATER_PIN_1, GPIO.OUT)
    GPIO.setup(HEATER_PIN_2, GPIO.OUT)
    GPIO.setup(HEATER_PIN_3, GPIO.OUT)
    GPIO.output(HEATER_PIN_1, 0) # Deactivated
    GPIO.output(HEATER_PIN_2, 0) # Deactivated
    GPIO.output(HEATER_PIN_3, 0) # Deactivated


def get_heater_active():
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(HEATER_PIN_1, GPIO.OUT)
    #GPIO.setup(HEATER_PIN_2, GPIO.OUT)
    #GPIO.setup(HEATER_PIN_3, GPIO.OUT)
    state1 = GPIO.input(HEATER_PIN_1)
    #state2 = GPIO.input(HEATER_PIN_2)
    #state3 = GPIO.input(HEATER_PIN_3)
    # print(f"Heater1: {state1} Heater2: {state2} Heater3: {state3} ")
    return {False: False, True: True, 0: False, 1: True}.get(state1, False)


def set_temp(temp):
    print(f"Setting termostat temp to:{temp} degrees")
    if temp < 30:
        temp = 30
    elif temp > 70:
        temp = 70

    GPIO.setmode(GPIO.BCM)
    GPIO.setup(servoPIN, GPIO.OUT)

    GPIO.setup(controlPIN, GPIO.OUT)
    GPIO.output(controlPIN, 1) #Power up servo
    time.sleep(0.5) #Wait 500ms
    p = GPIO.PWM(servoPIN, 50) # GPIO for PWM with 50Hz
    time.sleep(0.5)
    GPIO.output(controlPIN, 0)
    p.start(2.5) # Initialization
    GPIO.output(controlPIN, 1)
    
    amount = 10-(6.0/40.0*(float(temp-30.0)))
    print(f"Amount:{amount}")
    p.ChangeDutyCycle(amount)
    time.sleep(0.5)
    GPIO.output(controlPIN, 0)
    time.sleep(0.5)
    
    fh = open('current_temp.txt', 'w')
    fh.write(str(temp))
    fh.close()
    
    p.stop()
    GPIO.cleanup()
    print("Done")


    # PCP00 (set utility first)
    # PCP01 (set solar first)
    # PCP03 (set solar only charging)
    #res = inverter.getResponseDict("PCP03")
    #print(json.dumps(res, indent=4))
    
    #Set Device Output Source Priority
    # POP00 (set utility first)
    # POP01 (set solar first)
    # POP02 (set SBU priority)
    #res = inverter.getResponseDict("POP02")
    #print(json.dumps(res, indent=4))


def record_mode_change(conn, inverter_mode, old_mode, new_mode, battery, pv_power, heater_temp, heater_state, commands, action):
    cur = conn.cursor()
    values = (inverter_mode, battery, pv_power, old_mode, new_mode, heater_temp, {True:1, False:0}.get(heater_state, 0), commands, action)
    cur.execute("""INSERT INTO "SOLAR_ACTIONS" values(%s,%s,%s,%s,%s,%s,%s,%s,%s,current_timestamp)""", values)
    conn.commit()


def update_mode(global_mode, inverter_mode, night_shift, battery_volts, pv_power, current_hour, conn):
    # Function which takes care of all heating, night shift etc logic
    heater_state = get_heater_active()
    heated_hours = get_heated_hours(conn, 0)
    print(f"global_mode:{global_mode}, mode:{inverter_mode}, NS:{night_shift}, battery:{battery_volts}V, pv_power:{pv_power}W Heater:{heater_state} Heated:{heated_hours}h")
    
    
    rules = [
        {'hours': (22, 23, 24, 0, 1, 2, 3, 4, 5, 6), 'inverter_mode': ['Line', 'Battery'], 'bat_min': 0.0, 'bat_max': 54.0, 
         'name': 'night_charge_heater', 'parent': ["solar_power", "disable_heater"],
         'desc':"Activating night charge + heater mode"},
        
        {'hours': (22, 23, 24, 0, 1, 2, 3, 4, 5, 6), 'inverter_mode': ['Line'], 'bat_min': 52.0, 'bat_max': 60.0,
         'name': 'night_heater', 'parent': ["night_charge_heater"],
         'desc': "Disabling utility charger"}, 
        
        {'hours': (7, ), 'inverter_mode': ['Line', 'Battery'], 'bat_min': 46.0, 'bat_max': 60.0,
         'name': 'solar_init', 'parent': ['night_charge_heater', 'night_heater', 'solar_power', 'disable_heater'],
         'desc': "Disabling night charger + heater => solar init mode"},
         
        {'hours': (10, 11, 12, 13), 'inverter_mode': ['Battery'], 'bat_min': 52.5, 'bat_max': 57.5,
         'name': 'solar_heater', 'parent': ['solar_init', 'solar_power'],
         'desc': "Activate Solar heater"},

        {'hours': (11, 12, 13, 14, 15, 16), 'inverter_mode': ['Battery'], 'bat_min': 57.5, 'bat_max': 60.0,
         'name': 'solar_heater_extra', 'parent':['solar_heater'],
         'desc': "Activate Extra solar heater"},

        {'hours': (13, 14, 15, 16, 17), 'inverter_mode': ['Battery'], 'bat_min': 40.0, 'bat_max': 51.5,
         'name': 'solar_power', 'parent': ['solar_heater', 'solar_heater_extra', 'solar_init'],
         'desc': "Deactivate Solar heater, solar charge only"},
        
        {'hours': (18, 19, 20, 21), 'inverter_mode': ['Battery'], 'bat_min': 45.0, 'bat_max': 60.0,
         'name': 'solar_power', 'parent':['solar_heater', 'solar_heater_extra', 'solar_init'],
         'desc': "Deactivate Solar heater, solar charge only after 18 always"},

        {'hours': (8,9,10,11,12,13,14,15,16,17,18,19,20,21), 'inverter_mode': ['Line'], 'bat_min': 40.0, 'bat_max': 60.0,
         'name': 'disable_heater', 'parent':[],
         'desc': "Deactivate Heater always when in Line mode and heater is on"}
        ]
    
    modes = {
        'night_charge_heater': {'temp': 45, 'heater': True, 'commands': ["PCP00", "POP00"]},
        'night_heater': {'temp': None, 'heater': True, 'commands': ["PCP03"]},
        'solar_init': {'temp': None, 'heater': False, 'commands': ["PCP03", "POP02"]},
        'solar_heater': {'temp': 55, 'heater': True, 'commands': ["PCP03", "POP02"]},
        'solar_heater_extra': {'temp': 65, 'heater': True, 'commands': ["PCP03", "POP02"]},
        'solar_power': {'temp': None, 'heater': False, 'commands': ["PCP03", "POP02"]},
        'disable_heater': {'temp': None, 'heater': False, 'commands': []}
    }
    

    next_mode = []  # Collect all modes which were active
    rule_desc = ""
    for rulenum, rule in enumerate(rules):
        status = False
        if current_hour in rule['hours'] and inverter_mode in rule['inverter_mode'] and battery_volts >= rule['bat_min'] and battery_volts <= rule['bat_max']:
            # Check if parent mode is current mode, else change is not allowed
            if len(rule['parent']) > 0:
                if global_mode in rule['parent']:
                    status = True

            else: # We dont have any rules for previous rule. It will be ok
                status = True

        if status is True:
            next_mode.append(rule['name'])
            rule_desc = f"Rulenum:{rulenum}::: {rule['desc']}"
        
        print(f"Rulenum:{rulenum} s:{status} Mode:{rule['name']} Desc:{rule['desc']}")

    # Change mdoe if only one new mode is found
    new_modes = len(next_mode)
    # At least one mode defined which is different than current one
    if new_modes > 0 and next_mode[0] != global_mode:
        print(f"We have {new_modes} defined")
        if new_modes == 1:
            print(f"Only one mode found, Activate it {next_mode[0]}")
            params = modes[next_mode[0]]
            heater_temp = 0
            if params['temp'] is not None:
                set_temp(params['temp'])
                heater_temp = params['temp']
            
            if params['heater'] is True:
                activate_heater()
            else:
                deactivate_heater()

            run_commands(params['commands'])

            new_mode = next_mode[0]
            record_mode_change(conn, inverter_mode, global_mode, new_mode, battery_volts, pv_power, heater_temp, params['heater'], params['commands'], rule_desc)

            global_mode = new_mode
        else:
            print("Multipel modes found, do not activate anything")
            print(json.dumps(next_mode, indent=4))




    # print(f"New global mode: {global_mode}")
    return global_mode


def get_heated_hours(con, deltaday):
    mins = 0
    query = f"""select
                    EXTRACT(EPOCH from (MAX("PERIOD")-MIN("PERIOD"))) / COUNT("HEATER_ON") * SUM("HEATER_ON")/60 as "mins"                    
                from 
                    "MPP_RAW" 
                where 
                    date_trunc('day', "PERIOD") = date_trunc('day',current_timestamp-interval '{deltaday} day')  """
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    for row in result:
        mins = row[0]
        break
    
    return mins



if __name__ == '__main__':
    print("Start")
    setproctitle.setproctitle('mppcollector')

    GPIO.setmode(GPIO.BCM)
    GPIO.setup(HEATER_PIN_1, GPIO.OUT)
    GPIO.setup(HEATER_PIN_2, GPIO.OUT)
    GPIO.setup(HEATER_PIN_3, GPIO.OUT)
    GPIO.output(HEATER_PIN_1, 0) # Deactivated
    GPIO.output(HEATER_PIN_2, 0) # Deactivated
    GPIO.output(HEATER_PIN_3, 0) # Deactivated
    
    inverter = mppUtils(serial_device="/dev/hidraw0")
    
    conn = connect_db()

    cur = conn.cursor()
    sqlquery = """ prepare insertplan as
                 INSERT INTO
                  "MPP_RAW"
                 VALUES
                  ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, current_timestamp, $16) """
    cur.execute(sqlquery)
    
    
    # IF clock between 7:00 - 22:00
    #    If line mode active => Deactivate heating
    
    # If battery voltage below 49V and clock 22:00 - 7:00:
    #   Activate Line mode and Heater
    #   Set Termostat to 50 degrees
    
    # If Battery mode and Voltage 

    print("Init to solar_power and 55 degrees without heater")
    night_shift = False
    global_mode = "solar_power"
    run_commands(["PCP03", "POP02"])
    deactivate_heater()
    set_temp(55)

    record_mode_change(conn,
                       get_inverter_mode(),
                       "start",
                       "solar_power",
                       0,
                       0,
                       55,
                       False,
                       "PCP03, POP02",
                       "Init")
    
    #print(now.year, now.month, now.day, now.hour, now.minute, now.second)
    last_update = 0
    avg_battery_volts = None
    while True:
        now = datetime.datetime.now()
        epoch = now.timestamp()

        inverter_mode = get_inverter_mode()
        print(f"Inverted mode:{inverter_mode} hour:{now.hour} min:{now.minute} global_mode:{global_mode}")

        #Night power time window
        if now.hour in (22, 23, 24, 0, 1, 2, 3, 4, 5, 6):
            print("It is night time")
            night_shift = True

        # Read inverter full status
        status = inverter.getFullStatus()

        # If query returns valid results and previous update was over 5 seconds ago
        if 'error' not in status and (epoch - last_update) > 5:
            values = (
                1, # inverter number
                status['ac_input_voltage']['value'],
                status['ac_input_frequency']['value'],
                status['ac_output_voltage']['value'],
                status['ac_output_frequency']['value'],
                status['ac_output_apparent_power']['value'],
                status['ac_output_active_power']['value'],
                status['bus_voltage']['value'],
                status['battery_voltage']['value'],
                status['battery_charging_current']['value'],
                status['inverter_heat_sink_temperature']['value'],
                status['pv_input_current_for_battery']['value'],
                status['pv_input_voltage']['value'],
                status['battery_discharge_current']['value'],
                status['pv_input_power']['value'],
                {True:1, False:0}.get(get_heater_active(), 0)
            )
            battery_volts = status['battery_voltage']['value']
            pv_power = status['pv_input_power']['value']
            print(f"inserting value to database, battery:{battery_volts}V PV: {pv_power}W")
            insert(cur, values, "insertplan")
            conn.commit()
            last_update = epoch

            if avg_battery_volts is None:
                avg_battery_volts = battery_volts
            else:
                # 25sec moving average
                avg_battery_volts = ((avg_battery_volts * 4) + battery_volts) / 5

            # Update inverter mode only if we have valid status data available
            global_mode = update_mode(global_mode, inverter_mode, night_shift, avg_battery_volts, pv_power, now.hour, conn)


        # print("Looking commands to be executed")
        commands = look_commands("commands.txt")
        if len(commands) > 0:
            try:
                run_commands(commands)
            except:
                print("Error during command execution")
        #print(json.dumps(status, indent=4))
        time.sleep(1)
        
