"""
A simple demo for running device in CSP mode.

"""
__author__ = "Synapticon GmbH"
__email__ = "support@synapticon.com"

import logging
import argparse
import time
import json
import sys
import os
# Add the lib directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'lib'))
import threading

# Note: the wrapper is based on the binding and the proto
# To install from PyPi, run: $ pip install motion-master-bindings
from motion_master_wrapper import MotionMasterWrapper
import somanet_od as sod
import somanet_cia402_state_control as sst

def device_list(mmw):
    """Provide the list of devices"""

    _device_list = list(mmw.device_and_parameter_info_dict.values())
    assert _device_list, "Device list is empty"

    for device in _device_list:
        device_address = device['info'].device_address
        device['object_dictionary'] = sod.ObjectDictionary(mmw, device_address)
        device['state_control'] = sst.StateControl(mmw, device_address)

        # Get the hardware description data from each node too.
        try: 
            hardware_description_data = mmw.get_device_file(device_address, '.hardware_description')
            hardware_description = json.loads(hardware_description_data)
            device['hardware_description'] = hardware_description
        except Exception as e:
            logging.warning("Error retrieving .hardware_description: {}".format(e))
            # If this fails, just ignore it and make the data empty.
            device['hardware_description'] = {}

    return _device_list

"""
Define master frequency
Based on the system behavior, the program may not fit application with requirement in real-time performance
"""
master_frequency = 50 # Hz
master_period = 1.0 / master_frequency # second

"""
Some command data that we would like to keep
"""
slave_1_data_write = {'target_position': 0}

def do_every(period, function, *args):
    """Execute function periodically"""
    def _g():
        t = time.time()
        count = 0
        while True:
            t += period
            yield max(t - time.time(),0)
    g = _g()
    while True:
        try:
            time.sleep(next(g))
            function(*args)
        except KeyboardInterrupt:
            # Quick stop and disconnect before quitting
            logging.info("Quitting by Ctrl+C")
            sc.quick_stop()
            mmw.disconnect()
            break
        

def main_loop(od, sc):
    """Main loop function"""
    #print(time.time())
    # Here we enable the drive with deeper codes, to avoid a sudden jump after finding index in case an incremental encoder is used for commutation
    if (sc.current_state != 'state_operation_enabled'):
        sc._update_statusword()
        sc._state_machine(sc.STATE_COMMANDS.ENABLE_OPERATION) # Send Controlword
        slave_1_data_write['target_position'] = od.position_actual_value() # Set the target position as actual position before the drive is enabled
    else:
        # Target command while drive is enabled
        slave_1_data_write['target_position'] += 10 # Here we are doing a simple ramp
    # Send the target position
    od.target_position(slave_1_data_write['target_position'])

if __name__ == '__main__':
    # Configure the logger
    logging.basicConfig(format="[%(levelname).4s] %(message)s",
                        level=logging.DEBUG,
                        handlers=[logging.StreamHandler()])
    parser = argparse.ArgumentParser(description='Motion Master API Bindings')
    parser.add_argument('--address',
        type=str,
        help='The address (IP or name) of the Motion Master')
    args = parser.parse_args()

    # Initialize motion master connection
    mmw = MotionMasterWrapper(args.address if args.address!=None else '127.0.0.1', 0.01, 1) # address, timeout, logging on/off
    try:
        mmw.connect_to_motion_master()
        # Gather device and parameter info for address and type checking.
        mmw.initialize_device_parameter_info_dict()
    except Exception as e:
        mmw.disconnect()
        raise e
    
    # Get device list
    device_list = device_list(mmw)
    
    # Here we take only the first device as an example
    device = device_list[0]
    # Get the object dictionary object (for data access), and the state control object (for CiA402 state machine control)
    od = device['object_dictionary']
    sc = device['state_control']
    # Set operation mode to CSP (Cyclic synchronous position)
    sc.set_op_mode(sc.OP_MODES.CSP)

    # Send target position as actual position
    slave_1_data_write['target_position'] = od.position_actual_value()
    od.target_position(slave_1_data_write['target_position'])
    # sc.enable_operation() can be used to directly set CiA402 state to operation_enabled
    # Here we assume using an incremental encoder for commutation, thus the state control would be done with deeper codes in the main_loop function, to avoid sudden jump after finding index

    # If the device is in fault state, reset fault first
    if sc.current_state == 'state_fault':
        sc.fault_reset()

    # Execute main_loop function with the desired frequency
    do_every(master_period, main_loop, od, sc)

    # Alternative way for executing the main loop function periodically
    '''
    while(1):
        t = time.time()
        try:
            main_loop(od, sc)
            t += master_period
            time.sleep(max(t - time.time(),0))

        except KeyboardInterrupt:
            logging.info("Quitting by Ctrl+C")
            sc.quick_stop()
            mmw.disconnect()
            break
    '''
    
