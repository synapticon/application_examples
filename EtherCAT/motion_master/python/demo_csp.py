from motion_master_bindings.motion_master_bindings import MotionMasterBindings
from motion_master_proto.motion_master_pb2 import MotionMasterMessage
import logging
import argparse
import time
import threading
from rx import operators as ops
from datetime import datetime

master_period = 1000 # microseconds

def clean_string(message):
    """Strips out the newlines. Helper method to make a cleaner log."""
    return str(message).replace('\n', '')

def string_to_status(data):
    """Builds a Status message and returns it"""
    message = MotionMasterMessage()
    try:
        message.ParseFromString(data)
    except Exception:
        logging.error("Unable to parse data to a message: %s", data)
    return message

# if the data is mapped in PDO, the read/write would be through PDO, else SDO
def add_parameter_to_read(message, index, subindex): 
    parameter = message.request.get_device_parameter_values.parameters.add()
    parameter.index = index
    parameter.subindex = subindex

def add_parameter_to_read_topic(message, index, subindex): 
    parameter = message.request.start_monitoring_device_parameter_values.get_device_parameter_values.parameters.add()
    parameter.index = index
    parameter.subindex = subindex

def add_parameter_to_write(message, index, subindex):
    parameter_value = message.request.set_device_parameter_values.parameter_values.add()
    parameter_value.index = index
    parameter_value.subindex = subindex

slave_1_data_read = {'slave_address': 0, 'slave_statusword': 0, 'slave_402_state': 0, 'actual_position': 0, 'timestamp': 0}
slave_1_data_write = {'operation_mode': 0, 'target_position': 0}
master_data = {'master_time': 0, 'slave_1_timestamp_0': 0, 'master_error_code': 0}

dict_cia402_state = {0: 'UNSPECIFIED', 1: 'NOT_READY', 2: 'SWITCH_ON_DISABLED', 3: 'READY_SWITCH_ON', 4: 'SWITCHED_ON', 5: 'OP_ENABLED', 6: 'QUICK_STOP_ACTIVE', 7: 'FAULT_REACTION_ACTIVE', 8: 'FAULT'}
master_error_description = {
    1: 'Master / slave time difference larger than 5 ms',
    2: 'Slave not enabled in configrued time'
}

msg_t = MotionMasterMessage()
msg_r = MotionMasterMessage()
msg_t_write_parameters = MotionMasterMessage()

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

    # Initialize motion master binding
    mmb = MotionMasterBindings(args.address if args.address!=None else '127.0.0.1')
    mmb.subscribe_to_topic('pdo') # subscribe to the topic which will be used to monitor required pdo data
    mmb.connect()

    # Initialize sockets
    obs_topic = mmb.get_topics_subject().pipe(
            ops.map(lambda data: string_to_status(data[1])),
            ops.take(1)
    )
    obs_dealer = mmb.get_dealer_subject().pipe(
            ops.map(lambda data: string_to_status(data[0])),
            ops.take(1)
    )

    # Attach the main loop thread to the topic timer
    dispose_topic = mmb.get_topics_subject().subscribe(
            lambda data: main_loop(mmb, obs_topic, obs_dealer, data)
    )
    '''dispose_dealer = mmb.get_dealer_subject().subscribe(
            lambda data: logging.info("Dealer: %s", clean_string(string_to_status(data[0])))
    )'''
    '''dispose_health = mmb.get_healthstatus_subject().subscribe(
            lambda health_status: logging.info("Health status [%s] from %s: %s", 
                health_status.code, health_status.report_from, health_status.message)
    )'''
    
    # Read slave information and get slave address
    # Here we assume that there's only one slave
    msg_t.request.get_device_info.SetInParent()
    mmb.send_message(msg_t)
    msg_r = obs_dealer.run()
    slave_1_data_read['slave_address'] = msg_r.status.device_info.devices[0].device_address

    # Example for reading some parameters once, not used here. Here we would use StartMonitoringDeviceParameterValues instead to monitor a set of parameters of a device perodically
    '''
    msg_t.request.get_device_parameter_values.device_address = slave_1_data_read['slave_address']
    add_parameter_to_read(msg_t, 0x6041, 0)
    add_parameter_to_read(msg_t, 0x6064, 0)
    add_parameter_to_read(msg_t, 0x20F0, 0)
    mmb.send_message(msg_t)
    msg_r = obs_dealer.run()
    slave_1_data_read['slave_statusword'] = msg_r.status.device_parameter_values.parameter_values[0]
    slave_1_data_read['actual_position'] = msg_r.status.device_parameter_values.parameter_values[1]
    print(slave_1_data_read)
    '''
    
    # Configure a topic for reading parameters perodically
    # 1. Configure the parameters to read
    add_parameter_to_read_topic(msg_t, 0x6041, 0)
    add_parameter_to_read_topic(msg_t, 0x6064, 0)
    add_parameter_to_read_topic(msg_t, 0x20F0, 0)
    # 2. Configure slave address
    msg_t.request.start_monitoring_device_parameter_values.get_device_parameter_values.device_address = slave_1_data_read['slave_address']
    # 3. Configure reading period
    msg_t.request.start_monitoring_device_parameter_values.interval = master_period #in microsecond
    # 4. Configure the topic to attach to
    msg_t.request.start_monitoring_device_parameter_values.topic = 'pdo'
    # 5. Send message
    mmb.send_message(msg_t)
    # Done

    # Configure a message for writing parameters. This message would be kept.
    add_parameter_to_write(msg_t_write_parameters, 0x6060, 0) # Modes of operation
    add_parameter_to_write(msg_t_write_parameters, 0x607A, 0) # Target position
    msg_t_write_parameters.request.set_device_parameter_values.device_address = slave_1_data_read['slave_address']
    
    # You can use get_multi_device_parameter_values / set_multi_device_parameter_values to read / write multiple parameters of multiple devices.
    # Here the example is for one slave so the above functions are not used.


# Update monitored data space
def update_monitored_data(data):
    msg = string_to_status(data[1])
    slave_1_data_read['slave_statusword'] = msg.status.monitoring_parameter_values.device_parameter_values.parameter_values[0].uint_value
    slave_1_data_read['actual_position'] = msg.status.monitoring_parameter_values.device_parameter_values.parameter_values[1].int_value
    slave_1_data_read['timestamp'] = msg.status.monitoring_parameter_values.device_parameter_values.parameter_values[2].uint_value
    #print(slave_1_data_read['timestamp'])
    #dt=datetime.now()
    #print(dt.strftime('%Y-%m-%d %H:%M:%S %f'))

# Main loop function
def main_loop(mmb, obs_topic, obs_dealer, data):
    print('start')
    # Update monitored data
    update_monitored_data(data)
    
    if master_data['master_time'] == 0:
    # Record the beggining time of the slave timestamp
        master_data['slave_1_timestamp_0'] = slave_1_data_read['timestamp']

    # Compare slave clock and master clock, raise fault if the difference is too large
    if abs(master_data['master_time'] -  slave_1_data_read['timestamp'] + master_data['slave_1_timestamp_0']) >= 5000:
        master_data['master_error_code'] = 1

    # Master clock
    master_data['master_time'] += master_period;    
    #print(master_data['master_time'])

    # Ping system to show that the service is alive
    msg_t.request.ping_system.SetInParent()
    mmb.send_message(msg_t)

    # Get slave CIA402 state
    # As usually the timing of Statusword and Controlword is not affecting performance, 
    # here we are going to read the extracted 402 state / control the state machine directly using existing functions (which is simpler),
    # (although statusword is also monitored)
    msg_t.request.get_device_cia402_state.device_address = slave_1_data_read['slave_address']
    mmb.send_message(msg_t)
    msg_r = obs_dealer.run()
    if msg_r.status.device_cia402_state.state != 0:
        slave_1_data_read['slave_402_state'] = msg_r.status.device_cia402_state.state
    

    if master_data['master_time'] < 10000:
        # Set operation mode to CSP
        slave_1_data_write['operation_mode'] = 8

    # In the first 15s of running, set the 402 state of the slave to [operation enabled]
    # If the commutation encoder is not an incremental encoder, the needed time can be less
    if (master_data['master_time'] < 15000000) and (slave_1_data_read['slave_402_state'] != 5):
        # Set target position to actual position to avoid sudden jump while operation is enabled
        slave_1_data_write['target_position'] = slave_1_data_read['actual_position']

        # Reset fault if slave being in fault state
        if slave_1_data_read['slave_402_state'] == 8:
            msg_t.request.reset_device_fault.device_address = slave_1_data_read['slave_address']
            mmb.send_message(msg_t)

        # NOT READY -> SWITCH ON DISABLED
        elif slave_1_data_read['slave_402_state'] == 1:
            msg_t.request.set_device_cia402_state.device_address = slave_1_data_read['slave_address']
            msg_t.request.set_device_cia402_state.state = 2
            mmb.send_message(msg_t)
        
        # SWITCH ON DISABLED -> READY_SWITCH_ON
        elif slave_1_data_read['slave_402_state'] == 2:
            msg_t.request.set_device_cia402_state.device_address = slave_1_data_read['slave_address']
            msg_t.request.set_device_cia402_state.state = 3
            mmb.send_message(msg_t)

        # READY_SWITCH_ON -> SWITCHED ON
        elif slave_1_data_read['slave_402_state'] == 3:
            msg_t.request.set_device_cia402_state.device_address = slave_1_data_read['slave_address']
            msg_t.request.set_device_cia402_state.state = 4
            mmb.send_message(msg_t)

        # SWITCHED ON -> OP_ENABLED
        elif slave_1_data_read['slave_402_state'] == 4:
            msg_t.request.set_device_cia402_state.device_address = slave_1_data_read['slave_address']
            msg_t.request.set_device_cia402_state.state = 5
            mmb.send_message(msg_t)

    # The slave should be in [OP_ENABLED] with CSP
    elif slave_1_data_read['slave_402_state'] == 5:
        # Give target position
        slave_1_data_write['target_position'] += 10
        print(slave_1_data_write['target_position'])
    # Else raise an error
    else:
        master_data['master_error_code'] = 2

    # Write data to slave
    msg_t_write_parameters.request.set_device_parameter_values.parameter_values[0].uint_value = slave_1_data_write['operation_mode']
    msg_t_write_parameters.request.set_device_parameter_values.parameter_values[1].int_value = slave_1_data_write['target_position']
    mmb.send_message(msg_t_write_parameters)

    # Disconnect from server if an error occurs and log error description
    if master_data['master_error_code'] != 0:
        logging.debug(master_error_description[master_data['master_error_code']])
        mmb.disconnect()
        logging.debug("Disconnect from server.")
    
    print('end')