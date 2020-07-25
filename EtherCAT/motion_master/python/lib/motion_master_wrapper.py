"""
Motion Master Module

"""
import time
from typing import Any, List, Tuple, Dict
import logging
import uuid
from collections import OrderedDict
import rx
from rx import operators as ops

from motion_master_bindings.motion_master_bindings import MotionMasterBindings
from motion_master_proto.motion_master_pb2 import MotionMasterMessage

logger = logging.getLogger(__name__)

FILE_OPERATION_TIMEOUT = 5  # seconds


class OperationFailed(Exception):
    """Exception for operations that just didn't work."""
    pass


def decode(data: str) -> MotionMasterMessage:
    """Return a MotionMasterMessage decoded from the network Protobuf data.

    Parameters
    ----------
    data : str
        The raw data from the Motion Master bindings, needed to be converted.
    """
    try:
        msg = MotionMasterMessage()
        msg.ParseFromString(data)
    except Exception as e:
        logger.error("Exception while parsing the message: %s", e)
        raise e
    return msg


def _check_device_parameter_value_status(parameter):
    """Checks the status field of the MotionMasterMessage to see if the request was successful.

    Raises
    -------
    OperationFailed
        If the status is an error.

    Returns
    -------
    bool
        True if the message status was successful.
    """
    status_name = parameter.WhichOneof('status')
    if status_name == 'success':
        return True
    elif status_name == 'error':
        status_code = MotionMasterMessage.Status.DeviceParameterValues.ParameterValue.Error.Code.Name(
            getattr(parameter, status_name).code)
        raise OperationFailed("Status code returned error {} for object 0x{:04x}:{}".format(
            status_code, parameter.index, parameter.subindex))


class MotionMasterWrapper:
    """
    Provide a way to hide some of the code required to send and receive messages.
    """

    def __init__(self, address: str, message_timeout: float, logging: bool = False) -> None:
        """Setup the Motion Master bindings and init some stuff"""

        self.timeout_s = message_timeout
        self._mmb = MotionMasterBindings(address)
        self.device_and_parameter_info_dict = {}
        self.collection = dict()
        logger.disabled = not logging

        logger.debug("Using address %s", address)
        logger.debug("Using message_timeout %s seconds", message_timeout)

    def connect_to_motion_master(self):
        """Connect to the Motion Master and enable keepalive checks"""
        self._mmb.connect()
        self._mmb.enable_keepalive()

    def send_to_motion_master(self, message: MotionMasterMessage):
        """Send the message to the Motion Master

        Parameters
        ----------
        message : MotionMasterMessage
            A fully formed message, ready for encoding and transport to the Motion Master.
        """
        self._mmb.send_message(message)

    def disconnect(self):
        """Disconnects from the Motion Master"""
        self._mmb.disconnect()

    def _get_request_and_single_response_observable(self, description: str = None, timeout: int = None) -> Tuple[
        MotionMasterMessage, rx.Observable]:
        """Set up a unique request and an observable that filters for a single response.

        The caller should continue building the MotionMasterMessage for
        whatever they want to send, send it to the Motion Master, and then
        immediately run() the returned observable to catch the response.
        """
        if timeout is None:
            timeout = self.timeout_s
        unique_id = str(uuid.uuid4())
        # Build an exception message that describes the message that failed to return.
        if description is None:
            message = "No response from Motion Master for message ID {}".format(unique_id)
        else:
            message = "No response from Motion Master for message '{}'".format(description)
        # Build an observable that returns messages with the same unique_id.
        obs = self._mmb.get_dealer_subject().pipe(
            ops.map(lambda data: decode(data[0])),
            ops.filter(lambda decoded_message: decoded_message.id == unique_id),
            ops.take(1),
            ops.timeout(timeout, other=rx.throw(Exception(message))),
        )
        # A ReplaySubject will cache the last object and re-send it on subscription.
        # This prevents the loss of a message before a subscription can be made.
        replay_subject = rx.subject.ReplaySubject(1)
        obs.subscribe(replay_subject)
        # The caller will continue building this message.
        msg = MotionMasterMessage()
        msg.id = unique_id
        return msg, replay_subject

    def get_device_file_list(self, device_address: int) -> List[str]:
        """Requests and returns the list of files in the device flash filesystem.

        These returned strings can be requested with `open_device_file()` and others.

        Parameters
        ----------
        device_address : int
            The address of the device as provided by Motion Master

        Returns
        -------
        list : List[str]
            A list of file names on the device.
        """
        msg, obs = self._get_request_and_single_response_observable("get file list", FILE_OPERATION_TIMEOUT)
        msg.request.get_device_file_list.device_address = device_address
        self.send_to_motion_master(msg)
        message = obs.run()

        response = message.status.device_file_list.WhichOneof('data')
        if response == 'error':
            status_code = MotionMasterMessage.Status.DeviceFileList.Error.Code.Name(
                getattr(message.status.device_file_list, response).code)
            raise OperationFailed("File list request returned error {}".format(status_code))

        file_list = []
        for file_string in message.status.device_file_list.file_list.files:
            file_list.append(file_string)

        return file_list

    def get_device_file(self, device_address: int, filename: str) -> Any:
        """Retrieve and open a file on the device for reading.

        Returns the file content.
        """
        msg, obs = self._get_request_and_single_response_observable("GetDeviceFile {}".format(filename),
                                                                    FILE_OPERATION_TIMEOUT)
        msg.request.get_device_file.device_address = device_address
        msg.request.get_device_file.name = filename
        self.send_to_motion_master(msg)
        message = obs.run()

        response = message.status.device_file.WhichOneof('data')
        if response == 'error':
            status_code = MotionMasterMessage.Status.DeviceFile.Error.Code.Name(
                getattr(message.status.device_file, response).code)
            raise OperationFailed("GetDeviceFile request returned error {} for file {}".format(
                status_code, message.status.device_file.name))

        # Success! Return the data in the file.
        return message.status.device_file.content

    def set_device_file(self, device_address: int, filename: str, overwrite: bool, content: Any):
        """Save a file on the device."""
        msg, obs = self._get_request_and_single_response_observable("SetDeviceFile {}".format(filename),
                                                                    FILE_OPERATION_TIMEOUT)
        msg.request.set_device_file.device_address = device_address
        msg.request.set_device_file.name = filename
        msg.request.set_device_file.overwrite = overwrite
        msg.request.set_device_file.content = content
        self.send_to_motion_master(msg)
        message = obs.run()

        response = message.status.device_file.WhichOneof('data')
        if response == 'error':
            code = MotionMasterMessage.Status.DeviceFile.Error.Code.Name(
                getattr(message.status.device_file, response).code)
            raise OperationFailed("GetDeviceFile request returned error '{}' ({}) for file {}".format(
                getattr(message.status.device_file, response).message, code, message.status.device_file.name))

        logger.debug("Wrote file %s with success message '%s'",
                     message.status.device_file.name, getattr(message.status.device_file, response).message)

    def delete_device_file(self, device_address: int, filename: str):
        """Delete a file on the device."""
        msg, obs = self._get_request_and_single_response_observable("DeleteDeviceFile {}".format(filename))
        msg.request.delete_device_file.device_address = device_address
        msg.request.delete_device_file.name = filename
        self.send_to_motion_master(msg)
        message = obs.run()

        response = message.status.device_file.WhichOneof('data')
        if response == 'error':
            code = MotionMasterMessage.Status.DeviceFile.Error.Code.Name(
                getattr(message.status.device_file, response).code)
            raise OperationFailed("DeleteDeviceFile request returned error '{}' ({}) for file {}".format(
                getattr(message.status.device_file, response).message, code, message.status.device_file.name))

        logger.debug("Deleted file %s with success message '%s'",
                     message.status.device_file.name, getattr(message.status.device_file, response).message)

    def get_device_parameter_value(self, device_address: int, index: int, subindex: int) -> Any:
        """Return the value of the object at the given index and subindex.

        Good for retrieving a single object value. This method blocks with a timeout.

        Returns
        -------
        value : Any
            The content of the object requested
        """
        msg, obs = self._get_request_and_single_response_observable("get 0x{:04x}:{}".format(index, subindex))
        msg.request.get_device_parameter_values.device_address = device_address
        parameter = msg.request.get_device_parameter_values.parameters.add()
        parameter.index = index
        parameter.subindex = subindex
        self.send_to_motion_master(msg)
        message = obs.run()
        received_parameter_value = message.status.device_parameter_values.parameter_values[0]
        # Throw OperationFailed if the value wasn't found.
        _check_device_parameter_value_status(received_parameter_value)
        # Return the value.
        value = getattr(received_parameter_value, received_parameter_value.WhichOneof('type_value'))
        return value

    def get_multi_device_parameter_values(self, request_list: List[Tuple[int, List[Tuple[int, int]]]]) \
            -> List[Tuple[int, List[Tuple[int, int, Any]]]]:
        """Return the values of multiple parameter for multiple devices.

        This is the big brother of the `get_device_parameter_value()` method, and will, with one request, retrieve a
        set of object values across multiple devices. This method blocks with a timeout.

        Parameters
        ----------
        request_list : [(device_address, [(index, subindex)])]
            A list of devices and for each device a list of objects to read

        Returns
        -------
        response_list : [(device_address, [(index, subindex, value)])]
            Same structured list as the request, but with a value included for each object
        """
        # Pack the request into a MotionMasterMessage.
        msg, obs = self._get_request_and_single_response_observable("get multi {}".format(request_list))
        collection = msg.request.get_multi_device_parameter_values.collection
        for device_address, parameter_list in request_list:
            get_device_parameter_value = collection.add()
            get_device_parameter_value.device_address = device_address
            for index, subindex in parameter_list:
                parameter = get_device_parameter_value.parameters.add()
                parameter.index = index
                parameter.subindex = subindex
        # Transmit the message and wait for a response.
        self.send_to_motion_master(msg)
        message = obs.run()
        # Unpack the response.
        multi_device_parameter_values = []
        for device_parameter_values in message.status.multi_device_parameter_values.collection:
            device_address = device_parameter_values.device_address
            parameter_values = []
            for received_parameter_value in device_parameter_values.parameter_values:
                try:
                    _check_device_parameter_value_status(received_parameter_value)
                    value = getattr(received_parameter_value, received_parameter_value.WhichOneof('type_value'))
                    parameter = (received_parameter_value.index, received_parameter_value.subindex, value)
                    parameter_values.append(parameter)
                except OperationFailed as e:
                    logger.warning("Request to get a bunch of parameters failed.")
                    raise e

            multi_device_parameter_values.append((device_address, parameter_values))
        return multi_device_parameter_values

    def set_device_parameter_value(self, device_address: int, index: int, subindex: int, value: Any):
        """Set a device parameter to a specific value. This method blocks until write is confirmed.

        Please remember to initialize the wrapper with `initialize_device_parameter_info_dict()` before calling
        this function.

        Parameters
        ----------
        device_address : int
            The unique identifier for the device you wish to use
        index : int
            The designated index of an object in the Object dictionary
        subindex : int
            The designated subindex of the above specified object
        value : Any
            The value to write to the given index and subindex. The type will be looked up from the device parameter
            info stored locally.

        Returns
        -------
        TODO: This should probably return something useful. It throws if it fails, so what good is this?

        Raises
        ------
        TypeError
            When the type of the value can't be deduced.
        OperationFailed
            When the set operation reports an error.
        """

        msg, obs = self._get_request_and_single_response_observable(
            "set 0x{:04x}:{} to {}".format(index, subindex, value))
        msg.request.set_device_parameter_values.device_address = device_address
        parameter = msg.request.set_device_parameter_values.parameter_values.add()
        parameter.index = index
        parameter.subindex = subindex

        # Set the value based on the type.
        if len(self.device_and_parameter_info_dict) == 0:
            raise TypeError("Unclear how to encode {} in the message; dictionary is not populated.".format(value))

        param_info = self.device_and_parameter_info_dict.get(str(device_address)).get('parameters').get(
            "{:04x}:{}".format(index, subindex))
        if param_info is None:
            raise KeyError("Object 0x{:04x}:{} doesn't exist for device {}.".format(index, subindex, device_address))
        if 1 <= param_info.value_type <= 7:
            # Value is some type of integer.
            parameter.int_value = value
        elif param_info.value_type == 8:
            parameter.float_value = value
        elif param_info.value_type == 9:
            parameter.string_value = value
        elif 10 <= param_info.value_type <= 11:
            parameter.raw_value = value
        elif param_info.value_type == 12:
            parameter.int_value = value
        else:
            raise TypeError("Type for object 0x{:04x}:{} isn't understood.".format(index, subindex))

        self.send_to_motion_master(msg)
        message = obs.run()
        return_parameter = message.status.device_parameter_values.parameter_values[0]
        _check_device_parameter_value_status(return_parameter)

    def get_motion_master_version(self) -> str:
        """Return the version string of the Motion Master

        This method blocks with a timeout.

        Returns
        -------
        version : str
            The version string. I.e. v3.0.0-alpha.2
        """
        # Build the message and send it to the Motion Master.
        msg, obs = self._get_request_and_single_response_observable("get Motion Master version")
        msg.request.get_system_version.SetInParent()
        self.send_to_motion_master(msg)
        message = obs.run()
        return message.status.system_version.version

    def initialize_device_parameter_info_dict(self):
        """Retrieve and set device_parameter_info_dict for all devices

        This dictionary is stored locally and used by the wrapper to determine types. This method should be run once
        during initialization of this wrapper.

        Returns
        -------
        Nothing, but sets a local dict named `device_parameter_info_dict`
        """
        # Find all devices on the network
        logger.debug("Retrieving the list of devices found on the network...")
        msg, obs = self._get_request_and_single_response_observable("get device info")
        msg.request.get_device_info.SetInParent()
        self.send_to_motion_master(msg)
        message = obs.run()
        obs.dispose()
        self.device_and_parameter_info_dict.clear()
        for device_info in message.status.device_info.devices:
            # Store the device under the device_address as the primary key.
            # TODO: Rewrite these update() calls with a faster array notation.
            self.device_and_parameter_info_dict.update({str(device_info.device_address): {"info": device_info}})

        # TODO: Handle the case where this fails to get the list (timeout, bad error code?).
        if len(self.device_and_parameter_info_dict) < 1:
            logger.warning("No devices were found on the network!")
        else:
            logger.info("Found %s devices on the network.", len(self.device_and_parameter_info_dict))

        # Get the parameter info for each device
        for device_address, device_dict in self.device_and_parameter_info_dict.items():
            logger.info("Retrieving parameter info for device %s at position %s ...",
                        device_dict['info'].device_address, device_dict['info'].position)
            msg, obs = self._get_request_and_single_response_observable(
                "get parameters from device {} ({})".format(
                    device_dict['info'].position, device_dict['info'].device_address))
            msg.request.get_device_parameter_info.device_address = device_dict['info'].device_address
            self.send_to_motion_master(msg)
            message = obs.run()
            # Go through and store all the parameter information for lookup later.
            self.device_and_parameter_info_dict[device_address]['parameters'] = {}
            for parameter in message.status.device_parameter_info.parameters:
                self.device_and_parameter_info_dict[device_address]['parameters'].update(
                    {"{:04x}:{}".format(parameter.index, parameter.subindex): parameter})

            if len(self.device_and_parameter_info_dict.get(device_address)) < 2:  # 2 because 'info' is the first.
                logger.warning("  ...retrieval failed; didn't find any entries!")
            else:
                logger.info("  ...completed successfully.")

    def start_collection(self, topic_name: str, device_address: int, parameter_list: List[Tuple[int, int]],
                         sampling_period_us: int = None, duration_s: float = None):
        """Setup and start collection of the objects in the list.

        This will configure the Motion Master to monitor the objects and return their values at the specified frequency.

        Parameters
        ----------
        topic_name : str
            An identifier that should be unique to the running instance of Motion Master. Good practice may be to append
            the name of your application. That way, topics from other clients are unlikely to collide.
        device_address : int
            The unique identifier for the device you wish to use.
        parameter_list : List[Tuple[int, int]]
            A list of the parameters from the object dictionary for sampling. I.e. [(0x2001, 1), (0x603f, 0)]
        sampling_period_us : int
            Number of microseconds between samples. Note that this is (likely) limited to multiples of milliseconds.
        duration_s : float, optional
            The time in seconds to collect samples. Total number of samples is sampling_period_us * duration_s. If not
            specified, the collection will continue until canceled.

        Raises
        ------
        Exception
            TODO: Something is raised here, I think.

        See Also
        --------
        stop_collection
        clear_collection
        retrieve_collection
        """
        if topic_name in self.collection:
            raise KeyError("Key `{}` already exists. Cancel or retrieve collection for this key first."
                           .format(topic_name))

        # Init the collection with an empty list for each object.
        local_collection_topic = OrderedDict()
        for index, subindex in parameter_list:
            local_collection_topic["{:04x}:{}".format(index, subindex)] = list()
        local_collection_topic["time"] = list()
        self.collection[topic_name] = local_collection_topic

        # Build an observer that puts the message values into the array.
        def put_values_into_collection_array(message):
            """Assumes the message is already filtered for topic."""
            local_collection_topic["time"].append(message.status.monitoring_parameter_values.timestamp)
            for received_parameter_value in \
                    message.status.monitoring_parameter_values.device_parameter_values.parameter_values:
                value = getattr(received_parameter_value, received_parameter_value.WhichOneof('type_value'))
                local_collection_topic["{:04x}:{}".format(received_parameter_value.index,
                                                          received_parameter_value.subindex)].append(value)

        def handle_completed_collection():
            """Stops the Motion Master monitoring and flags the data as complete."""
            local_collection_topic['is_complete'] = True
            # Request the Motion Master to kill the monitoring topic.
            msg = MotionMasterMessage()
            msg.id = str(uuid.uuid4())
            msg.request.stop_monitor_device_parameter_values.start_monitoring_request_id = \
                local_collection_topic['request_id']
            self.send_to_motion_master(msg)

        # Build an observable that takes up to N samples.
        self._mmb.subscribe_to_topic(topic_name)

        if duration_s is None:
            # Complete means it can be retrieved immediately.
            local_collection_topic['is_complete'] = True
            disposable = self._mmb.get_topics_subject().pipe(
                ops.filter(lambda data: data[0].decode("utf-8") == topic_name),  # topic must match
                ops.map(lambda data: decode(data[1])),  # convert the data to a message
            ).subscribe(observer=put_values_into_collection_array)
        else:
            number_of_samples = int(duration_s * 1e6 / sampling_period_us)
            local_collection_topic['is_complete'] = False
            disposable = self._mmb.get_topics_subject().pipe(
                ops.filter(lambda data: data[0].decode("utf-8") == topic_name),  # topic must match
                ops.map(lambda data: decode(data[1])),  # convert the data to a message
                ops.take(number_of_samples),  # kill obs after we've seen all the messages we want
                ops.timeout(duration_s + self.timeout_s),  # default timeout after duration is exceeded
            ).subscribe(observer=put_values_into_collection_array,
                        on_completed=handle_completed_collection)

        # Save the resulting disposable for later reference.
        local_collection_topic['disposable'] = disposable

        # Request the Motion Master to start monitoring the desired values.
        msg = MotionMasterMessage()
        unique_message_id = str(uuid.uuid4())
        msg.id = unique_message_id
        local_collection_topic['request_id'] = unique_message_id
        msg.request.start_monitoring_device_parameter_values.topic = topic_name
        msg.request.start_monitoring_device_parameter_values.interval = sampling_period_us
        get_device_parameter_values = msg.request.start_monitoring_device_parameter_values.get_device_parameter_values
        get_device_parameter_values.device_address = device_address
        for index, subindex in parameter_list:
            parameter = get_device_parameter_values.parameters.add()
            parameter.index = index
            parameter.subindex = subindex
        self.send_to_motion_master(msg)

    def stop_collection(self, topic_name: str):
        """Interrupt collection, stop acquisition, and tell Motion Master to stop monitoring.

        Parameters
        ----------
        topic_name : str
            The name used to schedule the original collection.

        Returns
        -------
        Nothing.

        See Also
        --------
        start_collection
        clear_collection
        retrieve_collection
        """
        # Kill the observable.
        local_collection_topic = self.collection.get(topic_name)
        if not local_collection_topic:
            logger.warning("{} not inside collection".format(topic_name))
            return False
        local_collection_topic['disposable'].dispose()
        # Stop the monitoring service on the Motion Master.
        # TODO: I really wish there was a better way of triggering this message from the observer.
        msg = MotionMasterMessage()
        msg.id = str(uuid.uuid4())
        msg.request.stop_monitoring_device_parameter_values.start_monitoring_request_id = \
            local_collection_topic['request_id']
        self.send_to_motion_master(msg)
        return True

    def clear_collection(self, topic_name: str):
        """Interrupt collection, cancel the operation, and throw out any collected data.

        Parameters
        ----------
        topic_name : str
            This is the name used to schedule the original collection.

        See Also
        --------
        start_collection
        stop_collection
        retrieve_collection
        """
        # Stop everything!
        if self.stop_collection(topic_name):
            # Forget everything about this topic, including data and ID's.
            del self.collection[topic_name]

    def retrieve_collection(self, topic_name: str) -> Dict[str, List[Any]]:
        """Block until the collection completes and return the data.

        Note that collections with no duration are always 'complete', so this method will always directly return the
        most current chunk of data.

        Parameters
        ----------
        topic_name : str
            This is the name used to schedule the original collection.

        Raises
        ------
        KeyError
            If the topic name isn't part of an existing collection.

        Returns
        -------
        Dict[str, List[Any]]
            A dictionary containing one list of values for every 'index:subindex' key.

        See Also
        --------
        start_collection
        stop_collection
        clear_collection
        """
        local_collection_topic = self.collection.get(topic_name)
        if not local_collection_topic:
            logger.warning("{} not inside collection".format(topic_name))
            return None
        # Wait until it finishes or timeout.
        while not local_collection_topic['is_complete']:
            time.sleep(0.01)  # prevent spamming
        return local_collection_topic

    def set_signal_generator_parameters_position_bidirectional(self, device_address: int, acceleration: int,
                                                               deceleration: int, profile_velocity: int,
                                                               target_position: int, sustain_time: int):
        """
        Parameters
        ----------
        device_address : int
            address of the device
        acceleration : int
            acceleration value for the profile
        deceleration : int
            deceleration value for the profile
        profile_velocity : int
            Velocity value for the profile
        target_position : int
            target position value
        sustain_time : int
            flat part of the trajectory at the target velocity (ms)
        Returns
        -------
        """
        msg, obs = self._get_request_and_single_response_observable("SetSignalGeneratorParameters")
        msg.request.set_signal_generator_parameters.device_address = device_address
        position_bidirectional = msg.request.set_signal_generator_parameters.position_bidirectional
        position_bidirectional.target = target_position
        position_bidirectional.profile_velocity = profile_velocity
        position_bidirectional.profile_acceleration = acceleration
        position_bidirectional.profile_deceleration = deceleration
        position_bidirectional.sustain_time = sustain_time
        position_bidirectional.repeat = False
        position_bidirectional.master_generated = True
        position_bidirectional.absolute_target = False
        self.send_to_motion_master(msg)

        message = obs.run()
        response = message.status.signal_generator.WhichOneof('status')

        if response == 'error':
            code = MotionMasterMessage.Status.SignalGenerator.Error.Code.Name(
                getattr(message.status.signal_generator, response).code)
            raise OperationFailed("For device {}, signal generator returned error {}, while setting parameters"
                                  .format(device_address, code))

    def set_signal_generator_parameters_velocity_bidirectional(self, device_address: int, acceleration: int,
                                                               deceleration: int, target: int, sustain_time: int):
        """Set the parameters of the signal generator for bi-directional trapezoidal profile
        Parameters
        ----------
        device_address : int
            address of the device
        acceleration : int
            acceleration value for the profile
        deceleration : int
            deceleration value for the profile
        target : int
            target velocity of the profile
        sustain_time : int
            flat part of the trajectory at the target velocity (ms)
        Returns
        -------
            nothing
        """

        msg, obs = self._get_request_and_single_response_observable("SetSignalGeneratorParameters")
        msg.request.set_signal_generator_parameters.device_address = device_address
        velocity_bidirectional = msg.request.set_signal_generator_parameters.velocity_bidirectional
        velocity_bidirectional.target = target
        velocity_bidirectional.profile_acceleration = acceleration
        velocity_bidirectional.profile_deceleration = deceleration
        velocity_bidirectional.sustain_time = sustain_time
        velocity_bidirectional.repeat = False
        velocity_bidirectional.master_generated = True
        self.send_to_motion_master(msg)
        message = obs.run()
        response = message.status.signal_generator.WhichOneof('status')

        if response == 'error':
            code = MotionMasterMessage.Status.SignalGenerator.Error.Code.Name(
                getattr(message.status.signal_generator, response).code)
            raise OperationFailed("For device {}, signal generator returned error {}, while setting parameters"
                                  .format(device_address, code))

    def set_signal_generator_parameters_position_ramp(self, device_address: int, acceleration: int,
                                                      deceleration: int, profile_velocity: int,
                                                      target_position: int, sustain_time: int):
        """
        Parameters
        ----------
        device_address : int
            address of the device
        acceleration : int
            acceleration value for the profile
        deceleration : int
            deceleration value for the profile
        profile_velocity : int
            Velocity value for the profile
        target_position : int
            target position value
        sustain_time : int
            flat part of the trajectory at the target position (ms)
        Returns
        -------
        """
        msg, obs = self._get_request_and_single_response_observable("SetSignalGeneratorParameters")
        msg.request.set_signal_generator_parameters.device_address = device_address
        position_ramp = msg.request.set_signal_generator_parameters.position_ramp
        position_ramp.target = target_position
        position_ramp.profile_velocity = profile_velocity
        position_ramp.profile_acceleration = acceleration
        position_ramp.profile_deceleration = deceleration
        position_ramp.sustain_time = sustain_time
        position_ramp.master_generated = True
        position_ramp.absolute_target = False
        self.send_to_motion_master(msg)

        message = obs.run()
        response = message.status.signal_generator.WhichOneof('status')

        if response == 'error':
            code = MotionMasterMessage.Status.SignalGenerator.Error.Code.Name(
                getattr(message.status.signal_generator, response).code)
            raise OperationFailed("For device {}, signal generator returned error {}, while setting parameters"
                                  .format(device_address, code))

    def set_signal_generator_parameters_velocity_ramp(self, device_address: int, acceleration: int,
                                                      deceleration: int, target: int, sustain_time: int):
        """Set the parameters of the signal generator for ramp profile
               Parameters
               ----------
               device_address : int
                   address of the device
               acceleration : int
                   acceleration value for the profile
               deceleration : int
                   deceleration value for the profile
               target : int
                   target velocity of the profile
               sustain_time : int
                   flat part of the trajectory at the target velocity (ms)
               Returns
               -------
                   nothing
               """

        msg, obs = self._get_request_and_single_response_observable("SetSignalGeneratorParameters")
        msg.request.set_signal_generator_parameters.device_address = device_address
        velocity_ramp = msg.request.set_signal_generator_parameters.velocity_ramp
        velocity_ramp.target = target
        velocity_ramp.profile_acceleration = acceleration
        velocity_ramp.profile_deceleration = deceleration
        velocity_ramp.sustain_time = sustain_time
        velocity_ramp.master_generated = True
        self.send_to_motion_master(msg)
        message = obs.run()
        response = message.status.signal_generator.WhichOneof('status')

        if response == 'error':
            code = MotionMasterMessage.Status.SignalGenerator.Error.Code.Name(
                getattr(message.status.signal_generator, response).code)
            raise OperationFailed("For device {}, signal generator returned error {}, while setting parameters"
                                  .format(device_address, code))

    def set_signal_generator_parameters_position_sine_wave(self, device_address: int, amplitude: int,
                                                      frequency: float):
        """Set the parameters of the signal generator for position sine wave profile
               Parameters
               ----------
               device_address : int
                   address of the device
               amplitude : int
                   amplitude value for the position sine wave profile
               frequency : float
                   frequency value for the position sine wave profile
               Returns
               -------
                   nothing
               """
        # Package the motion master message with the sine wave parameters
        msg, obs = self._get_request_and_single_response_observable("SetSignalGeneratorParameters")
        msg.request.set_signal_generator_parameters.device_address = device_address
        position_sine_wave = msg.request.set_signal_generator_parameters.position_sine_wave
        position_sine_wave.amplitude = amplitude
        position_sine_wave.frequency = frequency
        position_sine_wave.absolute_target = False
        position_sine_wave.repeat = True

        # Send the packaged message to motion master and then run the returned observable.
        self.send_to_motion_master(msg)
        message = obs.run()

        # Catch the response
        response = message.status.signal_generator.WhichOneof('status')

        if response == 'error':
            code = MotionMasterMessage.Status.SignalGenerator.Error.Code.Name(
                getattr(message.status.signal_generator, response).code)
            raise OperationFailed("For device {}, signal generator returned error {}, while setting parameters"
                                  .format(device_address, code))

    def set_signal_generator_parameters_velocity_sine_wave(self, device_address: int, amplitude: int,
                                                      frequency: float):
        """Set the parameters of the signal generator for velocity sine wave profile
               Parameters
               ----------
               device_address : int
                   address of the device
               amplitude : int
                   amplitude value for the velocity sine wave profile
               frequency : float
                   frequency value for the velocity sine wave profile
               Returns
               -------
                   nothing
               """
        # Package the motion master message with the sine wave parameters
        msg, obs = self._get_request_and_single_response_observable("SetSignalGeneratorParameters")
        msg.request.set_signal_generator_parameters.device_address = device_address
        velocity_sine_wave = msg.request.set_signal_generator_parameters.velocity_sine_wave
        velocity_sine_wave.amplitude = amplitude
        velocity_sine_wave.frequency = frequency
        velocity_sine_wave.repeat = True

        # Send the packaged message to motion master and then run the returned observable.
        self.send_to_motion_master(msg)
        message = obs.run()

        # Catch the response
        response = message.status.signal_generator.WhichOneof('status')

        if response == 'error':
            code = MotionMasterMessage.Status.SignalGenerator.Error.Code.Name(
                getattr(message.status.signal_generator, response).code)
            raise OperationFailed("For device {}, signal generator returned error {}, while setting parameters"
                                  .format(device_address, code))

    def motion_master_start_signal_generator(self, device_address: int):
        """ Starts the signal generator and generates configured profile for the device
        Parameters
        ----------
        device_address : int
            address of the device
        Returns
        -------
            Nothing
        """
        msg, obs = self._get_request_and_single_response_observable("start_signal_generator")
        msg.request.start_signal_generator.device_address = device_address
        self.send_to_motion_master(msg)
        message = obs.run()
        response = message.status.signal_generator.WhichOneof('status')

        if response == 'error':
            code = MotionMasterMessage.Status.SignalGenerator.Error.Code.Name(
                getattr(message.status.signal_generator, response).code)
            raise OperationFailed("For device {}, signal generator returned error {}, while starting"
                                  .format(device_address, code))

    def motion_master_stop_signal_generator(self, device_address):
        """ Stops the signal generator
        Parameters
        ----------
        device_address : int
            address of the device
        Returns
        -------
        Nothing
        """

        msg, obs = self._get_request_and_single_response_observable("stop_signal_generator")
        msg.request.stop_signal_generator.device_address = device_address
        self.send_to_motion_master(msg)
        message = obs.run()
        response = message.status.signal_generator.WhichOneof('status')

        if response == 'error':
            code = MotionMasterMessage.Status.SignalGenerator.Error.Code.Name(
                getattr(message.status.signal_generator, response).code)
            raise OperationFailed("For device {}, signal generator returned error {}, while stopping".
                                  format(device_address, code))
