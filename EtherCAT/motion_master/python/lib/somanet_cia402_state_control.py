from motion_master_wrapper import MotionMasterWrapper

from typing import Any, List, Tuple, Dict
from enum import Enum, unique
import time
import logging

logger = logging.getLogger(__name__)


class ExceptionStateControl(Exception):
    pass


class ExceptionTimeout(ExceptionStateControl):
    pass


class ExceptionOpMode(ExceptionStateControl):
    pass


class Controlword:
    """
    Helps manage the bits in the CiA402 Control word.

    Bit table
    ---------

    Bit   Description
    ===   ===========
      0   Switch on
      1   Enable voltage
      2   Quick stop (INVERTED LOGIC)
      3   Enable operation
      4   -opmode specific-
      5   -opmode specific-
      6   -opmode specific-
      7   Fault reset
      8   Halt
      9   -reserved-
     10   -reserved-
     11   -manufacture-unused-
     12   -manufacture-unused-
     13   -manufacture-unused-
     14   -manufacture-unused-
     15   -manufacture-unused-

    Bit   Profile position    Homing
    ===   ================    ======
      4   New set point       Homing operation start
      5   Change set now      -
      6   Relative mode       -

    See Also
    --------
    IEC 61800-7-201 Generic interface and use of profiles for power drive systems
    """

    #
    # Define the bits in the Controlword.
    #

    SWITCH_ON = 0x0001
    ENABLE_VOLTAGE = 0x0002
    QUICK_STOP = 0x0004
    ENABLE_OPERATION = 0x0008
    FAULT_RESET = 0x0080
    HALT = 0x0100

    # Operation mode specific

    PP_NEW_SETPOINT = 0x0010
    PP_CHANGE_SET_POINT_NOW = 0x0020
    PP_RELATIVE_MODE = 0x0040
    PP_CHANGE_ON_SET_POINT = 0x0200

    HOMING_OPERATION_START = 0x0010

    def __init__(self, value: int = 0x00):
        self.value = int(value)

    def __int__(self):
        return self.value

    def update(self, cw: int):
        """
        Update internal controlword value

        Parameters
        ----------
        cw : int
            New controlword

        """
        self.value = cw

    def set(self, bits_in_word: int):
        """Set the bits in the argument in the Controlword."""
        self.value |= bits_in_word
        return self.value

    def clear(self, bits_in_word: int):
        """Clear the bits in the argument from the Controlword."""
        self.value &= ~bits_in_word
        return self.value

    def _bit_manipulator(self, bit: int, set_bit: bool = True):
        """
        Changes bit depending on set_bit.

        Parameters
        ----------
        bit : int
            Bit position
        set_bit : bool
            If true, set bit, otherwise clear it

        Returns
        -------
        int
            Return new Controlword

        """
        return self.set(bit) if set_bit else self.clear(bit)

    def shutdown(self):
        """Set the command to shutdown. (Enter Switch on disabled)

        Transitions: 2, 6, 8

        Returns
        -------
        value : int
            The value of the control word
        """
        # Clear Fault reset (7), Switch on (0)
        self.value &= (~self.FAULT_RESET & ~self.SWITCH_ON) & 0xffff
        # Set Quick-stop (2), Enable voltage (1)
        self.value |= self.QUICK_STOP | self.ENABLE_VOLTAGE
        return self.value

    def switch_on(self):
        """Set the command to switch the drive on. (Enter Switched on)

        Transitions: 3

        Returns
        -------
        value : int
            The value of the control word
        """
        # Clear Fault reset (7), Enable operation (3)
        self.value &= (~self.FAULT_RESET & ~self.ENABLE_OPERATION) & 0xffff
        # Set Quick-stop (2), Enable voltage (1), Switch on (0)
        self.value |= self.QUICK_STOP | self.ENABLE_VOLTAGE | self.SWITCH_ON
        return self.value

    def disable_voltage(self):
        """Set the command to disable voltage. (Enter Switch on disabled)

        Transitions: 7, 9, 10, 12

        Returns
        -------
        value : int
            The value of the control word
        """
        # Clear Fault reset (7), Enable voltage (1)
        self.value &= (~self.FAULT_RESET & ~self.ENABLE_VOLTAGE) & 0xffff
        return self.value

    def quick_stop(self):
        """Set the command to issue a quick-stop. (Enter Quick stop)

        Transition: 7, 10, 11

        Returns
        -------
        value : int
            The value of the control word
        """
        # Clear Fault reset (7), Quick stop (2)
        self.value &= (~self.FAULT_RESET & ~self.QUICK_STOP) & 0xffff
        # Set Enable voltage (1)
        self.value |= self.ENABLE_VOLTAGE
        return self.value

    def disable_operation(self):
        """Set the command to disable operation. (Enter Ready to switch on)

        Transitions: 5

        Returns
        -------
        value : int
            The value of the control word
        """
        # Clear Fault reset (7), Enable operation (3)
        self.value &= (~self.FAULT_RESET & ~self.ENABLE_OPERATION) & 0xffff
        # Set Quick-stop (2), Enable voltage (1), Switch on (0)
        self.value |= self.QUICK_STOP | self.ENABLE_VOLTAGE | self.SWITCH_ON
        return self.value

    def enable_operation(self):
        """Set the command to enable operation (Enter Operation enabled)

        This can be used to automatically transition through Switched on.

        Transition: 4, 16

        Returns
        -------
        value : int
            The value of the control word
        """
        # Clear Fault reset (7)
        self.value &= ~self.FAULT_RESET & 0xffff
        # Set Enable operation (3), Quick-stop (2), Enable voltage (1), Switch on (0)
        self.value |= self.QUICK_STOP | self.ENABLE_OPERATION | self.SWITCH_ON | self.ENABLE_VOLTAGE
        return self.value

    def fault_reset(self):
        """Set the command to reset the fault. (Enter Switch on disabled)

        This will reset the fault only after the bit transitions from 0 to 1. For this to
        work properly when a fault is triggered right after another fault was cleared, you can
        send the `shutdown()` command and _then_ send this `fault_reset()`. Therefore, unless
        speed is essential, clearing a fault should be always preceded by a `shutdown()`.

        Transition: 15

        Returns
        -------
        value : int
            The value of the control word
        """
        # Set Fault reset (7)
        self.value = self.FAULT_RESET
        return self.value

    def halt(self, set_bit: bool = True) -> int:
        """
        Set the Halt bit (bit 8)

        Parameters
        ----------
        set_bit : bool
            If true, set bit, otherwise clear it

        Returns
        -------
        int
            The new controlword
        """
        return self._bit_manipulator(self.HALT, set_bit)

    def relative_position_mode(self, set_bit: bool = True) -> int:
        """
        Changes bit 6 in controlword (Profile Position Mode).
        If bit is 1, target position shall be a relative value.
        If bit is 0, target position shall be an absolute value.

        Parameters
        ----------
        set_bit : bool
            If true, set bit, otherwise clear it
        Returns
        -------
        int
            The new controlword
        """

        return self._bit_manipulator(self.PP_RELATIVE_MODE, set_bit)

    def new_setpoint(self, set_bit: bool = True) -> int:
        """
        Changes bit 4 in controlword. (Profile Position Mode)

        Parameters
        ----------
        set_bit : bool
            If true, set bit, otherwise clear it

        Returns
        -------
        int
            The new controlword
        """
        return self._bit_manipulator(self.PP_NEW_SETPOINT, set_bit)

    def change_set_point_now(self, set_bit: bool = True) -> int:
        """
        Changes bit 5 in controlword. (Profile Position Mode)

        Parameters
        ----------
        set_bit : bool
            If true, set bit, otherwise clear it

        Returns
        -------
        int
            The new controlword
        """
        return self._bit_manipulator(self.PP_CHANGE_SET_POINT_NOW, set_bit)

    def change_on_set_point(self, set_bit: bool = True) -> int:
        """
        Changes bit 9 in controlword (Profile Position Mode)

        Parameters
        ----------
        set_bit : bool
            If true, set bit, otherwise clear it

        Returns
        -------
        int
            The new controlword
        """

        return self._bit_manipulator(self.PP_CHANGE_ON_SET_POINT, set_bit)

    def start_homing(self, set_bit: bool = True) -> int:
        """

        Parameters
        ----------
        set_bit : bool
            If true, set bit, otherwise clear it

        Returns
        -------
        int
            The new controlword
        """
        return self._bit_manipulator(self.HOMING_OPERATION_START, set_bit)


class Statusword:
    """
    Helps manage the bits in the CiA402 Statusword.

    Bit table
    ---------

    Bit   Description
    ===   ===========
      0   Ready to switch on
      1   Switched on
      2   Operation enabled
      3   Fault
      4   Voltage enabled
      5   Quick stop
      6   Switch on disabled
      7   Warning
      8   -manufacturer-specific-
      9   Remote
     10   Target reached
     11   Internal limit active
     12   -operation mode specific-
     13   -operation mode specific-
     14   -manufacturer-specific-
     15   -manufacturer-specific-

    Bit   Homing              ?
    ===   ================    ======
     12   Homing attained     ?
     13   Homing error        ?

    See Also
    --------
    IEC 61800-7-201 Generic interface and use of profiles for power drive systems
    """

    def __init__(self, sw: int = 0x00):
        self.value = sw

    def __int__(self):
        return self.value

    def update(self, value):
        """Update the value of the Statusword."""
        self.value = value

    def _compare(self, sw: int, mask: int, bitpattern: int) -> bool:
        """
        Checks, if bit pattern is set in statusword.
        Internal helper function.
        Parameters
        ----------
        sw : int
            Statusword. Can be None. Then it will not be updated
        mask : int
            Bit mask for statusword
        bitpattern : int
            Bit pattern against which the status word is compared

        Returns
        -------
        bool

        """
        if sw:
            self.update(sw)
        return self.value & mask == bitpattern

    def is_state_not_ready_to_switch_on(self, sw=None) -> bool:
        """Is the state 'Not ready to switch on'?

        Parameters
        ----------
        sw : int
            Statusword. Will update the internal value.

        Returns
        -------
        bool
            True if in state 'Not ready to switch on'
        """
        mask = 0b0000000001001111  # the bits we care about
        value = 0b0000000000000000  # the value of those bits
        return self._compare(sw, mask, value)

    def is_state_switch_on_disabled(self, sw=None) -> bool:
        """Is the state 'Switch on disabled'?

        Parameters
        ----------
        sw : int
            Statusword. Will update the internal value.

        Returns
        -------
        bool
            True if in state 'Switch on disabled'
        """
        mask = 0b0000000001001111  # the bits we care about
        value = 0b0000000001000000  # the value of those bits
        return self._compare(sw, mask, value)

    def is_state_ready_to_switch_on(self, sw=None) -> bool:
        """Is the state 'Ready to switch on'?

        Parameters
        ----------
        sw : int
            Statusword. Will update the internal value.

        Returns
        -------
        bool
            True if in state 'Ready to switch on'
        """
        mask = 0b0000000001101111  # the bits we care about
        value = 0b0000000000100001  # the value of those bits
        return self._compare(sw, mask, value)

    def is_state_switched_on(self, sw=None) -> bool:
        """Is the state 'Switched on'?

        Parameters
        ----------
        sw : int
            Statusword. Will update the internal value.

        Returns
        -------
        bool
            True if in state 'Switched on'
        """
        mask = 0b0000000001101111  # the bits we care about
        value = 0b0000000000100011  # the value of those bits
        return self._compare(sw, mask, value)

    def is_state_operation_enabled(self, sw=None) -> bool:
        """Is the state 'Operation enabled'?

        Parameters
        ----------
        sw : int
            Statusword. Will update the internal value.

        Returns
        -------
        bool
            True if in state 'Operation enabled'
        """
        mask = 0b0000000001101111  # the bits we care about
        value = 0b0000000000100111  # the value of those bits
        return self._compare(sw, mask, value)

    def is_state_quick_stop_active(self, sw=None) -> bool:
        """Is the state 'Quick stop active'?

        Parameters
        ----------
        sw : int
            Statusword. Will update the internal value.

        Returns
        -------
        bool
            True if in state 'Quick stop active'
        """
        mask = 0b0000000001101111  # the bits we care about
        value = 0b0000000000000111  # the value of those bits
        return self._compare(sw, mask, value)

    def is_state_fault_reaction_active(self, sw=None) -> bool:
        """Is the state 'Fault reaction active'?

        Parameters
        ----------
        sw : int
            Statusword. Will update the internal value.

        Returns
        -------
        bool
            True if in state 'Fault reaction active'
        """
        mask = 0b0000000001001111  # the bits we care about
        value = 0b0000000000001111  # the value of those bits
        return self._compare(sw, mask, value)

    def is_state_fault(self, sw=None) -> bool:
        """Is the state 'Fault'?

        Parameters
        ----------
        sw : int
            Statusword. Will update the internal value.

        Returns
        -------
        bool
            True if in state 'Fault'
        """
        mask = 0b0000000001001111  # the bits we care about
        value = 0b0000000000001000  # the value of those bits
        return self._compare(sw, mask, value)

    def has_fault(self, sw=None) -> bool:
        """Is the fault bit set?

        Parameters
        ----------
        sw : int
            Statusword. Will update the internal value.

        Returns
        -------
        bool
            True if 'Fault' bit is set
        """
        fault_bit = 0b0000000000001000
        return self._compare(sw, fault_bit, fault_bit)

    def has_warning(self, sw=None) -> bool:
        """Is the warning bit set?

        Parameters
        ----------
        sw : int
            Statusword. Will update the internal value.

        Returns
        -------
        bool
            True if 'Warning' bit is set
        """
        warning_bit = 0b0000000010000000
        return self._compare(sw, warning_bit, warning_bit)

    def is_target_reached(self, sw=None) -> bool:
        """Is the target reached bit set?

        Parameters
        ----------
        sw : int
            Statusword. Will update the internal value.

        Returns
        -------
        bool
            True if target is reached
        """
        target_reached_bit = 0b0000010000000000
        return self._compare(sw, target_reached_bit, target_reached_bit)

    def is_homing_attained(self, sw=None) -> bool:
        """Is the homing attained bit set? (Bit 12, Operation Mode Specific)

        Parameters
        ----------
        sw : int
            Statusword. Will update the internal value.

        Returns
        -------
        bool
            True if homing is attained
        """
        homing_attained_bit = 0b0001000000000000
        return self._compare(sw, homing_attained_bit, homing_attained_bit)

    def has_homing_error(self, sw=None) -> bool:
        """Is the homing error bit set? (Bit 13, Operation Mode Specific)

        Parameters
        ----------
        sw : int
            Statusword. Will update the internal value.

        Returns
        -------
        bool
            True if homing has error
        """
        homing_error_bit = 0b0010000000000000
        return self._compare(sw, homing_error_bit, homing_error_bit)

    def is_speed_zero(self, sw=None) -> bool:
        """
        Check, if bit 12 is set. (operation mode specific)

        Parameters
        ----------
        sw : int
            Statusword. Will update the internal value.

        Returns
        -------

        """
        speed_zero_bit = (1 << 12)
        return self._compare(sw, speed_zero_bit, speed_zero_bit)

    def get_human_readable_state(self, sw=None) -> str:
        """
        Return method name of the matching state.

        Parameters
        ----------
        sw : int
            Statusword. If None, method will use internal value

        Returns
        -------

        """
        if not sw:
            sw = self.value

        method_names = [a for a in dir(self) if a.startswith('is_state')]
        for m in method_names:
            if m in ['get_human_readable_state', 'update', 'value']:
                continue

            attr = getattr(self, m)
            if attr(sw & 0b1111111): # Just use the state-relevant bits
                self.update(sw)
                return m.replace('is_', '')

@unique
class StateCommands(Enum):
    SHUTDOWN = 0x1
    SWITCH_ON = 0x2
    ENABLE_OPERATION = 0x3
    DISABLE_OPERATION = 0x4
    DISABLE_VOLTAGE = 0x5
    QUICK_STOP = 0x6
    FAULT_RESET = 0x7


@unique
class OpModes(Enum):
    NOT_SET = 1000
    COMMUTATION_OFFSET = -2
    COGGING_COMP_RECORD = -1
    PROFILE_POSITION = 1
    PROFILE_VELOCITY = 3
    PROFILE_TORQUE = 4
    HOMING_MODE = 6
    CSP = 8
    CSV = 9
    CST = 10


@unique
class HomingState(Enum):
    HOMING_IN_PROGRESS = 0
    HOMING_IS_INTERRUPTED = 1
    HOMING_IS_ATTAINED = 2
    HOMING_COMPLETED = 3
    HOMING_ERROR_SPEED_NOT_ZERO = 4
    HOMING_ERROR_SPEED_ZERO = 5


class StateControl:
    OD_CONTROLWORD = (0x6040, 0)
    OD_STATUSWORD = (0x6041, 0)
    OD_OPERATIONMODE = (0x6060, 0)
    OD_OPERATIONMODEDISPLAY = (0x6061, 0)

    TIMEOUT = 4  # s
    TIMEOUT_FIND_INDEX_PULL_BRAKE = 8 # s

    FAULT_RESET_TRY = 5

    STATE_COMMANDS = StateCommands
    OP_MODES = OpModes
    HOMING_STATES = HomingState

    def __init__(self, mmw: MotionMasterWrapper, dev_address: int, log: bool = False):
        """
        StateControl class. Takes care of CiA402 states on our nodes.

        Parameters
        ----------
        mmw : MotionMasterWrapper
            Motion master wrapper provide communication with nodes.

        dev_address : int
            Device address in Ethercat chain.
        """
        self.dev_address = dev_address
        self.mmw = mmw
        self.op_mode = self.OP_MODES.NOT_SET
        self.cw = Controlword(self._get_controlword())
        self.sw = Statusword(self._get_statusword())
        self._current_state = self.sw.get_human_readable_state()
        self.current_timeout = 0

        self._fault_reset_counter = self.FAULT_RESET_TRY

        logger.disabled = not log

    def _mmw_get_param(self, index: int, subindex: int) -> Any:
        """
        Get parameter from device by motion master wrapper.

        Parameters
        ----------
        index : int
            Object dictionary index
        subindex : int
            Object dictionary sub index

        Returns
        -------
        Any
            Value inside object dictionary
        """
        return self.mmw.get_device_parameter_value(self.dev_address, index, subindex)

    def _mmw_set_param(self, index: int, subindex: int, value: Any):
        """
        Set parameter on device by motion master wrapper.

        Parameters
        ----------
        index : int
            Object dictionary index
        subindex : int
            Object dictionary sub index
        value : Any
            The new value for the OD entry

        """
        self.mmw.set_device_parameter_value(self.dev_address, index, subindex, value)

    def _set_timeout(self, timeout: int):
        """
        (Re-)Set timeout.
        Reset timeout after every successful state transmission.

        Parameters
        ----------
        timeout : int
            New timeout in seconds
        """
        self.current_timeout = time.time() + timeout

    def _get_statusword(self) -> int:
        """
        Get statusword from device.

        Returns
        -------
        int
            Statusword from object dictionary
        """
        return self._mmw_get_param(*self.OD_STATUSWORD)

    def _get_controlword(self) -> int:
        """
        Get controlword from device.

        Returns
        -------
        int
            Controlword from object dictionary
        """
        return self._mmw_get_param(*self.OD_CONTROLWORD)

    def _update_statusword(self):
        """
        Update statusword inside statusword manager.
        """
        self.sw.update(self._get_statusword())

    def _send_controlword(self, cw: int):
        """
        Send controlword to device.

        Parameters
        ----------
        cw : int
            Controlword value.

        """
        logger.debug("Controlword: {w:016b}, 0x{w:x}, {w}".format(w=int(self.cw)))
        self._mmw_set_param(*self.OD_CONTROLWORD, cw)

    def set_op_mode(self, mode: OpModes):
        """
        Set operation mode on device.

        Parameters
        ----------
        mode : OpModes
            Operation mode value from Enum OpModes.

        """

        self.op_mode = mode
        logger.debug("Change Op Mode to {}".format(self.op_mode.name))
        self._mmw_set_param(*self.OD_OPERATIONMODE, self.op_mode.value)
        self._set_timeout(self.TIMEOUT)

        # Raise exception if Op mode display is not updated.
        while True:
            op_mode_display = self._mmw_get_param(*self.OD_OPERATIONMODEDISPLAY)
            if op_mode_display == self.op_mode.value:
                break
            elif time.time() > self.current_timeout:
                raise ExceptionTimeout(
                    "Op mode is {} and didn't change to {}.".format(op_mode_display, self.op_mode.value))

    @property
    def device_address(self):
        return self.dev_address

    @property
    def current_state(self) -> str:
        """
        Retuns the current device state as an human readable string.

        Returns
        -------
        str
            Human readable state string (e.g. "state_switched_on")

        """
        return self._current_state

    def _state_machine(self, command: StateCommands) -> bool:
        """
        Heart of this class: The CiA402 state machine.

        Parameters
        ----------
        command : StateCommands
            The command for the state machine (e.g. Fault Reset, Enable Operations). As value of Enum _StateCommands.

        Returns
        -------
        bool
            True if requested state was reached, otherwise false.

        """

        self._current_state = self.sw.get_human_readable_state()

        if command == self.STATE_COMMANDS.FAULT_RESET:
            if not self.sw.has_fault():
                self._fault_reset_counter = self.FAULT_RESET_TRY
                return True
            if self._fault_reset_counter > 0:
                self._fault_reset_counter -= 1
                self._send_controlword(self.cw.shutdown())
                time.sleep(0.3)
                self._set_timeout(self.TIMEOUT)
                self._send_controlword(self.cw.fault_reset())
                time.sleep(0.3)
                return False

            else:
                error_description = self._mmw_get_param(0x203f, 1)
                raise ExceptionStateControl("Could not reset fault '{}'".format(error_description) )

        if self.sw.is_state_not_ready_to_switch_on():
            # Transition 1
            # Automatic state change to switched on disabled. Nothing to do
            return False

        elif self.sw.is_state_switch_on_disabled():
            logger.debug("state_switch_on_disabled")
            # 1, 7, 9, 15, 10, 12
            if command in [self.STATE_COMMANDS.DISABLE_VOLTAGE, self.STATE_COMMANDS.QUICK_STOP]:
                return True
            else:
                # Transition 2
                logger.debug("Do Shutdown")
                self._send_controlword(self.cw.shutdown())
                return False

        elif self.sw.is_state_ready_to_switch_on():
            logger.debug("state_ready_to_switch_on")
            # 2, 6, 8
            if command in [self.STATE_COMMANDS.SHUTDOWN, self.STATE_COMMANDS.FAULT_RESET]:
                return True
            elif command == self.STATE_COMMANDS.QUICK_STOP:
                # Transition 7
                logger.debug("Do Quick stop")
                self._send_controlword(self.cw.quick_stop())
            elif command == self.STATE_COMMANDS.DISABLE_VOLTAGE:
                # Transition 7
                logger.debug("Do Disable Voltage")
                self._send_controlword(self.cw.disable_voltage())
            else:
                # Transition 3
                logger.debug("Do Switch On")
                self._send_controlword(self.cw.switch_on())
            return False

        elif self.sw.is_state_switched_on():
            logger.debug("state_switched_on")
            # 3, 5
            if command in [self.STATE_COMMANDS.SWITCH_ON, self.STATE_COMMANDS.DISABLE_OPERATION]:
                return True
            elif command == self.STATE_COMMANDS.DISABLE_VOLTAGE:
                # Transition 10
                logger.debug("Do Disable Voltage")
                self._send_controlword(self.cw.disable_voltage())
            elif command == self.STATE_COMMANDS.QUICK_STOP:
                # Transition 10
                logger.debug("Do Quick Stop")
                self._send_controlword(self.cw.quick_stop())
            else:
                # Transition 4
                logger.debug("Do Enable Operation")
                self._send_controlword(self.cw.enable_operation())
            return False

        elif self.sw.is_state_operation_enabled():
            logger.debug("state_operation_enabled")
            # 4, 16
            if command == self.STATE_COMMANDS.ENABLE_OPERATION:
                return True
            elif command == self.STATE_COMMANDS.DISABLE_VOLTAGE:
                # Transition 9
                logger.debug("Do disable voltage")
                self._send_controlword(self.cw.disable_voltage())
            elif command == self.STATE_COMMANDS.SHUTDOWN:
                # Transition 8
                logger.debug("Do shutdown")
                self._send_controlword(self.cw.shutdown())
            else:
                # Transition 11
                logger.debug("Do quick stop")
                self._send_controlword(self.cw.quick_stop())
            return False

        elif self.sw.is_state_quick_stop_active():
            logger.debug("state_quick_stop_active")
            # 11
            if command == self.STATE_COMMANDS.QUICK_STOP:
                return True
            elif command == self.STATE_COMMANDS.DISABLE_VOLTAGE:
                # Transition 12
                logger.debug("Do disable voltage")
                self._send_controlword(self.cw.disable_voltage())
            else:
                # Transition 16
                logger.debug("Do enable operation")
                self._send_controlword(self.cw.enable_operation())

        elif self.sw.is_state_fault_reaction_active():
            return False

    def do_transition(self, command: StateCommands):
        """
        Transition manager. Checks, if states was reached. Throws timeout exception, if transition takes to long.

        Parameters
        ----------
        command : StateCommands
            The command for the state machine (e.g. Fault Reset, Enable Operations). As value of Enum _StateCommands.

        Raises
        ------
        ExceptionOpMode
            If op mode is not set by user.

        ExceptionTimeout
            If transition takes too long.
        """

        old_state = self.current_state
        # Check, if op mode was set by user.
        if self.op_mode == self.OP_MODES.NOT_SET and command == self.STATE_COMMANDS.ENABLE_OPERATION:
            raise ExceptionStateControl("No operation mode set")

        # Set timeout
        self._set_timeout(self.TIMEOUT)

        while True:
            logger.debug("Statusword: {sw:016b}, 0x{sw:x}, {st}".format(sw=int(self.sw), st=self.sw.get_human_readable_state()))
            # Get current statusword from device
            self._update_statusword()

            # Do transition.
            if self._state_machine(command):
                return

            # Check if state was changed and reset timeout
            if self.current_state != old_state:
                if self.current_state == 'state_switched_on' and command == self.STATE_COMMANDS.ENABLE_OPERATION:
                    # Set timeout for transition 4 to a higher value, because index detection and pulling brake,
                    # which both is happening in this state, might take longer than the standard timeout.
                    self._set_timeout(self.TIMEOUT_FIND_INDEX_PULL_BRAKE)
                else:
                    self._set_timeout(self.TIMEOUT)
                old_state = self.current_state

            # Raise exception, if transition takes too long.
            if time.time() > self.current_timeout:
                error_description = self._mmw_get_param(0x203f, 1)
                raise ExceptionTimeout("Timeout during state '{}'. Fault: {}".format(self.current_state, error_description))

    def shutdown(self):
        """
        Send command "Shutdown"
        """

        self.do_transition(self.STATE_COMMANDS.SHUTDOWN)

    def switch_on(self):
        """
        Send command "Switch On"
        """
        self.do_transition(self.STATE_COMMANDS.SWITCH_ON)

    def disable_voltage(self):
        """
        Send command "Disable Voltage"
        """
        self.do_transition(self.STATE_COMMANDS.DISABLE_VOLTAGE)

    def quick_stop(self):
        """
        Send command "Quick Stop"
        """
        self.do_transition(self.STATE_COMMANDS.QUICK_STOP)

    def disable_operation(self):
        """
        Send command "Disable Operation"
        """
        self.do_transition(self.STATE_COMMANDS.DISABLE_OPERATION)

    def enable_operation(self, reset_fault: bool =True):
        """
        Send command "Enable Operation"
        """
        if reset_fault:
            self.fault_reset()
        self.do_transition(self.STATE_COMMANDS.ENABLE_OPERATION)

    def fault_reset(self):
        """
        Send command "Fault Reset"
        """
        self.do_transition(self.STATE_COMMANDS.FAULT_RESET)

    def set_state(self, command: StateCommands):
        """
        Send your own state (for automation purposes)

        Parameters
        ----------
        command : StateCommands
            The command for the state machine (e.g. Fault Reset, Enable Operations). As value of Enum _StateCommands.
        """

        self.do_transition(command)

    def has_fault(self) -> bool:
        """
        Checks, if bit 3 is set.

        Returns
        -------
        bool
            True, if bit is 1
        """
        sw = self._get_statusword()
        return self.sw.has_fault(sw)

    def has_warning(self) -> bool:
        """
        Checks, if bit 7 is set.

        Returns
        -------
        bool
            True, if bit is 1

        """
        sw = self._get_statusword()
        return self.sw.has_warning(sw)

    def is_target_reached(self) -> bool:
        """
        Checks, if bit 10 is set.

        Returns
        -------
        bool
            True, if bit is 1
        """
        sw = self._get_statusword()
        return self.sw.is_target_reached(sw)

    def is_homing_attained(self) -> bool:
        """
        Checks, if bit 12 is set. (Homing Mode)

        Returns
        -------
        bool
            True, if bit is 1
        """
        sw = self._get_statusword()
        return self.sw.is_homing_attained(sw)

    def has_homing_error(self) -> bool:
        """
        Checks, if bit 13 is set. (Homing Mode)

        Returns
        -------
        bool
            True, if bit is 1
        """
        sw = self._get_statusword()
        return self.sw.has_homing_error(sw)

    def halt(self, set_bit: bool = True):
        """
        Sets bit 8 in controlword. (Operation mode specific)

        Parameters
        ----------
        set_bit : bool
            If true, set bit, otherwise clear it

        """

        self._send_controlword(self.cw.halt(set_bit))

    def is_speed_zero(self) -> bool:
        """
        Checks, if bit 12 is set. (Profile Velocity Mode)

        Returns
        -------
        bool
            True, if bit is 1
        """
        sw = self._get_statusword()
        return self.sw.is_speed_zero(sw)

    ####################################################
    # Homing Mode
    ####################################################

    def start_homing(self, set_bit: bool = True):
        """
        Set bit 4 to start homing procedure.
        True starts homing.
        False stops homing.

        Parameters
        ----------
        set_bit : bool
            If true, set bit, otherwise clear it


        """
        self._send_controlword(self.cw.start_homing(set_bit))

    def get_homing_state(self) -> HomingState:
        """
        Return the current state of the homing procedure.

        Returns
        -------
        HomingState

        """
        if not (self.has_homing_error() or self.is_homing_attained() or self.is_target_reached()):
            return self.HOMING_STATES.HOMING_IN_PROGRESS
        elif not self.has_homing_error() and not self.is_homing_attained() and self.is_target_reached():
            return self.HOMING_STATES.HOMING_IS_INTERRUPTED
        elif not self.has_homing_error() and self.is_homing_attained() and not self.is_target_reached():
            return self.HOMING_STATES.HOMING_IS_ATTAINED
        elif not self.has_homing_error() and self.is_homing_attained() and self.is_target_reached():
            return self.HOMING_STATES.HOMING_COMPLETED
        elif self.has_homing_error() and not self.is_homing_attained() and not self.is_target_reached():
            return self.HOMING_STATES.HOMING_ERROR_SPEED_NOT_ZERO
        elif self.has_homing_error() and not self.is_homing_attained() and self.is_target_reached():
            return self.HOMING_STATES.HOMING_ERROR_SPEED_ZERO

    ####################################################
    # Profile Position Mode
    ####################################################

    def relative_position_mode(self, set_bit: bool = True):
        """
        Changes bit 6 in controlword (Profile Position Mode).
        If set_bit is True, target position shall be a relative value.
        If set_bit is False, target position shall be an absolute value.

        Parameters
        ----------
        set_bit : bool
            If true, set bit, otherwise clear it

        """

        self._send_controlword(self.cw.relative_position_mode(set_bit))

    # Bit definitions (relates to next six methods)
    # +=======+=======+========+===================================================================================+
    # | Bit 9 | Bit 5 | Bit 4  | Definition                                                                        |
    # +=======+=======+========+===================================================================================+
    # | 0     | 0     | 0 -> 1 | Positioning shall be completed (target reached) before the next one gets started. |
    # +-------+-------+--------+-----------------------------------------------------------------------------------+
    # | x     | 1     | 0 -> 1 | Next positioning shall be started immediately.                                    |
    # +-------+-------+--------+-----------------------------------------------------------------------------------+
    # | 1     | 0     | 0 -> 1 | Positioning with the current profile velocity up to the current set-point shall   |
    # |       |       |        | be proceeded and then next positioning shall be applied.                          |
    # +-------+-------+--------+-----------------------------------------------------------------------------------+

    def new_setpoint(self, set_bit: bool = True):
        """
        Changes bit 4 in controlword. (Profile Position Mode)

        Parameters
        ----------
        set_bit : bool
            If true, set bit, otherwise clear it
        """
        self._send_controlword(self.cw.new_setpoint(set_bit))

    def change_set_point_now(self, set_bit: bool = True):
        """
        Changes bit 5 in controlword. (Profile Position Mode)

        Parameters
        ----------
        set_bit : bool
            If true, set bit, otherwise clear it
        """
        self._send_controlword(self.cw.change_set_point_now(set_bit))

    def change_on_set_point(self, set_bit: bool = True):
        """
        Changes bit 9 in controlword (Profile Position Mode)

        Parameters
        ----------
        set_bit : bool
            If true, set bit, otherwise clear it

        """
        self._send_controlword(self.cw.relative_position_mode(set_bit))

    def pp_complete_next_position(self):
        """
        See first table row
        """
        self.change_set_point_now(False)
        self.change_on_set_point(False)
        self.new_setpoint()

    def pp_start_next_position_now(self):
        """
        See second table row
        """
        self.change_set_point_now()
        self.new_setpoint()

    def pp_finish_positioning_then_next(self):
        """
        See third table row
        """
        self.change_on_set_point()
        self.change_set_point_now(False)
        self.new_setpoint()

    def pp_reset_bits(self):
        """
        Reset all position profile related bits
        """
        self.change_on_set_point(False)
        self.change_set_point_now(False)
        self.new_setpoint(False)
