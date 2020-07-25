"""
SOMANET Object Dictionary

Contains the numerations and helper methods for interacting with the object dictionary.

The design goal of this module is to provide a shallow layer on the OD to make
it easier to write tests. More complex behavioral things should be placed in the
SOMANET Toolbox module.

Usage
----

It is recommended to use a standard way of importing:
    ``import somanet_od as sod``
"""

import re
import logging
from typing import Any, Dict
from motion_master_wrapper import MotionMasterWrapper

logger = logging.getLogger(__name__)


class ExceptionOD(Exception):
    """
    Exception class for
    """
    pass


class _ODParser:
    """
    Parser for mmw.device_and_parameter_info_dict.
    Generates a dict with OD index as key. Value is a dict with Subindex as key and motion_master_pb2.Parameter as value.
    Attribute "name" will be used as method name.
    """
    re_od_index_subindex = re.compile(r'(\w+):(\d+)')

    def __init__(self, device_and_parameter_info_dict):
        self.mmw_od = device_and_parameter_info_dict['parameters']

    @staticmethod
    def format_od_name(attr_name: str) -> str:
        """
        Removes in methods forbidden chars from OD entry name.

        Parameters
        ----------
        attr_name : str
            Attribute name, will be the method name later.b
        Returns
        -------
        str
            Cleaned attribute name
        """

        attr_name = attr_name.replace('-', '_')
        attr_name = attr_name.replace('.', '_')
        attr_name = attr_name.replace('(', '')
        attr_name = attr_name.replace(')', '')
        attr_name = attr_name.replace(' ', '_')
        attr_name = attr_name.lower()
        return attr_name

    def parse(self) -> Dict[int, Dict[int, Any]]:  # Not actually Any, but motion_master_pb2.Parameter
        """
        Parses the device_and_parameter_info_dict.

        Returns
        -------
        Dict[int, Dict[int, Dict[int, motion_master_pb2.Parameter]]]
            Dictionary of dictionary of dictionary with OD parameters.
            First key is index and last key is subindex.

        Raises
        ------
        ExceptionOD
            If an index and subindex are already in the dictionary

        """
        od = {}

        for key, val in self.mmw_od.items():
            res = self.re_od_index_subindex.match(key)
            if not res:
                logger.warning("Could not parse index/subindex key '{}'".format(key))
                continue

            index = int(res.group(1), 16)
            subindex = int(res.group(2))

            if od.get(index) and od[index].get(subindex):
                raise ExceptionOD("Object 0x{:x}:{} already in dictionary".format(index, subindex))

            val.name = self.format_od_name(val.name)
            if val.group is not '':
                val.group = self.format_od_name(val.group)

            if not od.get(index):
                od[index] = {}

            od[index][subindex] = val
        return od


class _GetterSetter:
    """
    The callable attribute. Calls the setter and getter method in Object.
    """

    def __init__(self, parameter_method, index: int, subindex: int, name: str, ):
        """
        Initialize the GetterSetter object

        Parameters
        ----------
        parameter_method : ObjectDictionary.parameter
            MotionMasterWrapper object.
        addr : int
            Device address
        index : int
            OD entry index
        name : str
            Attribute name (for traceback, debug purposes)
        """
        self.parameter_method = parameter_method
        self.index = index
        self.subindex = subindex
        self.name = name

    def __call__(self, value: Any = None):
        """
        Implements the function call of this object.
        Example:
                    o = GetterSetter(mmw, addr, index) # __init__
                    o(3) # __call__

        Parameters
        ----------
        value : Any
            New value for OD entry. If None, method works as getter

        Returns
        -------
        Any
            Value from OD entry
        """
        return self.parameter_method(self.index, self.subindex, value)


class _GetterSetterWithSubindex:
    """
    The callable attribute. Calls the setter and getter method in Object.
    It's provide a slightly different API like the other class. You have to provide a subindex.
    Useful for iterations / loops
    """

    def __init__(self, parameter_method, index: int, name: str, ):
        """
        Initialize the GetterSetter object

        Parameters
        ----------
        parameter_method : ObjectDictionary.parameter
            MotionMasterWrapper object.
        addr : int
            Device address
        index : int
            OD entry index
        name : str
            Attribute name (for traceback, debug purposes)
        """
        self.parameter_method = parameter_method
        self.index = index
        self.name = name

    def __call__(self, subindex: int, value: Any = None):
        """
        Implements the function call of this object.
        Example:
                    o = GetterSetter(mmw, addr, index) # __init__
                    o(3) # __call__

        Parameters
        ----------
        value : Any
            New value for OD entry. If None, method works as getter

        Returns
        -------
        Any
            Value from OD entry
        """
        return self.parameter_method(self.index, subindex, value)


class ObjectDictionary:
    """
    This class generates with GetterSetter for every OD entry a method with the name of the entry.
    Instead calling mmw.get_device_parameter(device_address, 0x606C) you can simple call od.velocity_actual_value().
    If the entry has multiple subindexes, you can call: od.biss_encoder_1(7) for clock frequency.
    To change a value call: od.biss_encoder_1(7, 4e6)
    """

    def __init__(self, mmw: MotionMasterWrapper, device_address: int):
        """
        Initializer of ObjectDictionary.

        Parameters
        ----------
        mmw : MotionMasterWrapper
            MotionMasterWrapper object
        device_address : int
            Device address of connected node.

        Raises
        ------
        ExceptionOD
            If a name is already in the attribute list
        """
        self.mmw = mmw
        self.addr = device_address
        parser = _ODParser(self.mmw.device_and_parameter_info_dict[str(self.addr)])
        od = parser.parse()
        state_save_sub_index_group = None
        for index, subindexes in od.items():
            # if dict sub indexes is empty, continue
            for si, entry in subindexes.items():
                # If group name is not empty, take the group name. This is important for objects entries with sub indexes.
                if entry.name == 'subindex_000' and si == 0:
                    state_save_sub_index_group = entry.group
                    # Save the first entry also
                    attr_name = '{}_subindex'.format(entry.group)
                    self._add_method_with_subindex_argument(entry.group, index, entry)
                elif state_save_sub_index_group == entry.group:
                    # Save all other subindexes
                    attr_name = '{}_{}'.format(entry.group, entry.name)
                else:
                    # Save entry without subindex
                    attr_name = entry.name
                    state_save_sub_index_group = None

                self._add_method(attr_name, index, si, entry)

    @property
    def device_address(self):
        """
        Get device address
        """
        return self.addr

    def parameter(self, index: int, subindex: int, value=None):
        """
        The actual getter/setter function. Can also be uses in loops.

        Parameters
        ----------
        index : int
            OD index
        subindex : int
            OD subindex
        value : Any
            Value to add in od entry.

        Returns
        -------
        Any
        """
        return_value = None
        if value is not None:
            self.mmw.set_device_parameter_value(self.addr, index, subindex, value)
        else:
            return_value = self.mmw.get_device_parameter_value(self.addr, index, subindex)
        return return_value

    def _add_method(self, function_name: str, index: int, subindex: int, entry):
        """
        Add a new attribute to this object with name "function_name"

        Parameters
        ----------
        function_name : str
            New attribute name
        index : int
            The object dictionary index
        subindex : int
            The object dictionary sub index
        entry : object
            The parameter entry of the mmw. (for debugging)

        """
        if function_name in dir(self):
            raise ExceptionOD("Method {} already an attribute. {}".format(function_name, entry))
        setattr(self, function_name, _GetterSetter(self.parameter, index, subindex, function_name))

    def _add_method_with_subindex_argument(self, function_name: str, index: int, entry):
        """
        Add a new attribute to this object with name "function_name". But you have to provide the subindex.

        Parameters
        ----------
        function_name : str
            New attribute name
        index : int
            The object dictionary index
        entry : object
            The parameter entry of the mmw. (for debugging)

        """
        if function_name in dir(self):
            raise ExceptionOD("Method {} already an attribute. {}".format(function_name, entry))
        setattr(self, function_name, _GetterSetterWithSubindex(self.parameter, index, function_name))
