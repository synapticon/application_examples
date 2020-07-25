
import numpy as np
import math
import time
import logging
logger = logging.getLogger(__name__)


class TrajectoryReferenceVelocity:

    def __init__(self, acceleration, deceleration, si_unit_scaling_factor, ts=1):
        """ Initialize the velocity reference profiler

        Parameters
        ----------
        acceleration : int
            Acceleration value in in user defined units and in velocity control frame
        deceleration : int
            Deceleration value in in user defined units and in velocity control frame
        si_unit_scaling_factor : int
            Multiplier (x10 , x100, x1000) according to the configured SI Velocity Unit
        ts : float
            Sampling time
        """
        self.t = 0
        self.ts = ts
        self.current_velocity = 0
        self.target_velocities = []
        self.flat_part_duration = 0
        # Convert to the user defined unit
        self.acceleration = acceleration
        self.deceleration = deceleration
        self.si_unit_scaling_factor = si_unit_scaling_factor
        self.direction_multiplier = 1
        self.tarray = []
        self.varray = []
        self.delayValue = 0

    def set_traj_parameters(self, start_time, start_velocity, target_velocity, flat_part_duration=0):
        """Set the parameters to generate acceleration, deceleration or flat trajectory
        To generate a flat trajectory, set both start_velocity and target_velocity equal to the velocity at which
         trajectory needs to be generated.

        Parameters
        ----------
        start_time : float
            Value in seconds at which trajectory is supposed to start
        start_velocity : int
            Value of velocity in user defined units and in velocity control frame
        target_velocity : int
            Value of velocity in user defined units and in velocity control frame
        flat_part_duration : float, optional
            Duration in seconds, determines the length of flat trajectory at the target velocity

        Returns
        -------
            Nothing
        """
        self.t = start_time
        # Convert to the user defined unit
        self.current_velocity = start_velocity
        self.flat_part_duration = flat_part_duration

        if target_velocity == start_velocity:
            self.delayValue = self.t + flat_part_duration
            self.target_velocities.append(target_velocity)
        elif (start_velocity >= 0 and target_velocity >= 0) or (start_velocity <= 0 and target_velocity <= 0):
            self.target_velocities.append(target_velocity)
        else:
            # Target Velocity have different sign from start velocity
            # Split Trajectory in two (first : current velocity to  0 (decel) ; second : 0 to target (acel))
            self.target_velocities.append(0)
            self.target_velocities.append(target_velocity)

        if target_velocity > start_velocity and self.acceleration == 0:
            raise Exception("Acceleration value can't be zero to generate acceleration profile")

        if target_velocity < start_velocity and self.deceleration == 0:
            raise Exception("Deceleration value can't be zero to generate deceleration profile")

    def clear_values(self):
        """ Clear the time and velocity values of the generated trajectory

        Returns
        -------
            Nothing
        """
        self.varray = []
        self.tarray = []
        self.delayValue = 0
        self.target_velocities = []
        self.current_velocity = 0
        self.t = 0

    def run(self):
        """
        Generates the Trajectory values for velocity and time

        Returns
        -------
            Nothing, But populates the 'self.tarray' and 'self.varray' with time and velocity values

        """
        for target_velocity in self.target_velocities:

            # Remove the previous appended values in case of zer crossings to avoid double appending of same value
            if self.target_velocities.index(target_velocity) == 1:
                self.tarray.pop()
                self.varray.pop()
                self.t = self.t - self.ts

            while True:
                #  Determine direction based on current and target velocities
                if self.current_velocity >= 0 and target_velocity >= 0:
                    self.direction_multiplier = 1
                elif self.current_velocity <= 0 and target_velocity <= 0:
                    self.direction_multiplier = -1

                # append current values in respective lists
                self.tarray.append(self.t)
                self.varray.append(self.current_velocity)
                # Increase Time
                self.t = self.t + self.ts

                # Determine to accelerate and decelerate
                sig = np.sign(abs(target_velocity) - abs(self.current_velocity))
                if sig > 0:     # accelerate

                    if np.isclose(abs(self.current_velocity),
                                  abs(target_velocity), atol=1 * self.ts * self.acceleration):
                        self.current_velocity = target_velocity

                        # Set delay value for flat part
                        self.delayValue = self.t + self.flat_part_duration
                    else:
                        self.current_velocity = self.current_velocity + self.direction_multiplier * self.acceleration\
                                                * self.ts

                elif sig < 0:      # decelerate

                    if np.isclose(abs(self.current_velocity),
                                  abs(target_velocity), atol=1 * self.ts * self.deceleration):

                        self.current_velocity = target_velocity

                        # Set delay value for flat part
                        self.delayValue = self.t + self.flat_part_duration
                    else:
                        self.current_velocity = self.current_velocity - self.direction_multiplier * self.deceleration\
                                                * self.ts

                else:           # Flat Part after reaching the last target in target_velocities list
                    if self.target_velocities.index(target_velocity) == len(self.target_velocities)-1:
                        if self.t > self.delayValue:
                            self.tarray.append(self.t)
                            self.varray.append(self.current_velocity)
                            break
                    else:
                        break


class TrajectoryReferencePosition:

    def __init__(self, acceleration, deceleration, profile_max_velocity, resolution, si_unit_factor,
                 gear_ratio_factor, ts=1):
        """ Initialize the reference profiler
        The reference profiler, generates the reference position trajectory (ramp).
        The generated velocity and the position profiles are on the frame where position encoder is mounted.

        Parameters
        ----------
        acceleration : int
            acceleration value in motor shaft frame and in rpm/s
        deceleration : int
            deceleration value in motor shaft frame and in rpm/s
        profile_max_velocity : int
            Profile Velocity value in motor shaft frame and in rpm/s
        resolution : int
            Resolution of the position encoder
        si_unit_factor : int
            Multiplier (x10 , x100, x1000) according to the configured SI Velocity Unit
        gear_ratio_factor : int
            Value of gear box ratio if position sensor is on drive shaft else 1
        ts : float
            sampling time
        """
        self.t = 0
        self.ts = ts
        self.position_k_1 = 0
        self.position_k_2 = 0
        self.target_position = 0
        self.velocity_k_1 = 0
        self.acceleration = int(acceleration / gear_ratio_factor) * (resolution / 60)
        self.deceleration = int(deceleration / gear_ratio_factor) * (resolution / 60)
        self.profile_max_velocity = int(profile_max_velocity / gear_ratio_factor) * (resolution / 60)
        self.time_array = []
        self.pos_array = []
        self.velocity_array = []
        self.delayValue = 0
        self.halt_active_time = 0
        self.halt_clear_time = 0
        self.resolution = resolution
        self.si_unit_factor = si_unit_factor
        self.gear_ratio_factor = gear_ratio_factor
        self.start_time = 0
        self.start_point = 0

    def set_traj_parameters(self, start_time, start_position, position_k_1, position_k_2, target_position,
                            halt_active_time=float('inf'), halt_clear_time=float('inf')):
        """ Set the parameters to generate the reference position profile trajectory
        Parameters
        ----------
        start_time : float
            Value in seconds at which trajectory is supposed to start
        start_position : int
            value of position in ticks from where trajectory starts
        position_k_1 : int
            Position value, one step before the point where trajectory starts
        position_k_2 : int
            Position value, two steps before the point where trajectory starts
        target_position : int
            Value of position in ticks where trajectory ends
        halt_active_time : float
            Time value in seconds, at which halt bit is set
        halt_clear_time : float
            Time value in seconds, at which halt bit is cleared
        Returns
        -------
        """

        self.start_time = start_time
        self.position_k_1 = position_k_1
        self.target_position = target_position
        self.halt_active_time = halt_active_time
        self.halt_clear_time = halt_clear_time
        self.position_k_2 = position_k_2
        self.start_point = start_position

    def clear_values(self):
        """ Clear the time and velocity values of the generated trajectory
        Returns
        -------
            Nothing
        """
        self.pos_array = []
        self.time_array = []
        self.velocity_array = []
        self.target_position = 0
        self.position_k_1 = 0
        self.velocity_k_1 = 0
        self.position_k_2 = 0
        self.velocity_array = []
        self.t = 0

    def run(self):
        """Generates the Trajectory values for position, velocity and time
        Returns
        -------
            Nothing, But populates the 'self.time_array', 'self.position_array' and 'self.velocity_array'
             with time, position and velocity values respectively.

        """
        self.t = self.start_time
        halt_bit_active = False
        sign = np.sign(self.target_position - self.position_k_1)
        while True:

            if abs(self.position_k_1) >= abs(self.target_position):
                break

            self.velocity_k_1 = (self.position_k_1 - self.position_k_2) / self.ts
            if abs(self.velocity_k_1) >= self.profile_max_velocity:
                self.velocity_k_1 = sign * self.profile_max_velocity

            if not math.isinf(self.halt_active_time) and np.isclose(self.t, self.halt_active_time, atol=0.5*self.ts)\
                    and halt_bit_active is False:
                halt_bit_active = True

            if halt_bit_active is True:

                if np.isclose(self.velocity_k_1, 0, atol=self.ts * self.deceleration):

                    if math.isinf(self.halt_clear_time):
                        break
                    else:
                        self.velocity_k_1 = 0

                else:
                    new_position = sign * self.ts * self.ts * -1 * self.deceleration + (
                            2 * self.position_k_1 - self.position_k_2)

            if not math.isinf(self.halt_clear_time) and np.isclose(self.t, self.halt_clear_time, atol=0.5*self.ts) and \
                    halt_bit_active is True:
                halt_bit_active = False

            if halt_bit_active is False:

                ticks_covered_during_dec = self.velocity_k_1 * self.velocity_k_1 / (2 * self.deceleration)
                dec_point = self.target_position - sign * ticks_covered_during_dec

                if abs(self.position_k_1) <= abs(dec_point):

                    if abs(self.velocity_k_1) < self.profile_max_velocity:  # acceleration part

                        new_position = sign * self.ts * self.ts * self.acceleration + (2 * self.position_k_1 -
                                                                                       self.position_k_2)

                    else:
                        new_position = self.position_k_1 + sign * self.profile_max_velocity * self.ts
                else:

                    new_position = sign * self.ts * self.ts * -1 * self.deceleration + (
                                2 * self.position_k_1 - self.position_k_2)

            # Don't append values in ref arrays until, reference trajectory gets closed to start point
            if abs(self.position_k_1) > (abs(self.start_point) - self.acceleration * self.ts * self.ts):
                self.time_array.append(self.t)
                self.pos_array.append(new_position)
                self.velocity_array.append(self.velocity_k_1)
                self.t = self.t + self.ts

            else:
                self.t = self.start_time
            self.position_k_2 = self.position_k_1
            self.position_k_1 = new_position

        # Rescaling velocity_array for better visualization
        self.velocity_array = [x * 60 * sign / self.resolution for x in self.velocity_array]  # Converting back to RPM
        self.time_array = [x * 1000 for x in self.time_array]  # Converting Time to ms

        return True