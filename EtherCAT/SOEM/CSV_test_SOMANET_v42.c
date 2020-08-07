/** \file
 * \brief Example code for Simple Open EtherCAT master with Synapticon SOMANET servo drive
 *
 * Usage : CSV_test_SOMANET_v42 [ifname1]
 * ifname is NIC interface, f.e. eth0
 *
 * This is a minimal test, programmed based on the simple_test of SOEM, driving a motor with SOMANET (v4.2 firmware) in CSV mode at 100RPM.
 *
 * Chencheng Tang 2019
 */

#include <stdio.h>
#include <string.h>
#include <inttypes.h>

#include "ethercat.h"

#define EC_TIMEOUTMON 500

char IOmap[4096];
OSAL_THREAD_HANDLE thread1;
int expectedWKC;
boolean needlf;
volatile int wkc;
boolean inOP;
uint8 currentgroup = 0;

/* define pointer structure */
typedef struct PACKED
{
  int16 Statusword;
  int8  OpModeDisplay;
  int32 PositionValue;
  int32 VelocityValue;
  int16 TorqueValue;
  int32 SecPositionValue;
  int32 SecVelocityValue;
  int16 AnalogInput1;
  int16 AnalogInput2;
  int16 AnalogInput3;
  int16 AnalogInput4;
  int32 TuningStatus;
  int8  DigitalInput1;
  int8  DigitalInput2;
  int8  DigitalInput3;
  int8  DigitalInput4;
  int32 UserMISO;
  int32 Timestamp;
  int32 PositionDemandInternalValue;
  int32 VelocityDemandValue;
  int16 TorqueDemand;
} in_somanet_42t;

typedef struct PACKED
{
  int16 Controlword;
  int8  OpMode;
  int16 TargetTorque;
  int32 TargetPosition;
  int32 TargetVelocity;
  int16 TorqueOffset;
  int32 TuningCommand;
  int8  DigitalOutput1;
  int8  DigitalOutput2;
  int8  DigitalOutput3;
  int8  DigitalOutput4;
  int32 UserMOSI;
  int32 VelocityOffset;
} out_somanet_42t;


void simpletest(char *ifname)
{
    int i, j, chk;
    needlf = FALSE;
    inOP = FALSE;

   printf("Starting simple test\n");

   /* initialise SOEM, bind socket to ifname */
   if (ec_init(ifname))
   {
      printf("ec_init on %s succeeded.\n",ifname);
      /* find and auto-config slaves */


       if ( ec_config_init(FALSE) > 0 )
      {
         printf("%d slaves found and configured.\n",ec_slavecount);

         ec_config_map(&IOmap);

         ec_configdc();

         printf("Slaves mapped, state to SAFE_OP.\n");
         /* wait for all slaves to reach SAFE_OP state */
         ec_statecheck(0, EC_STATE_SAFE_OP,  EC_TIMEOUTSTATE * 4);

         printf("segments : %d : %d %d %d %d\n",ec_group[0].nsegments ,ec_group[0].IOsegment[0],ec_group[0].IOsegment[1],ec_group[0].IOsegment[2],ec_group[0].IOsegment[3]);

         printf("Request operational state for all slaves\n");
         expectedWKC = (ec_group[0].outputsWKC * 2) + ec_group[0].inputsWKC;
         printf("Calculated workcounter %d\n", expectedWKC);
         ec_slave[0].state = EC_STATE_OPERATIONAL;
         /* send one valid process data to make outputs in slaves happy*/
         ec_send_processdata();
         ec_receive_processdata(EC_TIMEOUTRET);
         /* request OP state for all slaves */
         ec_writestate(0);
         chk = 200;
         /* wait for all slaves to reach OP state */
         do
         {
            ec_send_processdata();
            ec_receive_processdata(EC_TIMEOUTRET);
            ec_statecheck(0, EC_STATE_OPERATIONAL, 50000);
         }
         while (chk-- && (ec_slave[0].state != EC_STATE_OPERATIONAL));
         if (ec_slave[0].state == EC_STATE_OPERATIONAL )
         {
            printf("Operational state reached for all slaves.\n");
            inOP = TRUE;

            // initialize counter j
            j = 0;
                /* create and connect struture pointers to I/O */
            in_somanet_42t* in_somanet_1;
            in_somanet_1 = (in_somanet_42t*) ec_slave[0].inputs;
            out_somanet_42t* out_somanet_1;
            out_somanet_1 = (out_somanet_42t*) ec_slave[0].outputs;

                /* cyclic loop */
            for(i = 1; i <= 10000; i++)
            { 
               ec_send_processdata();
               wkc = ec_receive_processdata(EC_TIMEOUTRET);

                   if(wkc >= expectedWKC)
                    {
                        j++;

                          /* Example code simplest */
                        /*
                        if (j == 100) out_somanet_1->OpMode = 9; // Set Opmode, 9 = CSV
                        if (j == 200) out_somanet_1->Controlword = -128; // Fault reset: Fault -> Swith on disabled, if the drive is in fault state
                        if (j == 300) out_somanet_1->Controlword = 0; // Resetting the Controlword, can be skipped theoretically
                        if (j == 400) out_somanet_1->Controlword = 6; // Shutdown: Switch on disabled -> Ready to switch on
                        if (j == 500) out_somanet_1->Controlword = 7; // Switch on: Ready to switch on -> Switched on
                        if (j == 600) out_somanet_1->Controlword = 15; // Enable operation: Switched on -> Operation enabled
                        if (j >= 1000) out_somanet_1->TargetVelocity = 100; // Sending velocity command
                        */
                        
                           /* Example code more proper for the same functioning, for refernce*/
                        
                           // Set Opmode, 9 = CSV
                        if (j == 1) out_somanet_1->OpMode = 9;

                           // Fault reset: Fault -> Swith on disabled, if the drive is in fault state
                        if ((in_somanet_1->Statusword & 0b0000000001001111) == 0b0000000000001000)
			                  out_somanet_1->Controlword = 0b10000000;

                           // Shutdown: Switch on disabled -> Ready to switch on
		                  else if ((in_somanet_1->Statusword & 0b0000000001001111) == 0b0000000001000000)
			                  out_somanet_1->Controlword = 0b00000110;

                           // Switch on: Ready to switch on -> Switched on
		                  else if ((in_somanet_1->Statusword & 0b0000000001101111) == 0b0000000000100001)
			                  out_somanet_1->Controlword = 0b00000111;
                           
                           // Enable operation: Switched on -> Operation enabled
                        else if ((in_somanet_1->Statusword & 0b0000000001101111) == 0b0000000000100011)
			                  out_somanet_1->Controlword = 0b00001111;
                           
                           // Sending velocity command
                        else if ((in_somanet_1->Statusword & 0b0000000001101111) == 0b0000000000100111)
			                  out_somanet_1->TargetVelocity = 100;
                        

                        printf("Processdata cycle %4d , WKC %d ,", i, wkc);
                          // print statusword
                        printf(" Statusword: %X ,", in_somanet_1->Statusword);
                          // print opmode display
                        printf(" Op Mode Display: %d ,", in_somanet_1->OpModeDisplay);
                          // print actual position value
                        printf(" ActualPos: %" PRId32 " ,", in_somanet_1->PositionValue);
                          // print actual velocity value
                        printf(" ActualVel: %" PRId32 " ,", in_somanet_1->VelocityValue);
                          // print demand velocity value
                        printf(" DemandVel: %" PRId32 " ,", in_somanet_1->VelocityDemandValue);

                        printf(" T:%" PRId64 "\r",ec_DCtime);
                        needlf = TRUE;
                    }
                    osal_usleep(5000);
                }
                inOP = FALSE;
            }
            else
            {
                printf("Not all slaves reached operational state.\n");
                ec_readstate();
                for(i = 1; i<=ec_slavecount ; i++)
                {
                    if(ec_slave[i].state != EC_STATE_OPERATIONAL)
                    {
                        printf("Slave %d State=0x%2.2x StatusCode=0x%4.4x : %s\n",
                            i, ec_slave[i].state, ec_slave[i].ALstatuscode, ec_ALstatuscode2string(ec_slave[i].ALstatuscode));
                    }
                }
            }
            printf("\nRequest init state for all slaves\n");
            ec_slave[0].state = EC_STATE_INIT;
            /* request INIT state for all slaves */
            ec_writestate(0);
        }
        else
        {
            printf("No slaves found!\n");
        }
        printf("End simple test, close socket\n");
        /* stop SOEM, close socket */
        ec_close();
    }
    else
    {
        printf("No socket connection on %s\nExcecute as root\n",ifname);
    }
}

OSAL_THREAD_FUNC ecatcheck( void *ptr )
{
    int slave;
    (void)ptr;                  /* Not used */

    while(1)
    {
        if( inOP && ((wkc < expectedWKC) || ec_group[currentgroup].docheckstate))
        {
            if (needlf)
            {
               needlf = FALSE;
               printf("\n");
            }
            /* one ore more slaves are not responding */
            ec_group[currentgroup].docheckstate = FALSE;
            ec_readstate();
            for (slave = 1; slave <= ec_slavecount; slave++)
            {
               if ((ec_slave[slave].group == currentgroup) && (ec_slave[slave].state != EC_STATE_OPERATIONAL))
               {
                  ec_group[currentgroup].docheckstate = TRUE;
                  if (ec_slave[slave].state == (EC_STATE_SAFE_OP + EC_STATE_ERROR))
                  {
                     printf("ERROR : slave %d is in SAFE_OP + ERROR, attempting ack.\n", slave);
                     ec_slave[slave].state = (EC_STATE_SAFE_OP + EC_STATE_ACK);
                     ec_writestate(slave);
                  }
                  else if(ec_slave[slave].state == EC_STATE_SAFE_OP)
                  {
                     printf("WARNING : slave %d is in SAFE_OP, change to OPERATIONAL.\n", slave);
                     ec_slave[slave].state = EC_STATE_OPERATIONAL;
                     ec_writestate(slave);
                  }
                  else if(ec_slave[slave].state > EC_STATE_NONE)
                  {
                     if (ec_reconfig_slave(slave, EC_TIMEOUTMON))
                     {
                        ec_slave[slave].islost = FALSE;
                        printf("MESSAGE : slave %d reconfigured\n",slave);
                     }
                  }
                  else if(!ec_slave[slave].islost)
                  {
                     /* re-check state */
                     ec_statecheck(slave, EC_STATE_OPERATIONAL, EC_TIMEOUTRET);
                     if (ec_slave[slave].state == EC_STATE_NONE)
                     {
                        ec_slave[slave].islost = TRUE;
                        printf("ERROR : slave %d lost\n",slave);
                     }
                  }
               }
               if (ec_slave[slave].islost)
               {
                  if(ec_slave[slave].state == EC_STATE_NONE)
                  {
                     if (ec_recover_slave(slave, EC_TIMEOUTMON))
                     {
                        ec_slave[slave].islost = FALSE;
                        printf("MESSAGE : slave %d recovered\n",slave);
                     }
                  }
                  else
                  {
                     ec_slave[slave].islost = FALSE;
                     printf("MESSAGE : slave %d found\n",slave);
                  }
               }
            }
            if(!ec_group[currentgroup].docheckstate)
               printf("OK : all slaves resumed OPERATIONAL.\n");
        }
        osal_usleep(10000);
    }
}

int main(int argc, char *argv[])
{
   printf("SOEM (Simple Open EtherCAT Master)\nSimple test\n");

   if (argc > 1)
   {
      /* create thread to handle slave error handling in OP */
//      pthread_create( &thread1, NULL, (void *) &ecatcheck, (void*) &ctime);
      osal_thread_create(&thread1, 128000, &ecatcheck, (void*) &ctime);
      /* start cyclic part */
      simpletest(argv[1]);
   }
   else
   {
      printf("Usage: simple_test ifname1\nifname = eth0 for example\n");
   }

   printf("End program\n");
   return (0);
}
