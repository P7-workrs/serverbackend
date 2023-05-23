using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace smappeeLogger
{
    internal class ModbusRtuMaster
    {
        Comport _driver;
        byte[] tx = new byte[8];
        public ModbusRtuMaster(Comport driver)
        {
            _driver = driver;
        }

        public byte[] ReadHoldingRestier(int address, int startRegister, int registerCnt)
        {
            int retVal = -1;
            int replyBytes = 0;
            //Thread.Sleep(5);//TODO TEST JSM der var problemer med at læse 2 ir øje i loop

            
            tx[0] = (byte)address;
            tx[1] = 3;
            tx[2] = (byte)(startRegister >> 8);
            tx[3] = (byte)startRegister;
            tx[4] = (byte)(registerCnt >> 8);
            tx[5] = (byte)registerCnt;
            CrcModbus.InsertCrc(tx, 6);

            byte[] rx = _driver.WriteAndReadResponse(tx, tx.Length);
            if (rx.Length <= 0) throw new Exception("Zero bytes recieved");

            return rx;
        }

    }
    public static class CrcModbus
    {
        public static void InsertCrc(byte[] buf, int len)
        {
            byte lo;
            byte hi;
            ushort crc = CalculateCrc(buf, len);
            lo = (byte)(crc);
            hi = (byte)(crc >> 8);
            buf[len + 0] = hi;
            buf[len + 1] = lo;
        }

        public static bool CheckCrc(byte[] buffer, int len)
        {
            bool retVal = false;
            byte lo;
            byte hi;
            ushort crc = CalculateCrc(buffer, len - 2);

            lo = (byte)crc;
            hi = (byte)(crc >> 8);
            if (lo == buffer[len - 1] && hi == buffer[len - 2])
            {
                retVal = true;
            }

            return retVal;
        }

        private static ushort CalculateCrc(byte[] buf, int len)
        {
            ushort crc16;
            ushort u16;
            crc16 = 0xffff;             //init the CRC WORD 
            for (int pos = 0; pos < len; pos++)
            {
                u16 = buf[pos];       //temp has the first byte 
                u16 &= 0x00FF;        //mask the MSB 
                crc16 = (ushort)(crc16 ^ u16);      //crc16 XOR with temp 
                for (int c = 0; c < 8; c++)         // Loop over each bit
                {
                    if ((crc16 & 0x0001) != 0)              //If the LSB is set
                    {
                        crc16 = (ushort)(crc16 >> 1);
                        crc16 = (ushort)(crc16 ^ 0x0a001);  //Shift right and XOR 0xA001
                    }
                    else                                    //Else LSB is not set
                    {
                        crc16 = (ushort)(crc16 >> 1);       //Just shift right
                    }
                }
            }
            crc16 = (ushort)((crc16 >> 8) | (crc16 << 8));  // LSB is exchanged with MSB 
            return crc16;
        }
    }
}
