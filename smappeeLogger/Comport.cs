using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO.Ports;

namespace smappeeLogger
{
    internal class Comport
    {
        public SerialPort _serialPort = new SerialPort();
        public bool IsConnected { get { return _serialPort.IsOpen; } }

        public void Connect()
        {
            _serialPort.Open();
        }

        public byte[] WriteAndReadResponse(byte[] dataToWrite, int count)
        {
            byte[] retData = new byte[0];
            if (!_serialPort.IsOpen)
                _serialPort.Open();
            
            if (_serialPort.IsOpen)
            {
                _serialPort.DiscardInBuffer();
                _serialPort.DiscardOutBuffer();
                _serialPort.Write(dataToWrite, 0, count);
                int waitCnt = 100;
                while (_serialPort.BytesToRead == 0 && waitCnt-- > 0) Thread.Sleep(1);

                Thread.Sleep(10);
                if (_serialPort.BytesToRead > 0)
                {
                    retData = new byte[_serialPort.BytesToRead];
                    _serialPort.Read(retData, 0, retData.Length);
                }
            }
            
            return retData;
        }

    }
}
