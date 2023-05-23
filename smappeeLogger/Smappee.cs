using System;
using System.Collections.Generic;
using System.Diagnostics.SymbolStore;
using System.IO.Ports;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Serilog;
using System.Net.Http;
using System.Text.Json;
using System.Reflection.Metadata.Ecma335;

namespace smappeeLogger
{
    internal class Smappee
    {
        bool _keepAlive = true;
        ILogger _logger;
        string _comportName = string.Empty;
        int _slaveAddress = 0;
        ModbusRtuMaster _modbus;
        Comport _comport;
        Thread _thread;
        SmappeeDto _data;
        object _lock = new object();


        public Smappee(string comport, int slaveAddress, ILogger logger)
        {
            _logger = logger;
            _slaveAddress = slaveAddress;
            _comportName = comport;
            _comport = new Comport();
            _comport._serialPort.Parity = Parity.None;
            _comport._serialPort.PortName =_comportName;
            _comport._serialPort.BaudRate = 115200;
            _comport._serialPort.StopBits = StopBits.One;
            _modbus = new ModbusRtuMaster(_comport);
              _thread = new Thread(Run);
            _thread.Start();
            _data = new SmappeeDto();
        }

        public SmappeeDto Readings { get
            {
                string s;
                lock (_lock)
                {
                    s = JsonSerializer.Serialize(_data);
                }
                return JsonSerializer.Deserialize<SmappeeDto>(s);
            }
        }

        void Run()
        {
            byte[] rx = new byte[255];
            byte[] tmp = new byte[rx.Length];

            while(_keepAlive)
            {
                try
                {
                    if(_comport.IsConnected) 
                    {
                        SmappeeDto smappeeDto = new SmappeeDto();
                        double energy;
                        float power;
                        rx = _modbus.ReadHoldingRestier(_slaveAddress, 896, 8);

                        for (int i = 0; i < 16;)
                        {
                            tmp[0] = rx[5 + i];
                            tmp[1] = rx[6 + i];
                            tmp[2] = rx[3 + i];
                            tmp[3] = rx[4 + i];
                            power = ByteArrayConvert.ToSingle(tmp, 0);
                            smappeeDto.Powers[i / 4] = power;
                            //_logger.Debug($"value: {power}");
                            i += 4;
                        }

                        rx = _modbus.ReadHoldingRestier(_slaveAddress, 12288, 16);

                        for (int i = 0; i < 32;)
                        {
                            tmp[0] = rx[5 + i];
                            tmp[1] = rx[6 + i];
                            tmp[2] = rx[3 + i];
                            tmp[3] = rx[4 + i];
                            energy = ByteArrayConvert.ToUInt32(tmp, 0) * 0.001;
                            smappeeDto.Energy[i / 8] = energy;
                            //_logger.Debug($"value:{energy}");
                            i += 8;
                        }

                        lock (_lock)
                        {
                            _data = smappeeDto;
                        }
                    }
                    else
                    {
                        try
                        {
                            _comport.Connect();
                        }
                        catch (Exception ex)
                        {
                            Thread.Sleep(1000);
                        }
                    }
                }
                catch (IndexOutOfRangeException)
                {
                    //do nothing cuz we don't care!
                }
                catch (Exception ex) 
                {
                    _logger.Error(ex.ToString());
                }
                Thread.Sleep(500);
            }
        }
    }
}
