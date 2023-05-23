using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace smappeeLogger
{
    public class ResponseDTO
    {
        public string ClientId { get; set; }
        public string ServerName { get; set; }
        public string DataServerName { get; set; }

        public ResponseDTO(string clientId, string serverName, string dataServerName)
        {
            ClientId = clientId;
            ServerName = serverName;
            DataServerName = dataServerName;
        }
    }
}
