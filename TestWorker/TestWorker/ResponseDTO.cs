using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestWorker
{
   public class ResponseDTO
   {
      public string WorkerId { get; set; }
      public string ServerName { get; set; }

      public ResponseDTO(string workerId, string serverName)
      {
         WorkerId = workerId;
         ServerName = serverName;
      }
   }
}
