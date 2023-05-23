using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace smappeeLogger
{

    internal class SmappeeDto
    {
        public float[] Powers { get; set; }
        public double[] Energy { get; set; }

        public SmappeeDto()
        {
            Powers = new float[4];
            Energy = new double[4];
        }
    }
}
