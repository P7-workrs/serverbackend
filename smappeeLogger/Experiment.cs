using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace smappeeLogger
{
    public enum Status
    {
        waiting,
        running,
        done
    }

    public class Experiment
    {
        public double Mu { get; private set; }
        public int Interval { get; private set; }
        public int NumberOfCheckpoints { get; private set; }
        public int Count { get; set; }
        public Status Status { get; set; }
        private int _baseSeed;

        public Experiment(double mu, int numberOfCheckpoints, int baseSeed, double time)
        {
            Mu = mu;
            NumberOfCheckpoints = numberOfCheckpoints;
            Interval = numberOfCheckpoints == 0 ? 999999999 : (int)Math.Floor(time / (numberOfCheckpoints + 1) * 1000);
            Count = 0;
            Status = Status.waiting;
            _baseSeed = baseSeed;
        }
        public int GetSeed()
        {
            return _baseSeed + Count;
        }
    }
}
