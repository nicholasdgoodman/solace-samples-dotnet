using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Tutorial.Common;


namespace Tutorial
{
    class Program
    {
        static void Main(string[] args)
        {
            if (CommandLine.TryLoadConfig(args, out var config))
            {
                var queueConsumer = new QueueConsumer(config.Host, config.Vpn, config.UserName, config.Password);
                queueConsumer.Run(CancellationToken.None);
            }
        }
    }
}
