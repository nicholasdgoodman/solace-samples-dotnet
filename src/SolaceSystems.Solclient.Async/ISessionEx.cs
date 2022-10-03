using System;
using System.Threading.Tasks;
using SolaceSystems.Solclient.Messaging;

namespace SolaceSystems.Solclient.Async
{
    public interface ISessionEx : IDisposable
    {
        Task<ReturnCode> ConnectAsync();
        Task<ReturnCode> SendAsync(IMessage message);

        Task<ReturnCode> SubscribeAsync(ISubscription subscription);
        event EventHandler<MessageEventArgs> MessageReceived;
    }
}
