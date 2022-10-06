using System;
using System.Threading.Tasks;
using SolaceSystems.Solclient.Messaging;

namespace Tutorial.Common
{
    public interface IClient : IDisposable
    {
        ISession Session { get; }
        Task<ReturnCode> ConnectAsync();
        Task<ReturnCode> SendAsync(IMessage message);

        Task<ReturnCode> SubscribeAsync(ISubscription subscription);

        event EventHandler<MessageEventArgs> MessageReceived;
    }
}
