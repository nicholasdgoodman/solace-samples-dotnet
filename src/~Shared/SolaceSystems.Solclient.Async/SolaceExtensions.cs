using System;
using System.Threading.Tasks;
using SolaceSystems.Solclient.Messaging;

namespace SolaceSystems.Solclient.Async
{
    public static class SolaceExtensions
    {
        public static ISessionEx CreateSessionEx(this IContext context, SessionProperties properties)
        {
            return new SessionEx(context, properties);
        }

        private class SessionEx : ISessionEx
        {
            ISession session;
            TaskCompletionSource<ReturnCode> connectTaskCompletion;

            public SessionEx(IContext context, SessionProperties properties)
            {
                this.session = context.CreateSession(properties, MessageEventHandler, SessionEventHandler);
            }

            public event EventHandler<MessageEventArgs> MessageReceived;

            public Task<ReturnCode> ConnectAsync()
            {
                try
                {
                    this.connectTaskCompletion = new TaskCompletionSource<ReturnCode>();
                    var connectResult = this.session.Connect();

                    if(this.session.Properties.ConnectBlocking && !this.connectTaskCompletion.Task.IsCompleted)
                    {
                        this.connectTaskCompletion.SetResult(connectResult);
                    }
                }
                catch (Exception ex)
                {
                    this.connectTaskCompletion.SetException(ex);
                }

                return this.connectTaskCompletion.Task;
            }

            public Task<ReturnCode> SubscribeAsync(ISubscription subscription)
            {
                // TODO: Make this actually Async
                var result = this.session.Subscribe(subscription, true);
                return Task.FromResult(result);
            }

            public Task<ReturnCode> SendAsync(IMessage message)
            {
                var sendTaskCompletion = new TaskCompletionSource<ReturnCode>();
                try
                {
                    message.CorrelationKey = sendTaskCompletion;
                    var sendResult = this.session.Send(message);

                    if (this.session.Properties.SendBlocking || message.DeliveryMode == MessageDeliveryMode.Direct && !sendTaskCompletion.Task.IsCompleted)
                    {
                        sendTaskCompletion.SetResult(sendResult);
                    }
                }
                catch (Exception ex)
                {
                    sendTaskCompletion.SetException(ex);
                }

                return sendTaskCompletion.Task;
            }

            private void SessionEventHandler(object sender, SessionEventArgs e)
            {
                switch (e.Event)
                {
                    case SessionEvent.UpNotice:
                        this.connectTaskCompletion?.SetResult(ReturnCode.SOLCLIENT_OK);
                        break;
                    case SessionEvent.ConnectFailedError:
                        this.connectTaskCompletion?.SetResult(ReturnCode.SOLCLIENT_FAIL);
                        break;
                    case SessionEvent.Acknowledgement:
                        var sendTaskCompletion = e.CorrelationKey as TaskCompletionSource<ReturnCode>;
                        sendTaskCompletion?.SetResult(ReturnCode.SOLCLIENT_OK);
                        break;
                    case SessionEvent.RejectedMessageError:
                        var sendTaskRejected = e.CorrelationKey as TaskCompletionSource<ReturnCode>;
                        sendTaskRejected?.SetResult(ReturnCode.SOLCLIENT_FAIL);
                        break;
                }
            }

            private void MessageEventHandler(object sender, MessageEventArgs e)
            {
                // This could be automatically dispatched on a thread pool thread to prevent deadlocks
                this.MessageReceived?.Invoke(this, e);
            }

            public void Dispose()
            {
                session?.Dispose();
            }
        }
    }
}
