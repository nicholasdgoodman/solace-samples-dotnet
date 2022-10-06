using System;
using System.Threading.Tasks;
using SolaceSystems.Solclient.Messaging;

namespace Tutorial.Common
{
    public static class SolaceExtensions
    {
        public static IClient CreateClient(this IContext context, SessionProperties properties)
        {
            return new Client(context, properties);
        }

        private class Client : IClient
        {
            TaskCompletionSource<ReturnCode> connectTaskCompletion;

            public Client(IContext context, SessionProperties properties)
            {
                this.Session = context.CreateSession(properties, MessageEventHandler, SessionEventHandler);
            }

            public ISession Session { get; private set; }

            public event EventHandler<MessageEventArgs> MessageReceived;

            public Task<ReturnCode> ConnectAsync()
            {
                try
                {
                    this.connectTaskCompletion = new TaskCompletionSource<ReturnCode>(TaskCreationOptions.RunContinuationsAsynchronously);
                    var connectResult = this.Session.Connect();

                    if(this.Session.Properties.ConnectBlocking && !this.connectTaskCompletion.Task.IsCompleted)
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
                var tsc = new TaskCompletionSource<ReturnCode>(TaskCreationOptions.RunContinuationsAsynchronously);
                try
                {
                    var target = this.Session.CreateDispatchTarget(subscription, MessageEventHandler);
                    var waitForConfirm = !this.Session.Properties.TopicDispatch;
                    var result = this.Session.Properties.TopicDispatch ?
                        this.Session.Subscribe(target, SubscribeFlag.RequestConfirm, tsc) :
                        this.Session.Subscribe(subscription, waitForConfirm);

                    if ((waitForConfirm || this.Session.Properties.SubscribeBlocking) && !tsc.Task.IsCompleted)
                    {
                        tsc.SetResult(result);
                    }
                }
                catch(Exception ex)
                {
                    tsc.SetException(ex);
                }

                return tsc.Task;
            }

            public Task<ReturnCode> ProvisionAsync(IEndpoint endpoint, EndpointProperties props, int flags)
            {
                var tsc = new TaskCompletionSource<ReturnCode>(TaskCreationOptions.RunContinuationsAsynchronously);
                try
                {
                    var result = this.Session.Provision(endpoint, props, flags, tsc);

                    if (((flags & ProvisionFlag.WaitForConfirm) > 0) && !tsc.Task.IsCompleted)
                    {
                        tsc.SetResult(result);
                    }
                }
                catch(Exception ex)
                {
                    tsc.SetException(ex);
                }
                return tsc.Task;
            }

            public Task<ReturnCode> SendAsync(IMessage message)
            {
                var tsc = new TaskCompletionSource<ReturnCode>(TaskCreationOptions.RunContinuationsAsynchronously);
                try
                {
                    message.CorrelationKey = tsc;
                    var sendResult = this.Session.Send(message);

                    if (this.Session.Properties.SendBlocking || message.DeliveryMode == MessageDeliveryMode.Direct && !tsc.Task.IsCompleted)
                    {
                        tsc.SetResult(sendResult);
                    }
                }
                catch (Exception ex)
                {
                    tsc.SetException(ex);
                }
                return tsc.Task;
            }

            private void SessionEventHandler(object sender, SessionEventArgs e)
            {
                CommandLine.WriteLine($"SessionEventHandler {e.Event}");
                switch (e.Event)
                {
                    // ConnectAsync
                    case SessionEvent.UpNotice:
                        this.connectTaskCompletion?.SetResult(ReturnCode.SOLCLIENT_OK);
                        break;
                    case SessionEvent.ConnectFailedError:
                        this.connectTaskCompletion?.SetException(new Exception(e.Info));
                        break;

                    // SubscribeAsync
                    // ProvisionAsync
                    // SendAsync
                    case SessionEvent.ProvisionOk:
                    case SessionEvent.SubscriptionOk:
                    case SessionEvent.Acknowledgement:
                        var tsc = e.CorrelationKey as TaskCompletionSource<ReturnCode>;
                        tsc?.SetResult(ReturnCode.SOLCLIENT_OK);
                        break;
                    case SessionEvent.ProvisionError:
                    case SessionEvent.SubscriptionError:
                    case SessionEvent.RejectedMessageError:
                        var tscE = e.CorrelationKey as TaskCompletionSource<ReturnCode>;
                        tscE?.SetException(new Exception(e.Info));
                        break;
                }
            }

            private void MessageEventHandler(object sender, MessageEventArgs e)
            {
                // This could be automatically dispatched on a thread pool thread to prevent deadlocks
                this.MessageReceived?.Invoke(this, e);
                e.Message.Dispose();
            }

            public void Dispose()
            {
                Session?.Dispose();
            }
        }
    }
}
