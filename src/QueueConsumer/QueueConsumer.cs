#region Copyright & License
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#endregion

using System;
using System.Collections.Concurrent;
using System.Text;

using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

using SolaceSystems.Solclient.Messaging;

/// <summary>
/// Solace Systems Messaging API tutorial: QueueConsumer
/// </summary>

namespace Tutorial
{
    /// <summary>
    /// Demonstrates how to use Solace Systems Messaging API for sending and receiving a guaranteed delivery message
    /// </summary>
    public class QueueConsumer
    {
        const int DefaultConnectRetries = 3;
        const int ProcessingTimeMs = 500;
        const int MaxDegreeOfParallelism = 4;
        const int MaxUnackedMessages = MaxDegreeOfParallelism * 2;

        string host;
        string vpnname;
        string username;
        string password;
        
        ActionBlock<IMessage> processMessageActionBlock;
        IFlow flow;
        
        public QueueConsumer(string host, string vpnname, string username, string password)
        {
            this.host = host;
            this.vpnname = vpnname;
            this.username = username;
            this.password = password;

            var processMessageOptions = new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = MaxDegreeOfParallelism
            };

            this.processMessageActionBlock = new ActionBlock<IMessage>(ProcessMessage, processMessageOptions);
        }

        public void Run(CancellationToken cancellationToken)
        {
            try
            {
                // Initialize Solace Systems Messaging API with logging to console at Warning level
                var props = new ContextFactoryProperties() { SolClientLogLevel = SolLogLevel.Warning };
                props.LogToConsoleError();
                ContextFactory.Instance.Init(props);
                
                // Define context and session properties
                var contextProperties = new ContextProperties();
                var sessionProperties = new SessionProperties()
                {
                    Host = host,
                    VPNName = vpnname,
                    UserName = username,
                    Password = password,
                    ConnectRetries = DefaultConnectRetries,
                    KeepAliveIntervalInMsecs = 500,
                    KeepAliveIntervalsLimit = 4
                };
                
                // Create context and session instances
                using (var context = ContextFactory.Instance.CreateContext(contextProperties, null))
                using (var session = context.CreateSession(sessionProperties, null, null))
                {
                    // Connect to the Solace messaging router
                    Console.WriteLine($"Connecting as {username}@{vpnname} on {host}...");
                    var connectResult = session.Connect();

                    if (connectResult == ReturnCode.SOLCLIENT_OK)
                    {
                        Console.WriteLine("Session successfully connected.");

                        // Provision the queue
                        var queueName = "Q/tutorial";
                        Console.WriteLine($"Attempting to provision the queue '{queueName}'...");

                        // Set queue permissions to "consume" and access-type to "exclusive"
                        var endpointProps = new EndpointProperties()
                        {
                            Permission = EndpointProperties.EndpointPermission.Consume,
                            AccessType = EndpointProperties.EndpointAccessType.Exclusive
                        };

                        // Create the queue
                        using (var queue = ContextFactory.Instance.CreateQueue(queueName))
                        {
                            session.Provision(queue, endpointProps,
                                ProvisionFlag.IgnoreErrorIfEndpointAlreadyExists | ProvisionFlag.WaitForConfirm, null);
                            Console.WriteLine($"Queue '{queueName}' has been created and provisioned.");

                            var flowProperties = new FlowProperties()
                            {
                                AckMode = MessageAckMode.ClientAck,
                                MaxUnackedMessages = MaxUnackedMessages
                            };

                            using (this.flow = session.CreateFlow(flowProperties, queue, null, HandleMessageEvent, HandleFlowEvent))
                            {
                              this.flow.Start();
                              this.processMessageActionBlock.Completion.Wait(cancellationToken);
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Error connecting, return code: {connectResult}");
                    }
                }
            }
            finally
            {
                // Dispose Solace Systems Messaging API
                ContextFactory.Instance.Cleanup();
            }
            Console.WriteLine("Finished.");
        }

        /// <summary>
        /// This event handler is invoked by Solace Systems Messaging API when a message arrives
        /// </summary>
        /// <param name="source"></param>
        /// <param name="args"></param>
        void HandleMessageEvent(object source, MessageEventArgs args)
        {
            Console.WriteLine("Received message from broker.");

            var message = args.Message;
            processMessageActionBlock.Post(message);
        }

        void HandleFlowEvent(object sender, FlowEventArgs args)
        {
            // Received a flow event
            Console.WriteLine($"Received Flow Event '{args.Event}' Type: '{args.ResponseCode}' Text: '{args.Info}'");
        }

        void ProcessMessage(IMessage message)
        {
            // Expecting the message content as a binary attachment
            Console.WriteLine($"Processing message: {Encoding.UTF8.GetString(message.BinaryAttachment)}");

            Thread.Sleep(ProcessingTimeMs);

            // ACK the message
            this.flow.Ack(message.ADMessageId);
        }
    }

}
