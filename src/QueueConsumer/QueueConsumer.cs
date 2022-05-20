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
using System.Text;
using SolaceSystems.Solclient.Messaging;
using System.Threading;

/// <summary>
/// Solace Systems Messaging API tutorial: QueueConsumer
/// </summary>

namespace Tutorial
{
    /// <summary>
    /// Demonstrates how to use Solace Systems Messaging API for sending and receiving a guaranteed delivery message
    /// </summary>
    class QueueConsumer
    {
        const int DefaultConnectRetries = 3;
        private readonly AutoResetEvent messageReceivedEvent = new AutoResetEvent(false);

        public void Run(string host, string vpnname, string username, string password)
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
                                AckMode = MessageAckMode.ClientAck
                            };

                            using (var flow = session.CreateFlow(flowProperties, queue, null, HandleMessageEvent, HandleFlowEvent))
                            {
                                flow.Start();

                                Console.WriteLine($"Waiting for a message in the queue '{queueName}'...");
                                messageReceivedEvent.WaitOne();
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
        private void HandleMessageEvent(object source, MessageEventArgs args)
        {
            var flow = source as IFlow;

            // Received a message
            Console.WriteLine("Received message.");
            using (IMessage message = args.Message)
            {
                // Expecting the message content as a binary attachment
                Console.WriteLine($"Message content: {Encoding.UTF8.GetString(message.BinaryAttachment)}");
                // ACK the message
                flow.Ack(message.ADMessageId);
                // finish the program
                messageReceivedEvent.Set();
            }
        }

        public void HandleFlowEvent(object sender, FlowEventArgs args)
        {
            // Received a flow event
            Console.WriteLine($"Received Flow Event '{args.Event}' Type: '{args.ResponseCode}' Text: '{args.Info}'");
        }
    }

}
