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
using System.Linq;
using System.Text;
using SolaceSystems.Solclient.Messaging;
using System.Threading;

/// <summary>
/// Solace Systems Messaging API tutorial: TopicToQueueMapping
/// </summary>

namespace Tutorial
{
    /// <summary>
    /// Demonstrates how to use Solace Systems Messaging API for mapping a topic to a queue
    /// </summary>
    class TopicToQueueMapping
    {
        const int DefaultConnectRetries = 3;
        const int TotalMessages = 5;

        CountdownEvent CountdownEvent = new CountdownEvent(TotalMessages);

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

                        var requiredCapabilites = new[]
                        {
                            CapabilityType.PUB_GUARANTEED,
                            CapabilityType.SUB_FLOW_GUARANTEED,
                            CapabilityType.ENDPOINT_MANAGEMENT,
                            CapabilityType.QUEUE_SUBSCRIPTIONS,
                        };

                        foreach(var requiredCapability in requiredCapabilites)
                        {
                            if(!session.IsCapable(requiredCapability))
                            {
                                Console.WriteLine("Required capabilities are not supported.");
                                throw new InvalidOperationException($"Cannot proceed because session's {requiredCapability} capability is not supported.");
                            }
                        }
                        Console.WriteLine("All required capabilities supported.");

                        // Provision the queue
                        string queueName = "Q/tutorial/topicToQueueMapping";
                        Console.WriteLine($"Attempting to provision the queue '{queueName}'...");

                        // Create the queue
                        using (IQueue queue = ContextFactory.Instance.CreateQueue(queueName))
                        {
                            // Set queue permissions to "consume" and access-type to "exclusive"
                            EndpointProperties endpointProps = new EndpointProperties()
                            {
                                Permission = EndpointProperties.EndpointPermission.Consume,
                                AccessType = EndpointProperties.EndpointAccessType.Exclusive
                            };
                            // Provision it, and do not fail if it already exists
                            session.Provision(queue, endpointProps,
                                ProvisionFlag.IgnoreErrorIfEndpointAlreadyExists | ProvisionFlag.WaitForConfirm, null);
                            Console.WriteLine($"Queue '{queueName}' has been created and provisioned.");

                            // Add subscription to the topic mapped to the queue
                            using (var tutorialTopic = ContextFactory.Instance.CreateTopic("T/mapped/topic/sample"))
                            {
                                session.Subscribe(queue, tutorialTopic, SubscribeFlag.WaitForConfirm, null);

                                // Create the message
                                using (var message = ContextFactory.Instance.CreateMessage())
                                {
                                    // Message's destination is the queue and the message is persistent
                                    message.Destination = queue;
                                    message.DeliveryMode = MessageDeliveryMode.Persistent;

                                    for (int i = 0; i < TotalMessages; i++)
                                    {
                                        // Create the message content as a binary attachment
                                        message.BinaryAttachment = Encoding.UTF8.GetBytes(
                                            string.Format($"Topic to Queue Mapping Tutorial! Message ID: {i}"));

                                        // Send the message to the queue on the Solace messaging router
                                        Console.WriteLine($"Sending message ID {i} to topic '{tutorialTopic.Name}' mapped to queue '{queueName}'...");
                                        var sendResult = session.Send(message);
                                        if (sendResult == ReturnCode.SOLCLIENT_OK)
                                        {
                                            Console.WriteLine("Done.");
                                        }
                                        else
                                        {
                                            Console.WriteLine($"Sending failed, return code: {sendResult}");
                                        }
                                    }
                                }

                                Console.WriteLine($"{TotalMessages} messages sent. Processing replies.");

                                // Create and start flow to the newly provisioned queue
                                // NOTICE HandleMessageEvent as the message event handler 
                                // and HandleFlowEvent as the flow event handler
                                var flowProperties = new FlowProperties()
                                {
                                    AckMode = MessageAckMode.ClientAck
                                };

                                using (var flow = session.CreateFlow(flowProperties, queue, null, HandleMessageEvent, HandleFlowEvent))
                                {
                                    flow.Start();

                                    // block the current thread until a confirmation received
                                    CountdownEvent.Wait();
                                }
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
                CountdownEvent.Signal();
            }
        }

        /// <summary>
        /// This event handler is invoked by Solace Systems Messaging API when a flwo event happens
        /// </summary>
        public void HandleFlowEvent(object sender, FlowEventArgs args)
        {
            // Received a flow event
            Console.WriteLine($"Received Flow Event '{args.Event}' Type: '{args.ResponseCode}' Text: '{args.Info}'");
        }
    }

}
