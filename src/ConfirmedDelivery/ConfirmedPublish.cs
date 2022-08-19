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
using System.Threading;
using System.Collections.Generic;

using SolaceSystems.Solclient.Messaging;
using Tutorial.Common;

/// <summary>
/// Solace Systems Messaging API tutorial: ConfirmedPublish
/// </summary>

namespace Tutorial
{
    /// <summary>
    /// Demonstrates how to use Solace Systems Messaging API for sending a confirmed guaranteed delivery message
    /// </summary>
    class ConfirmedPublish
    {
        static readonly int DefaultConnectRetries = 3;
        static readonly int TotalMessages = 5;
        static readonly CountdownEvent CountdownEvent = new CountdownEvent(TotalMessages);
        static void Main(string[] args)
        {
            if (CommandLine.TryLoadConfig(args, out var config))
            {
                Run(config.Host, config.Vpn, config.UserName, config.Password);
            }
        }

        static void Run(string host, string vpnname, string username, string password)
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
                using (var session = context.CreateSession(sessionProperties, null, HandleSessionEvent))
                {
                    // Connect to the Solace messaging router
                    Console.WriteLine($"Connecting as {username}@{vpnname} on {host}...");
                    var connectResult = session.Connect();

                    if (connectResult == ReturnCode.SOLCLIENT_OK)
                    {
                        Console.WriteLine("Session successfully connected.");

                        var queueName = "Q/tutorial";
                        var msgInfoList = new List<MsgInfo>();

                        // Create the queue
                        using (var queue = ContextFactory.Instance.CreateQueue(queueName))
                        {
                            // Set queue permissions to "consume" and access-type to "exclusive"
                            var endpointProps = new EndpointProperties()
                            {
                                Permission = EndpointProperties.EndpointPermission.Consume,
                                AccessType = EndpointProperties.EndpointAccessType.Exclusive
                            };

                            Console.WriteLine(value: $"Attempting to provision the queue '{queueName}'...");
                            session.Provision(queue, endpointProps,
                                ProvisionFlag.IgnoreErrorIfEndpointAlreadyExists | ProvisionFlag.WaitForConfirm, null);
                            Console.WriteLine($"Queue '{queueName}' has been created and provisioned.");

                            // Create the message
                            using (var message = ContextFactory.Instance.CreateMessage())
                            {
                                // Message's destination is the queue and the message is persistent
                                message.Destination = queue;
                                message.DeliveryMode = MessageDeliveryMode.Persistent;

                                // Send it to the mapped topic a few times with different content
                                for (var i = 0; i < TotalMessages; i++)
                                {
                                    // Create the message content as a binary attachment
                                    message.BinaryAttachment = Encoding.UTF8.GetBytes(
                                        $"Confirmed Publish Tutorial! Message ID: {i}");

                                    // Create a message correlation object and attach it to the message
                                    var msgInfo = new MsgInfo(message, i);
                                    message.CorrelationKey = msgInfo;
                                    msgInfoList.Add(msgInfo);

                                    // Send the message to the queue on the Solace messaging router
                                    Console.WriteLine($"Sending message to queue {queueName}...");
                                    var sendResult = session.Send(message);
                                    if (sendResult != ReturnCode.SOLCLIENT_OK)
                                    {
                                        Console.WriteLine($"Sending failed, return code: {sendResult}");
                                    }
                                }
                            }
                        }

                        Console.WriteLine($"{TotalMessages} messages sent. Processing replies.");

                        // block the current thread until a confirmation received
                        CountdownEvent.Wait();

                        foreach (var msgInfo in msgInfoList)
                        {
                            if (msgInfo.Accepted)
                            {
                                Console.WriteLine($"Message {msgInfo.Id} was accepted by the router.");
                            }
                            if (msgInfo.Acked)
                            {
                                Console.WriteLine($"Message {msgInfo.Id} was acknowledged by the router.");
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

        static void HandleSessionEvent(object sender, SessionEventArgs args)
        {
            // Received a session event
            Console.WriteLine($"Received session event. Event Type = {args.Event}.");
            switch (args.Event)
            {
                // this is the confirmation
                case SessionEvent.Acknowledgement:
                case SessionEvent.RejectedMessageError:
                    var messageRecord = args.CorrelationKey as MsgInfo;
                    if (messageRecord != null)
                    {
                        messageRecord.Acked = true;
                        messageRecord.Accepted = args.Event == SessionEvent.Acknowledgement;
                        CountdownEvent.Signal();
                    }
                    break;
                default:
                    break;
            }
        }
        
        class MsgInfo
        {
            public bool Acked { get; set; }
            public bool Accepted { get; set; }
            public readonly IMessage Message;
            public readonly int Id;
            public MsgInfo(IMessage message, int id)
            {
                Acked = false;
                Accepted = false;
                Message = message;
                Id = id;
            }
        }
    }

}
