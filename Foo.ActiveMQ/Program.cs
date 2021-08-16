using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.Util;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Foo.ActiveMQ
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.WriteLine("App Starting");
            var queueDestination = "TestQueue";

            // queue
            EnqueueToQueue(queueDestination, "Settl activemq");
            Console.WriteLine("Send an item to the queue.");

            //string dequeuedItem = DequeueFromQueue(queueDestination);
            //Console.WriteLine($"Dequeued item is '{dequeuedItem}' from the queue.");

            //EnqueueToQueue(queueDestination, "foo message1");
            //EnqueueToQueue(queueDestination, "foo message2");
            //EnqueueToQueue(queueDestination, "foo message3");

            //int pendingMessageCount = GetPendingMessagesCount(queueDestination);
            //Console.WriteLine($"Pending message count is '{pendingMessageCount}'.");

            //EnqueueToQueue(queueDestination, "foo message4");
            //Console.WriteLine("Send an item to the queue. Write them but do not enqueue:");

            //List<string> pendingMessages = GetPendingMessages(queueDestination);
            //foreach (var pendingMessage in pendingMessages)
            //{
            //    Console.WriteLine($"Pending message is '{pendingMessage}'.");
            //}

            //// delete destination
            //DeleteDestination(queueDestination);
            //Console.WriteLine("Deleted destination operation was successfull.");

            //EnqueueToQueue(queueDestination, "foo new message");
            //Console.WriteLine("Send an item to the queue (after delete destination).");

            //dequeuedItem = DequeueFromQueue(queueDestination);
            //Console.WriteLine($"Dequeued item (after delete destination) is '{dequeuedItem}' from the queue.");

            //// topic
            //var topicDequeueTask = Task.Factory.StartNew(() =>
            //{
            //    var exit = false;

            //    while (!exit)
            //    {
            //        // always listen to the topic
            //        var dequeuedItem = DequeueFromTopic(queueDestination);
            //        if (!string.IsNullOrEmpty(dequeuedItem))
            //        {
            //            Console.WriteLine($"Dequeued an item is '{dequeuedItem}' from the topic");
            //            exit = true;
            //        }
            //    }
            //});

            //var topicEnqueueTask = Task.Factory.StartNew(() =>
            //{
            //    EnqueueToTopic(queueDestination, "foo topic message");
            //    Console.WriteLine("Send an item to the topic");
            //});

            //Task.WaitAll(topicEnqueueTask, topicDequeueTask);

            //Console.WriteLine("Program has finished. Press any key to close your application.");
            Console.ReadKey();
        }

        private static string BrokerUri => "failover:(ssl://b-4195bc73-37c0-4c29-a497-722d6d6bda28-1.mq.us-east-2.amazonaws.com:61617,ssl://b-4195bc73-37c0-4c29-a497-722d6d6bda28-2.mq.us-east-2.amazonaws.com:61617)";

        //private static string BrokerUri => "amqp+ssl://b-4195bc73-37c0-4c29-a497-722d6d6bda28-1.mq.us-east-2.amazonaws.com:5671";

        private static string UserName => "MorganJay";

        private static string Password => "ghostboy1998`";

        public static void EnqueueToQueue(string queueDestination, string queueItem)
        {
            IConnectionFactory factory = new ConnectionFactory(BrokerUri);

            using IConnection connection = factory.CreateConnection(UserName, Password);
             connection.Start();

            using ISession session = connection.CreateSession();
            using IDestination dest = session.GetQueue(queueDestination);
            using IMessageProducer producer = session.CreateProducer(dest);

            ITextMessage textMessage = producer.CreateTextMessage(queueItem);
            producer.Send(textMessage);
            producer.Close();
            session.Close();
            connection.Close();
        }

        public static string DequeueFromQueue(string queueDestination)
        {
            IConnectionFactory factory = new ConnectionFactory(BrokerUri);

            using IConnection connection = factory.CreateConnection(UserName, Password);
            connection.Start();

            using ISession session = connection.CreateSession();
            using IDestination dest = session.GetQueue(queueDestination);
            using IMessageConsumer consumer = session.CreateConsumer(dest);

            IMessage receivedObj = consumer.Receive(TimeSpan.FromSeconds(10));
            if (!(receivedObj is ITextMessage receivedMessage))
            {
                throw new TimeoutException("Queue message can not receive.");
            }
            session.Close();
            connection.Close();
            string value = receivedMessage.Text;
            return value;
        }

        public static int GetPendingMessagesCount(string queueDestination)
        {
            int messageCount = 0;

            IConnectionFactory factory = new ConnectionFactory(BrokerUri);

            using IConnection connection = factory.CreateConnection(UserName, Password);
            connection.Start();

            using ISession session = connection.CreateSession();
            using IDestination requestDestination = SessionUtil.GetDestination(session, queueDestination);
            using IQueueBrowser queueBrowser = session.CreateBrowser((IQueue)requestDestination);

            IEnumerator messages = queueBrowser.GetEnumerator();
            while (messages.MoveNext())
            {
                IMessage message = (IMessage)messages.Current;
                messageCount++;
            }

            return messageCount;
        }

        public static List<string> GetPendingMessages(string queueDestination)
        {
            var resultMessages = new List<string>();

            IConnectionFactory factory = new ConnectionFactory(BrokerUri);

            using IConnection connection = factory.CreateConnection(UserName, Password);
            connection.Start();

            using ISession session = connection.CreateSession();
            using IDestination requestDestination = SessionUtil.GetDestination(session, queueDestination);
            using IQueueBrowser queueBrowser = session.CreateBrowser((IQueue)requestDestination);

            IEnumerator messages = queueBrowser.GetEnumerator();
            while (messages.MoveNext())
            {
                IMessage message = (IMessage)messages.Current;
                ITextMessage textMessage = message as ITextMessage;
                resultMessages.Add(textMessage.Text);
            }

            return resultMessages;
        }

        public static void DeleteDestination(string destination)
        {
            IConnectionFactory factory = new ConnectionFactory(BrokerUri);

            using IConnection connection = factory.CreateConnection(UserName, Password);
            connection.Start();

            using ISession session = connection.CreateSession();
            IQueue queue = session.GetQueue(destination);
            session.DeleteDestination(queue);
        }

        public static void EnqueueToTopic(string queueDestination, string queueItem)
        {
            IConnectionFactory factory = new ConnectionFactory(BrokerUri);

            using IConnection connection = factory.CreateConnection(UserName, Password);
            connection.Start();

            using ISession session = connection.CreateSession();
            using IDestination dest = session.GetTopic(queueDestination);
            using IMessageProducer producer = session.CreateProducer(dest);

            ITextMessage textMessage = producer.CreateTextMessage(queueItem);
            producer.Send(textMessage);
        }

        public static string DequeueFromTopic(string queueDestination)
        {
            IConnectionFactory factory = new ConnectionFactory(BrokerUri);

            using IConnection connection = factory.CreateConnection(UserName, Password);
            connection.Start();

            using ISession session = connection.CreateSession();
            using IDestination dest = session.GetTopic(queueDestination);
            using IMessageConsumer consumer = session.CreateConsumer(dest);

            IMessage receivedObj = consumer.Receive();
            if (!(receivedObj is ITextMessage receivedMessage))
            {
                throw new TimeoutException("Topic message can not receive.");
            }

            string queueItem = receivedMessage.Text;
            return queueItem;
        }
    }
}