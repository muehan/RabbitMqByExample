using System;
using Common;
using RabbitMQ.Client;

namespace StandardQueue
{
    class Program
    {
        private static ConnectionFactory _factory;
        private static IConnection _connection;
        private static IModel _model;

        private const string QueueName = "StandardQueue_ExampleQueue";

        static void Main()
        {
            var payment1 = new Payment {AmountToPay = 25.0m, CardNumber = "123456789112233"};
            var payment2 = new Payment {AmountToPay = 26.0m, CardNumber = "123456789112233"};
            var payment3 = new Payment {AmountToPay = 23.0m, CardNumber = "123456789112233"};
            var payment4 = new Payment {AmountToPay = 22.0m, CardNumber = "123456789112233"};
            var payment5 = new Payment {AmountToPay = 25.0m, CardNumber = "123456789112233"};
            var payment6 = new Payment {AmountToPay = 32.0m, CardNumber = "123456789112233"};
            var payment7 = new Payment {AmountToPay = 22.0m, CardNumber = "123456789112233"};
            var payment8 = new Payment {AmountToPay = 253.0m, CardNumber = "123456789112233"};
            var payment9 = new Payment {AmountToPay = 2.0m, CardNumber = "123456789112233"};
            var payment10 = new Payment {AmountToPay = 245.0m, CardNumber = "123456789112233"};
            var payment11 = new Payment {AmountToPay = 20.0m, CardNumber = "123456789112233"};
            var payment12= new Payment {AmountToPay = 250.0m, CardNumber = "123456789112233"};

            CreateQueue();
            SendMessage(payment1);
            SendMessage(payment2);
            SendMessage(payment3);
            SendMessage(payment4);
            SendMessage(payment5);
            SendMessage(payment6);
            SendMessage(payment7);
            SendMessage(payment8);
            SendMessage(payment9);
            SendMessage(payment10);
            SendMessage(payment11);
            SendMessage(payment12);

            Recieve();
        }

        private static void CreateQueue()
        {
            _factory = new ConnectionFactory { HostName = "172.17.2.126", UserName = "guest", Password = "guest" };
            _connection = _factory.CreateConnection();
            _model = _connection.CreateModel();

            _model.QueueDeclare(QueueName, true, false, false, null);
        }

        private static void SendMessage(Payment payment)
        {
            _model.BasicPublish("", QueueName, null, payment.Serialize());

            Console.WriteLine(" [x] Payment Message Sent : {0} : {1} : {2}", payment.CardNumber, payment.AmountToPay, payment.Name);
        }

        public static void Recieve()
        {
            var consumer = new QueueingBasicConsumer(_model);
            var msgCount = GetMessageCount(_model, QueueName);

            _model.BasicConsume(QueueName, true, consumer);

            var count = 0;

            while (count < msgCount)
            {
                var message = (Payment)consumer.Queue.Dequeue().Body.DeSerialize(typeof(Payment));

                Console.WriteLine("----- Received {0} : {1} : {2}", message.CardNumber, message.AmountToPay, message.Name);
                count++;
            }
        }

        private static uint GetMessageCount(IModel channel, string queueName)
        {
            var results = channel.QueueDeclare(queueName, true, false, false, null);
            return results.MessageCount;
        }
    }
}
