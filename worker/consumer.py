import asyncio
import sys
from aio_pika import connect, IncomingMessage, ExchangeType


def on_message(message: IncomingMessage):
    with message.process():
        print(f" [x] Routing key: {message.routing_key}, Messages: {str(message.body)}")


async def main(loop):
    # Perform connection
    connection = await connect(
        host='localhost',
        login='guest',
        password='guest'
    )

    # Creating a channel
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    # Declare an exchange
    topic_logs_exchange = await channel.declare_exchange(
        'test', ExchangeType.TOPIC
    )

    arg = sys.argv[1:]

    if len(arg) < 2:
        sys.stderr.write(
            "Usage: %s [queue] [binding keys]...\n" % sys.argv[0]
        )
        sys.exit(1)

    # Declaring queue
    queue = await channel.declare_queue(
        arg[0], durable=True
    )

    for binding_key in arg[1:]:
        await queue.bind(topic_logs_exchange, routing_key=binding_key)

    # Start listening the queue with name 'task_queue'
    await queue.consume(on_message)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))
    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()
