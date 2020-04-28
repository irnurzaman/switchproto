import asyncio
from random import randint
from aio_pika import connect, Message, DeliveryMode, ExchangeType


async def main(loop):
    # Perform connection
    connection = await connect(
        host='localhost',
        login='guest',
        password='guest'
    )

    # Creating a channel
    channel = await connection.channel()

    topic_logs_exchange = await channel.declare_exchange(
        'test', ExchangeType.TOPIC
    )

    routing_key = ('1', '2', '3', '4')

    message_body = [(f"{i} - Messages", str(randint(1,4))) for i in range(10)]

    for el in message_body:
        message = Message(
            bytes(el[0], encoding='utf-8'),
            delivery_mode=DeliveryMode.PERSISTENT
        )

        # Sending the message
        await topic_logs_exchange.publish(
            message, routing_key=el[1]
        )

        print(f" [x] Sent Message: {el[0]} | to: {el[1]}")

    await connection.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))