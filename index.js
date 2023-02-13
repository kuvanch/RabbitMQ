require('dotenv').config()
const express = require('express')
const lib = require('amqplib/callback_api')
var fetch = require('node-fetch')

const app = express()
const PORT = 3000

app.use(express.json())
app.listen(
    PORT,
    () => console.log("Started!")
)

consumeMessages()

//Реализуем методы API
app.get('/info', (req, res) => {
    res.status(200).send({
        message: "Hello world!"
    })
})

app.post('/upload/:queue', (req, res) => {
    const { queue } = req.params
    const { message } = req.body

    sendAmqp(queue, message).then(r => {
        if (r) {
            res.status(200).send({ message: `The message ${message} for ${queue} is queued!` })
        } else {
            res.status(500).send({ message: `The message ${message} for ${queue} is NOT queued!` })
        }
    })
})

//Функция обработки сообщений из очереди
function consumeMessages() {
    lib.connect(process.env.AMQP_URL, (err, connection) => {
        if (err) {
            throw err
        }

        connection.createChannel((err, channel) => {
            if (err) {
                throw (err)
            }
            let queueName = process.env.QUEUE_TO_LISTEN

            channel.assertQueue(queueName, {
                durable: true
            })

            channel.consume(queueName, (msg) => {
                const msgStr = msg.content.toString()
                console.log(msgStr)

                fetch(process.env.ENDPOINT, {
                    method: 'POST',
                    body: JSON.stringify({
                        "context": {
                            "__name": msgStr,
                        }
                    }),
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': process.env.TOKEN
                    }
                })
                    .then(res => {
                        if (res.ok) {
                            channel.ack(msg)
                            res.text().then(r => console.log(r))
                        } else {
                            channel.nack(msg, false, true)
                            res.text().then(r => console.log(r))
                        }
                    })
            })
        })
    })
}

//Функция отправки сообщений в очередь
async function sendAmqp(queue, mess) {
    lib.connect(process.env.AMQP_URL, (err, connection) => {
        if (err) {
            throw err
        }
        connection.createChannel((err, channel) => {
            if (err) {
                throw (err)
            }
            let queueName = queue
            let message = mess
            channel.assertQueue(queueName, {
                durable: true
            })
            channel.sendToQueue(queueName, Buffer.from(message))
            setTimeout(() => {
                connection.close()
            }, 1000)
        })
    })
    return true
}

console.log(process.env.AMQP_URL)