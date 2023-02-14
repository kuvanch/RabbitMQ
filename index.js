require('dotenv').config()
process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;
const http = require("https");

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

function PostCode(name) {
    // Build the post string from an object
    const post_data = JSON.stringify({
        "context": {
          "client_region": [
            "45af5e7e-7bac-4ca0-808d-5ea80294dbe4"
          ],
          "client_district": [
            "0dbd49d5-c3d1-4b64-98ae-79fa9d7b3503"
          ],
          "bank_info": [
            "db5c6cde-05cf-4e69-9af8-7267b2915af6"
          ],
          "bank_info_str": "00433",
          "local": "01000",
          "loan_id": name,
          "segment_code": "",
          "client_name": "ZAXIDOV MAKSAD MUKIMDJANOVICH",
          "balance_account": "12501",
          "loan_account": "12501000660062582802",
          "currency": [
            "e537a9ca-d0a6-45ea-989d-e5f5e29938fc"
          ],
          "contract_amount": 5000000,
          "contract_amount_nat_cur": 5000000,
          "issue_date": "2022-08-22T00:00:00+00:00",
          "maturity_date": "2023-08-21T00:00:00+00:00",
          "term_type": [
            "d5563399-0473-4693-85ff-f795a04a8481"
          ],
          "num_and_date_of_contract": "2-22.08.2022",
          "loan_percent": "27",
          "smr_loan_percent": "",
          "overdue_percentage": "40.5",
          "credit_acc_balance": 2223381.79,
          "balance_rev": 0,
          "num_and_date_renewal": "",
          "maturity_date_renewal": null,
          "overdue_balance": 0,
          "overdue_start_date": null,
          "judicial_balance": 0,
          "total_debt": 2223381.79,
          "quality_class": [
            "0b1a1c82-93f2-4645-b2ce-022a4c7c329d"
          ],
          "reserve_balance": 0,
          "security_valuation": 0,
          "security": [],
          "security_description": "",
          "funds_sources": [
            "a03530c0-8db6-4655-a598-e3393ea38df9"
          ],
          "lending_type": [
            "84f4582b-cc5d-4fca-9a5b-ca74559ffbfb"
          ],
          "loan_goal": [
            "e35a3b85-b62f-4d53-9967-e121fa04553a"
          ],
          "parent_org_of_client": [
            "a1d9a15d-65a3-4720-8bb4-93ce36d71f15"
          ],
          "loan_industry": [
            "d511af8a-7ae2-4d1c-8944-284434fb1020"
          ],
          "client_industry": [],
          "credit_rating": [
            "7af21e36-2f9c-4115-99e9-9d679b0f0413"
          ],
          "cb_chairman": "Online",
          "client_address": "ANDIJON SH OXUNBABOYEV 33-UY,712947970",
          "contract_uid": "11718202",
          "inn_passport": "AA9245205",
          "balance_interest_offbalance": 0,
          "overdue_balance_interest_offbalance": 0,
          "sum_cur_year": 0,
          "repaid_cur_year": 196.45,
          "total_issued": 0,
          "total_repaid": 2776618.21,
          "loan_purpose_specific": "ZAXIDOV MAKSAD MUKIMDJANOVICH - Online",
          "credit_line_purpose": "ZAXIDOV MAKSAD MUKIMDJANOVICH - Онлайн микрозайм",
          "judicial_account": "15703000560062582002",
          "judicial_account_writeoff": "",
          "debt_account": "12505000660062582802",
          "inps": "31701861240076",
          "birth_date": "1986-01-17T00:00:00+00:00",
          "balance_16309": 34538.56,
          "balance_16379": 0,
          "balance_16323": 0,
          "balance_16325": 0,
          "balance_16377": 0,
          "date_overdue_percent": null,
          "balance_16397": 0,
          "balance_91501": 0,
          "balance_91503": 0,
          "balance_95413": 0,
          "balance_91809": 0,
          "credit_status": "Текущая ссуда",
          "checking_account_balance": 0,
          "loan_product": "3460-ТК - ЖШ - Микрокарз - Online (12 ойлик)",
          "mahalla_name": "",
          "borrower_type": [
            "ee49ea96-1414-4ad3-9453-e2104115ca49"
          ],
          "count": 0,
          "gender": [
            "055c5c27-76ad-47a4-8268-f58acfa8ccb3"
          ],
          "isRepaid": false,
          "isTarget": false
        }
      });
  
    const post_options = {
      host: "elma365.mkb.uz",
      path: "/pub/v1/app/monitoring/individual/create",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": process.env.TOKEN,
      },
    };
  
    // Set up the request
    const post_req = http.request(post_options, function (res) {
      res.setEncoding("utf8");
      res.on("data", function (chunk) {
        console.log("Response: " + chunk);
      });
    });
    post_req.write(post_data);
    post_req.end();
  }


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
                console.log(msg)

                PostCode(msgStr)
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

setInterval(() => {
    const timestamp = new Date().getTime()
    sendAmqp('test', `rabbit: ${timestamp}`)
}, 100)
    


console.log(process.env.AMQP_URL)