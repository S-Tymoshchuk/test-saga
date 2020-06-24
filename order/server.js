const express = require("express");
const {
  Kafka
} = require("kafkajs");
const bodyParser = require("body-parser");
const app = express();
const mongoose = require('mongoose');
const User = require('./mongoDB')

const urlencodedParser = bodyParser.urlencoded({
  extended: false,
});

const kafka = new Kafka({
  clientId: "api",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({
  groupId: 'test-group'
})

async function test() {
  await consumer.connect();
  await consumer.subscribe({
    topic: 'check-user',
    fromBeginning: true
  })
  await consumer.run({
    eachMessage: async ({
      topic,
      partition,
      message
    }) => {
      const obj = JSON.parse(message.value)
      const newTransaction = {
        transactionId: obj.transactionId,
        event: obj.event
      }
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString()
      });
    }
  })
}

app.post("/user", urlencodedParser, async (req, res) => {
  if (!req.body) return res.sendStatus(400);
  const user = {
    event: 'CHECK_USER',
    name: req.body.name,
  };

  const newTransaction = await new User(user);
  newTransaction.save()

  const newMessage = {
    transactionId: newTransaction._id,
    event: 'CHECK_USER',
    name: req.body.name,
  }
  const message = await JSON.stringify(newMessage);
  await producer.send({
    topic: "check-user",
    messages: [{
      value: message,
    }, ],
  });
  res.send(user);
});
test()

async function run() {
  mongoose.connect(
    "mongodb://localhost:27017/transaction", {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    },
    function (err) {
      if (err) return console.log(err);
      app.listen(3000, function () {
        console.log("Сервер ожидает подключения...");
      });
    }
  );
}

run()