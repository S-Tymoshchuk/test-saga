const express = require('express');
const {
  Kafka
} = require('kafkajs');
const mongoose = require('mongoose');
const User = require('./mongoDB');
const {
  json
} = require('express');

const app = express();


const kafka = new Kafka({
  clientId: 'api',
  brokers: ['localhost:9092']
})

const consumer = kafka.consumer({
  groupId: 'test-group'
})

const producer = kafka.producer;

async function run() {
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



      const obj = JSON.parse(message.value);
      const newmessage = {
        transactionId: obj.transactionId,
        event: "VERIFY_USER",
        name: obj.name
      }
      const user = await User.findOne({
        name: obj.name
      })
      if (user) {
        await producer.send({
          topic: 'check-user',
          messages: [{
            value: JSON.stringify(newmessage)
          }]
        })

        console.log({
          partition,
          offset: message.offset,
          value: message.value.toString()
        });
      }
      return null


    }
  })

  mongoose.connect(
    "mongodb://localhost:27017/usersdb", {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    },
    function (err) {
      if (err) return console.log(err);
      app.listen(3001, function () {
        console.log("Pament service is running....");
      });
    }
  );
}
run()