const {
    Kafka
} = require("kafkajs");

const msg = process.argv[2];

async function run() {
    try {
        const kafka = new Kafka({
            clientId: "kafka",
            brokers: ["localhost:9092"]
        });

        const producer = kafka.producer();
        console.log("Connecting...")
        await producer.connect();
        console.log("Connected!");

        // A-M 0 N-Z 1
        const partition = msg[0] < "N" ? 0 : 1;
        const result = await producer.send({
            topic: "Users",
            messages: [
                { value: msg, partition: partition }
            ]
        })

        console.log(`DONE! ${JSON.stringify(result)}`);
        await producer.disconnect();
        
    } catch (error) {
        console.error(error);
    }
    finally {
        process.exit(0);
    }
}

run();