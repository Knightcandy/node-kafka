const {
    Kafka
} = require("kafkajs");



async function run() {
    try {
        const kafka = new Kafka({
            clientId: "kafka",
            brokers: ["localhost:9092"]
        });

        const consumer = kafka.consumer({ groupId: "test" });
        console.log("Connecting...")
        await consumer.connect();
        console.log("Connected!");

        await consumer.subscribe({
            topic: "Users",
            fromBeginning: false
        })

        await consumer.run({
            eachMessage: async (result) => {
                console.log(`Received! ${result.message.value} on partition ${result.partition}`);
            }
        });
        
    } catch (error) {
        console.error(error);
    }
}

run();