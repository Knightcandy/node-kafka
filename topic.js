const {
    Kafka
} = require("kafkajs");

async function run() {
    try {
        const kafka = new Kafka({
            clientId: "kafka",
            brokers: ["localhost:9092"]
        });

        const admin = kafka.admin();
        console.log("Connecting...")
        await admin.connect();
        console.log("Connected!");

        // A-M 0 N-Z 1
        await admin.createTopics({
            topics: [{
                topic: "Users",
                numPartitions: 2
            }]
        });

        console.log("DONE!");

        await admin.disconnect();
        
    } catch (error) {
        console.error(error);
    }
    finally {
        process.exit(0);
    }
}

run();