import { ServiceBusClient } from "@azure/service-bus";

const conn = process.env.SERVICEBUS_CONNECTION;
const queue = process.env.SERVICEBUS_QUEUE || "process";
if (!conn) throw new Error("Missing SERVICEBUS_CONNECTION");

const sb = new ServiceBusClient(conn);
const cmd = process.argv[2];

if (cmd === "send") {
  const sender = sb.createSender(queue);
  const body = { ping: "mediaflow", at: new Date().toISOString() };
  await sender.sendMessages({ body: JSON.stringify(body), subject: "smoke" });
  console.log("Sent:", body);
  await sender.close();
} else if (cmd === "recv") {
  const receiver = sb.createReceiver(queue);
  const msgs = await receiver.receiveMessages(1, { maxWaitTimeInMs: 5000 });
  if (msgs.length) {
    let payload = msgs[0].body;
    if (Buffer.isBuffer(payload)) payload = payload.toString();
    console.log("Received:", payload);
    await receiver.completeMessage(msgs[0]);
  } else {
    console.log("No messages available");
  }
  await receiver.close();
} else {
  console.log("Usage: node scripts/sb-smoke.mjs send|recv");
}
await sb.close();
