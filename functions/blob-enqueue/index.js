const { ServiceBusClient } = require("@azure/service-bus");
const { MongoClient } = require("mongodb");

let sbSender, jobsColl;

async function sender() {
  if (sbSender) return sbSender;
  const sb = new ServiceBusClient(process.env.SERVICEBUS_CONNECTION);
  sbSender = sb.createSender(process.env.SERVICEBUS_QUEUE || "process");
  return sbSender;
}

async function jobs() {
  if (jobsColl) return jobsColl;
  const client = new MongoClient(process.env.COSMOS_URI, { retryWrites: false, appName: "mediaflow-blob-enqueue" });
  await client.connect();
  jobsColl = client.db(process.env.COSMOS_DB || "mediaflow").collection(process.env.COSMOS_COLL || "jobs");
  return jobsColl;
}

module.exports = async function (context, blob) {
  const name = context.bindingData.name;
  const base = process.env.BLOB_BASE_URL; // e.g. https://<storage>.blob.core.windows.net
  const url = `${base}/uploads/${encodeURIComponent(name)}`;

  // Default to blob name (matches how API sets job id)
  let jobId = name;

  // Try to refine from Cosmos if present
  try {
    const coll = await jobs();
    const job = await coll.findOne({ url });
    if (job && (job.id || job._id)) {
      jobId = job.id || String(job._id);
    }
  } catch (e) {
    context.log.warn("Cosmos lookup failed:", e.message);
  }

  const payload = { id: jobId, url, blob: { container: "uploads", name } };

  try {
    const s = await sender();
    await s.sendMessages({
      body: JSON.stringify(payload),
      contentType: "application/json",
      subject: "mediaflow.process.request"
    });
    context.log(`Enqueued ${name} (id=${jobId})`);
  } catch (e) {
    context.log.error("Service Bus send failed:", e.message);
    throw e; // retry
  }
};
