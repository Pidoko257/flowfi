/**
 * Workers registry
 *
 * Exports a single `startWorkers` function that is called from the main server
 * entry-point after the database connection is confirmed healthy.
 */

import { sorobanEventWorker } from "./soroban-event-worker.js";
import { startStreamRunwayWorker } from "./stream-runway-worker.js";
import {
  purgeExpiredAlertHistory,
  purgeExpiredWebhookDeliveries,
} from "../services/webhook.service.js";
import logger from "../logger.js";

let runwayWorkerTimer: NodeJS.Timeout | null = null;
let webhookRetentionTimer: NodeJS.Timeout | null = null;
const WEBHOOK_RETENTION_INTERVAL_MS = 60 * 60 * 1000;

async function runWebhookRetention(): Promise<void> {
  try {
    const [deletedDeliveries, deletedAlerts] = await Promise.all([
      purgeExpiredWebhookDeliveries(),
      purgeExpiredAlertHistory(),
    ]);
    if (deletedDeliveries > 0 || deletedAlerts > 0) {
      logger.info(
        `[Retention] Deleted ${deletedDeliveries} expired webhook deliveries and ${deletedAlerts} alert records`,
      );
    }
  } catch (error) {
    logger.warn("[WebhookRetention] Delivery cleanup failed", error);
  }
}

export async function startWorkers(): Promise<void> {
  logger.info("[Workers] Starting background workers...");
  await sorobanEventWorker.start();

  // Start stream runway alert worker (Issue #1190)
  runwayWorkerTimer = startStreamRunwayWorker();

  void runWebhookRetention();
  webhookRetentionTimer = setInterval(
    () => void runWebhookRetention(),
    WEBHOOK_RETENTION_INTERVAL_MS,
  );
}

export function stopWorkers(): void {
  sorobanEventWorker.stop();

  if (runwayWorkerTimer) {
    clearInterval(runwayWorkerTimer);
    runwayWorkerTimer = null;
    logger.info("[Workers] Stream runway worker stopped");
  }

  if (webhookRetentionTimer) {
    clearInterval(webhookRetentionTimer);
    webhookRetentionTimer = null;
    logger.info("[Workers] Webhook retention worker stopped");
  }
}
