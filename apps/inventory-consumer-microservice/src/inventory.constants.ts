/** Key per-product; value = orderId. Redis lock is only a mutex during reserve (released right after); the 15 min "lock" is the reservation in Postgres (expiresAt). */
export const LOCK_PREFIX = 'product-lock:';

export const CONSUMER = 'inventory-service';

/** Short TTL: mutex only during reserve; if process crashes lock auto-expires */
export const LOCK_TTL_SECONDS = 30;

/** 15 min: reservation linked to order in DB (expiresAt); payment must complete before or job releases */
export const RESERVATION_EXPIRY_SECONDS = 900;

export const LOCK_RETRY_BASE_MS = 200;

/** ~16–24s max wait with jitter so burst of same-product orders can queue */
export const LOCK_RETRY_ATTEMPTS = 80;

/** Every 1 min, release DB reservations that passed expiresAt */
export const EXPIRED_RESERVATION_POLL_MS = 60_000;

export const OUT_OF_STOCK_CODE = 'OUT_OF_STOCK';

/** Redis lock so only one instance runs the expired-reservations job (TTL < poll interval). */
export const EXPIRY_JOB_LOCK_KEY = 'inventory:expired-reservations:job-lock';
export const EXPIRY_JOB_LOCK_TTL_SECONDS = 55;
