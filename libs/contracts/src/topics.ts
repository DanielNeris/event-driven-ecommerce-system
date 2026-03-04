/**
 * Central place for Kafka topic names (e-commerce POC).
 * Partition key: orderId.
 */

export const TOPICS = {
  ORDER_EVENTS: 'order.events',
  INVENTORY_EVENTS: 'inventory.events',
  PAYMENT_EVENTS: 'payment.events',
  SHIPPING_EVENTS: 'shipping.events',
  NOTIFICATION_EVENTS: 'notification.events',
  ORDER_DLQ: 'order.dlq',
  INVENTORY_DLQ: 'inventory.dlq',
  PAYMENT_DLQ: 'payment.dlq',
  SHIPPING_DLQ: 'shipping.dlq',
  NOTIFICATION_DLQ: 'notification.dlq',
} as const;

export type TopicName = (typeof TOPICS)[keyof typeof TOPICS];

export const EVENT_TYPES = {
  // order.events
  OrderCreated: 'OrderCreated',
  OrderCancelled: 'OrderCancelled',
  OrderConfirmed: 'OrderConfirmed',
  // inventory.events
  StockReserved: 'StockReserved',
  StockReservationFailed: 'StockReservationFailed',
  StockReleased: 'StockReleased',
  // payment.events
  PaymentAuthorized: 'PaymentAuthorized',
  PaymentFailed: 'PaymentFailed',
  PaymentCaptured: 'PaymentCaptured',
  // shipping.events
  ShipmentCreated: 'ShipmentCreated',
  ShipmentFailed: 'ShipmentFailed',
  // notification.events
  NotificationRequested: 'NotificationRequested',
  NotificationSent: 'NotificationSent',
  Failed: 'Failed',
} as const;

export type EventType = (typeof EVENT_TYPES)[keyof typeof EVENT_TYPES];
