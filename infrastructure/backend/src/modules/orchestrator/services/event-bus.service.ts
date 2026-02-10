/**
 * Event Bus Service
 *
 * Redis Pub/Sub wrapper for real-time event distribution:
 * - Event publishing to channels
 * - Event subscription with handlers
 * - Request-response pattern support
 * - Broadcast capabilities
 */

import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import {
  EventChannel,
  EVENT_CHANNELS,
  OrchestratorEvent,
} from '../types/message.types';

export type EventHandler = (event: OrchestratorEvent) => void | Promise<void>;

interface Subscription {
  id: string;
  channel: EventChannel;
  handler: EventHandler;
  unsubscribe: () => void;
}

interface PendingRequest {
  resolve: (response: any) => void;
  reject: (error: Error) => void;
  timeout: NodeJS.Timeout;
}

@Injectable()
export class EventBusService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(EventBusService.name);

  private publisher: Redis;
  private subscriber: Redis;
  private subscriptions: Map<string, Subscription> = new Map();
  private channelHandlers: Map<EventChannel, Set<string>> = new Map();
  private pendingRequests: Map<string, PendingRequest> = new Map();

  constructor(private configService: ConfigService) {}

  async onModuleInit() {
    const redisConfig = {
      host: this.configService.get('REDIS_HOST', 'localhost'),
      port: this.configService.get('REDIS_PORT', 6379),
    };

    this.publisher = new Redis(redisConfig);
    this.subscriber = new Redis(redisConfig);

    // Handle incoming messages
    this.subscriber.on('message', (channel: string, message: string) => {
      this.handleMessage(channel as EventChannel, message);
    });

    this.logger.log('Event Bus initialized');
  }

  async onModuleDestroy() {
    // Unsubscribe all
    for (const sub of this.subscriptions.values()) {
      sub.unsubscribe();
    }

    await this.publisher?.quit();
    await this.subscriber?.quit();
  }

  /**
   * Publish an event to a channel
   */
  async publish(channel: EventChannel, payload: Record<string, any>, source: string = 'system'): Promise<void> {
    const event: OrchestratorEvent = {
      channel,
      payload,
      timestamp: new Date(),
      source,
    };

    await this.publisher.publish(channel, JSON.stringify(event));
    this.logger.debug(`Published event to ${channel}`);
  }

  /**
   * Subscribe to a channel
   */
  subscribe(channel: EventChannel, handler: EventHandler): Subscription {
    const subscriptionId = uuidv4();

    // Add to channel handlers
    if (!this.channelHandlers.has(channel)) {
      this.channelHandlers.set(channel, new Set());
      // Subscribe to Redis channel
      this.subscriber.subscribe(channel);
    }
    this.channelHandlers.get(channel)!.add(subscriptionId);

    const subscription: Subscription = {
      id: subscriptionId,
      channel,
      handler,
      unsubscribe: () => this.unsubscribe(subscriptionId),
    };

    this.subscriptions.set(subscriptionId, subscription);
    this.logger.debug(`Subscribed to ${channel} (${subscriptionId})`);

    return subscription;
  }

  /**
   * Unsubscribe from a channel
   */
  private unsubscribe(subscriptionId: string): void {
    const subscription = this.subscriptions.get(subscriptionId);
    if (!subscription) return;

    // Remove from channel handlers
    const handlers = this.channelHandlers.get(subscription.channel);
    if (handlers) {
      handlers.delete(subscriptionId);
      if (handlers.size === 0) {
        this.channelHandlers.delete(subscription.channel);
        // Unsubscribe from Redis channel
        this.subscriber.unsubscribe(subscription.channel);
      }
    }

    this.subscriptions.delete(subscriptionId);
    this.logger.debug(`Unsubscribed from ${subscription.channel} (${subscriptionId})`);
  }

  /**
   * Send a request and wait for response (request-response pattern)
   */
  async request(
    targetChannel: EventChannel,
    payload: Record<string, any>,
    timeoutMs: number = 30000,
  ): Promise<any> {
    const requestId = uuidv4();
    const responseChannel = `${targetChannel}.response.${requestId}` as EventChannel;

    return new Promise((resolve, reject) => {
      // Set up timeout
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(requestId);
        this.subscriber.unsubscribe(responseChannel);
        reject(new Error(`Request timeout after ${timeoutMs}ms`));
      }, timeoutMs);

      // Store pending request
      this.pendingRequests.set(requestId, { resolve, reject, timeout });

      // Subscribe to response channel
      this.subscriber.subscribe(responseChannel);

      // Publish request
      this.publish(targetChannel, {
        ...payload,
        _requestId: requestId,
        _responseChannel: responseChannel,
      });
    });
  }

  /**
   * Respond to a request
   */
  async respond(responseChannel: string, response: any): Promise<void> {
    await this.publisher.publish(responseChannel, JSON.stringify({
      channel: responseChannel,
      payload: response,
      timestamp: new Date(),
      source: 'response',
    }));
  }

  /**
   * Broadcast event to all subscribers
   */
  async broadcast(payload: Record<string, any>, source: string = 'system'): Promise<void> {
    await this.publish(EVENT_CHANNELS.AGENT_MESSAGE, {
      ...payload,
      _broadcast: true,
    }, source);
  }

  /**
   * Handle incoming message
   */
  private handleMessage(channel: EventChannel, message: string): void {
    try {
      const event: OrchestratorEvent = JSON.parse(message);

      // Check for response to pending request
      if (channel.includes('.response.')) {
        const requestId = channel.split('.response.')[1];
        const pending = this.pendingRequests.get(requestId);
        if (pending) {
          clearTimeout(pending.timeout);
          this.pendingRequests.delete(requestId);
          this.subscriber.unsubscribe(channel);
          pending.resolve(event.payload);
          return;
        }
      }

      // Dispatch to handlers
      const handlers = this.channelHandlers.get(channel);
      if (handlers) {
        for (const subscriptionId of handlers) {
          const subscription = this.subscriptions.get(subscriptionId);
          if (subscription) {
            try {
              subscription.handler(event);
            } catch (err) {
              this.logger.error(`Handler error for ${channel}: ${err}`);
            }
          }
        }
      }
    } catch (err) {
      this.logger.error(`Failed to parse message on ${channel}: ${err}`);
    }
  }

  /**
   * Get all available channels
   */
  getAvailableChannels(): EventChannel[] {
    return Object.values(EVENT_CHANNELS);
  }
}
