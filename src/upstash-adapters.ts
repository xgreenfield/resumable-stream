import { Redis } from "@upstash/redis";
import { Publisher, Subscriber } from "./types";

type RedisSubscriber = ReturnType<typeof Redis.prototype.subscribe>;
const activeSubscriptions: Map<string, RedisSubscriber> = new Map();

/**
 * Creates a Subscriber adapter for a Redis client.
 * @param client - The Redis client to adapt
 * @returns A Subscriber interface compatible with the resumable stream
 */
export function createSubscriberAdapter(client: Redis): Subscriber {
  const adapter: Subscriber = {
    connect: () => Promise.resolve(),
    subscribe: async function (channel: string, callback: (message: string) => void) {
      const subscriber = activeSubscriptions.get(channel) ?? client.subscribe(channel);
      console.log("Subscribing to channel", channel, activeSubscriptions.get(channel));
      subscriber.on("message", (message) => {
        if (message.channel === channel) {
          if (typeof message.message === "string") {
            callback(message.message);
          } else {
            console.error("Received non-string message", message);
            try {
              const stringifiedMessage = JSON.stringify(message.message);
              callback(stringifiedMessage);
            } catch (e) {
              console.error("Error stringifying message", e);
            }
          }
        }
      });
      subscriber.on("subscribe", (channel) => {
        console.log("SUBSCRIBER SUBSCRIBED TO CHANNEL", channel);
      });
      subscriber.on("unsubscribe", (channel) => {
        console.log("SUBSCRIBER UNSUBSCRIBED FROM CHANNEL", channel);
      });
      subscriber.on("error", (error) => {
        console.error("SUBSCRIBER ERROR", error);
      });
      activeSubscriptions.set(channel, subscriber);
    },
    unsubscribe: async (channel: string) => {
      const subscriber = activeSubscriptions.get(channel);
      console.log("Unsubscribing from channel", channel, subscriber);
      if (subscriber) {
        await subscriber.unsubscribe();
        activeSubscriptions.delete(channel);
      }
    },
  };
  return adapter;
}

/**
 * Creates a Publisher adapter for a Redis client.
 * @param client - The Redis client to adapt
 * @returns A Publisher interface compatible with the resumable stream
 */
export function createPublisherAdapter(client: Redis): Publisher {
  const adapter: Publisher = {
    connect: () => Promise.resolve(),
    publish: (channel: string, message: string | Buffer) => {
      if (typeof message !== "string") {
        console.warn("Attempted to publish non-string message", message);
        return client.publish(channel, JSON.stringify(message));
      } else {
        return client.publish(channel, message);
      }
    },
    set: (key: string, value: string | Buffer, options?: { EX?: number }) => {
      if (options?.EX) {
        return client.setex(key, options.EX, value);
      }
      return client.set(key, value);
    },
    get: (key: string) => client.get(key),
    incr: (key: string) => client.incr(key),
  };
  return adapter;
}
