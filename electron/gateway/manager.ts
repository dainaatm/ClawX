/**
 * Gateway Process Manager
 * Manages the OpenClaw Gateway process lifecycle
 */
import { app, utilityProcess, UtilityProcess } from 'electron';
import { EventEmitter } from 'events';
import WebSocket from 'ws';
import { PORTS } from '../utils/config';
import {
  getOpenClawEntryPath,
  isOpenClawInstalled,
  getOpenClawConfigDir,
  ensureDir
} from '../utils/paths';
import { getSetting } from '../utils/store';
import { getApiKey } from '../utils/secure-storage';
import { getProviderEnvVar } from '../utils/openclaw-auth';
import { GatewayEventType, JsonRpcNotification, isNotification, isResponse } from './protocol';
import { logger } from '../utils/logger';

/**
 * Gateway connection status
 */
export interface GatewayStatus {
  state: 'stopped' | 'starting' | 'running' | 'error' | 'reconnecting';
  port: number;
  pid?: number;
  uptime?: number;
  error?: string;
  connectedAt?: number;
  version?: string;
  reconnectAttempts?: number;
}

/**
 * Gateway Manager Events
 */
export interface GatewayManagerEvents {
  status: (status: GatewayStatus) => void;
  message: (message: unknown) => void;
  notification: (notification: JsonRpcNotification) => void;
  exit: (code: number | null) => void;
  error: (error: Error) => void;
  'channel:status': (data: { channelId: string; status: string }) => void;
  'chat:message': (data: { message: unknown }) => void;
}

/**
 * Reconnection configuration
 */
interface ReconnectConfig {
  maxAttempts: number;
  baseDelay: number;
  maxDelay: number;
}

const DEFAULT_RECONNECT_CONFIG: ReconnectConfig = {
  maxAttempts: 10,
  baseDelay: 1000,
  maxDelay: 30000,
};

/**
 * Gateway Manager
 * Handles starting, stopping, and communicating with the OpenClaw Gateway
 */
export class GatewayManager extends EventEmitter {
  private ws: WebSocket | null = null;
  private process: UtilityProcess | null = null;
  private status: GatewayStatus = { state: 'stopped', port: PORTS.OPENCLAW_GATEWAY };
  private reconnectTimer: NodeJS.Timeout | null = null;
  private pingInterval: NodeJS.Timeout | null = null;
  private healthCheckInterval: NodeJS.Timeout | null = null;
  private reconnectAttempts = 0;
  private reconnectConfig: ReconnectConfig;
  private shouldReconnect = true;
  private pendingRequests: Map<string, {
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
    timeout: NodeJS.Timeout;
  }> = new Map();

  constructor(config?: Partial<ReconnectConfig>) {
    super();
    this.reconnectConfig = { ...DEFAULT_RECONNECT_CONFIG, ...config };
  }

  /**
   * Get current Gateway status
   */
  getStatus(): GatewayStatus {
    return { ...this.status };
  }

  /**
   * Check if Gateway is connected and ready
   */
  isConnected(): boolean {
    return this.status.state === 'running' && this.ws?.readyState === WebSocket.OPEN;
  }

  /**
   * Start Gateway process
   */
  async start(): Promise<void> {
    if (this.status.state === 'running') {
      return;
    }

    this.shouldReconnect = true;
    this.reconnectAttempts = 0;
    this.setStatus({ state: 'starting', reconnectAttempts: 0 });

    try {
      // Check if Gateway is already running
      const existing = await this.findExistingGateway();
      if (existing) {
        logger.info('Found existing Gateway on port', existing.port);
        await this.connect(existing.port);
        this.startHealthCheck();
        return;
      }

      // Start new Gateway process
      await this.startProcess();

      // Wait for Gateway to be ready
      await this.waitForReady();

      // Connect WebSocket
      await this.connect(this.status.port);

      // Start health monitoring
      this.startHealthCheck();

    } catch (error) {
      logger.error('Gateway start failed:', error);
      this.setStatus({ state: 'error', error: String(error) });
      throw error;
    }
  }

  /**
   * Stop Gateway process
   */
  async stop(): Promise<void> {
    // Disable auto-reconnect
    this.shouldReconnect = false;

    // Clear all timers
    this.clearAllTimers();

    // Close WebSocket
    if (this.ws) {
      this.ws.close(1000, 'Gateway stopped by user');
      this.ws = null;
    }

    // Kill process
    if (this.process) {
      this.process.kill();
      // Force kill after timeout if still alive? UtilityProcess.kill is usually immediate.
      this.process = null;
    }

    // Reject all pending requests
    for (const [, request] of this.pendingRequests) {
      clearTimeout(request.timeout);
      request.reject(new Error('Gateway stopped'));
    }
    this.pendingRequests.clear();

    this.setStatus({ state: 'stopped', error: undefined });
  }

  /**
   * Restart Gateway process
   */
  async restart(): Promise<void> {
    logger.info('Restarting Gateway...');
    await this.stop();
    // Brief delay before restart
    await new Promise(resolve => setTimeout(resolve, 1000));
    await this.start();
  }

  /**
   * Clear all active timers
   */
  private clearAllTimers(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
    }
  }

  /**
   * Make an RPC call to the Gateway
   * Uses OpenClaw protocol format: { type: "req", id: "...", method: "...", params: {...} }
   */
  async rpc<T>(method: string, params?: unknown, timeoutMs = 30000): Promise<T> {
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        reject(new Error('Gateway not connected'));
        return;
      }

      const id = crypto.randomUUID();

      // Set timeout for request
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(id);
        reject(new Error(`RPC timeout: ${method}`));
      }, timeoutMs);

      // Store pending request
      this.pendingRequests.set(id, {
        resolve: resolve as (value: unknown) => void,
        reject,
        timeout,
      });

      // Send request using OpenClaw protocol format
      const request = {
        type: 'req',
        id,
        method,
        params,
      };

      try {
        this.ws.send(JSON.stringify(request));
      } catch (error) {
        this.pendingRequests.delete(id);
        clearTimeout(timeout);
        reject(new Error(`Failed to send RPC request: ${error}`));
      }
    });
  }

  /**
   * Start health check monitoring
   */
  private startHealthCheck(): void {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
    }

    this.healthCheckInterval = setInterval(async () => {
      if (this.status.state !== 'running') {
        return;
      }

      try {
        const health = await this.checkHealth();
        if (!health.ok) {
          logger.warn('Gateway health check failed:', health.error);
          this.emit('error', new Error(health.error || 'Health check failed'));
        }
      } catch (error) {
        logger.error('Health check error:', error);
      }
    }, 30000); // Check every 30 seconds
  }

  /**
   * Check Gateway health via WebSocket ping
   * OpenClaw Gateway doesn't have an HTTP /health endpoint
   */
  async checkHealth(): Promise<{ ok: boolean; error?: string; uptime?: number }> {
    try {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        const uptime = this.status.connectedAt
          ? Math.floor((Date.now() - this.status.connectedAt) / 1000)
          : undefined;
        return { ok: true, uptime };
      }
      return { ok: false, error: 'WebSocket not connected' };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  }

  /**
   * Find existing Gateway process by attempting a WebSocket connection
   */
  private async findExistingGateway(): Promise<{ port: number } | null> {
    try {
      const port = PORTS.OPENCLAW_GATEWAY;
      // Try a quick WebSocket connection to check if gateway is listening
      return await new Promise<{ port: number } | null>((resolve) => {
        const testWs = new WebSocket(`ws://localhost:${port}/ws`);
        const timeout = setTimeout(() => {
          testWs.close();
          resolve(null);
        }, 2000);

        testWs.on('open', () => {
          clearTimeout(timeout);
          testWs.close();
          resolve({ port });
        });

        testWs.on('error', () => {
          clearTimeout(timeout);
          resolve(null);
        });
      });
    } catch {
      // Gateway not running
    }

    return null;
  }

  /**
   * Start Gateway process
   * Uses installed OpenClaw dependency
   */
  private async startProcess(): Promise<void> {
    const entryScript = getOpenClawEntryPath();

    // Verify dependencies are installed
    if (!isOpenClawInstalled()) {
      throw new Error(
        'OpenClaw dependency not found. Please run: pnpm install'
      );
    }

    // Get or generate gateway token
    const gatewayToken = await getSetting('gatewayToken');
    logger.info('Using gateway token:', gatewayToken.substring(0, 10) + '...');

    // Ensure config directory exists for CWD
    const workDir = getOpenClawConfigDir();
    ensureDir(workDir);

    // Always run using Electron node executable pointing to the installed entry script
    const command = process.execPath;
    const args = [
      'gateway',
      '--port', String(this.status.port),
      '--token', gatewayToken,
      '--allow-unconfigured'
    ];

    // Add --dev flag in development for verbose logging if needed, or based on user preference
    // Defaults to production-like behavior (less verbose) unless debug is on?
    // We'll keep --dev if it was generally useful, or remove it if it implies 'development mode' of the gateway itself (reloading etc).
    // The installed package is separate, so 'dev' mode might just mean log level.
    if (!app.isPackaged) {
      args.push('--dev');
    }

    console.log(`Spawning Gateway: ${command} ${args.join(' ')}`);
    logger.info(`Spawning Gateway at ${workDir}`);
    logger.debug(`Command: ${command} ${args.join(' ')}`);

    // Load provider API keys from secure storage to pass as environment variables
    const providerEnv: Record<string, string> = {};
    const providerTypes = ['anthropic', 'openai', 'google', 'openrouter'];
    for (const providerType of providerTypes) {
      try {
        const key = await getApiKey(providerType);
        if (key) {
          const envVar = getProviderEnvVar(providerType);
          if (envVar) {
            providerEnv[envVar] = key;
            logger.debug(`Loaded API key for ${providerType} -> ${envVar}`);
          }
        }
      } catch (err) {
        logger.warn(`Failed to load API key for ${providerType}:`, err);
      }
    }

    logger.info(`Starting OpenClaw gateway from: ${entryScript}`);
    logger.info(`Workspace directory: ${workDir}`);

    try {
      this.process = utilityProcess.fork(entryScript, args, {
        cwd: workDir,
        stdio: 'pipe',
        env: {
          ...process.env,
          // Provider API keys
          ...providerEnv,
          // Also set token via environment variable as fallback
          OPENCLAW_GATEWAY_TOKEN: gatewayToken,
          // Ensure OPENCLAW_SKIP_CHANNELS is NOT set so channels auto-start
          OPENCLAW_SKIP_CHANNELS: '',
          CLAWDBOT_SKIP_CHANNELS: '',
        },
      });

      this.process.stdout?.on('data', (data: Buffer) => {
        const line = data.toString().trim();
        if (line) {
          logger.info(`[GATEWAY] ${line}`);
        }
      });

      this.process.stderr?.on('data', (data: Buffer) => {
        const line = data.toString().trim();
        if (line) {
          logger.error(`[GATEWAY-STDERR] ${line}`);
        }
      });

      this.process.on('exit', (code) => {
        logger.info(`Gateway process exited with code ${code}`);
        this.process = null;
        if (this.status.state !== 'stopped') {
          this.setStatus({
            state: 'error',
            error: `Gateway process exited with code ${code}`,
          });
          this.scheduleReconnect();
        }
      });

      if (this.process.pid) {
        this.setStatus({ pid: this.process.pid });
      }
    } catch (error) {
      logger.error('Failed to spawn gateway process:', error);
      throw error;
    }
  }

  /**
   * Wait for Gateway to be ready by checking if the port is accepting connections
   */
  private async waitForReady(retries = 30, interval = 1000): Promise<void> {
    for (let i = 0; i < retries; i++) {
      try {
        // Try a quick WebSocket connection to see if the gateway is listening
        const ready = await new Promise<boolean>((resolve) => {
          const testWs = new WebSocket(`ws://localhost:${this.status.port}/ws`);
          const timeout = setTimeout(() => {
            testWs.close();
            resolve(false);
          }, 1000);

          testWs.on('open', () => {
            clearTimeout(timeout);
            testWs.close();
            resolve(true);
          });

          testWs.on('error', () => {
            clearTimeout(timeout);
            resolve(false);
          });
        });

        if (ready) {
          return;
        }
      } catch {
        // Gateway not ready yet
      }

      await new Promise((resolve) => setTimeout(resolve, interval));
    }

    throw new Error('Gateway failed to start');
  }

  /**
   * Connect WebSocket to Gateway
   */
  private async connect(port: number): Promise<void> {
    // Get token for WebSocket authentication
    const gatewayToken = await getSetting('gatewayToken');

    return new Promise((resolve, reject) => {
      // WebSocket URL (token will be sent in connect handshake, not URL)
      const wsUrl = `ws://localhost:${port}/ws`;

      this.ws = new WebSocket(wsUrl);
      let handshakeComplete = false;

      this.ws.on('open', async () => {
        logger.info('WebSocket opened, sending connect handshake...');

        // Send proper connect handshake as required by OpenClaw Gateway protocol
        // The Gateway expects: { type: "req", id: "...", method: "connect", params: ConnectParams }
        const connectId = `connect-${Date.now()}`;
        const connectFrame = {
          type: 'req',
          id: connectId,
          method: 'connect',
          params: {
            minProtocol: 3,
            maxProtocol: 3,
            client: {
              id: 'gateway-client',
              displayName: 'ClawX',
              version: '0.1.0',
              platform: process.platform,
              mode: 'ui',
            },
            auth: {
              token: gatewayToken,
            },
            caps: [],
            role: 'operator',
            scopes: [],
          },
        };

        logger.debug('Sending connect handshake:', JSON.stringify(connectFrame));
        this.ws?.send(JSON.stringify(connectFrame));

        // Store pending connect request
        const connectTimeout = setTimeout(() => {
          if (!handshakeComplete) {
            logger.error('Connect handshake timeout');
            reject(new Error('Connect handshake timeout'));
            this.ws?.close();
          }
        }, 10000);

        this.pendingRequests.set(connectId, {
          resolve: (_result) => {
            clearTimeout(connectTimeout);
            handshakeComplete = true;
            logger.info('WebSocket handshake complete, gateway connected');
            this.setStatus({
              state: 'running',
              port,
              connectedAt: Date.now(),
            });
            this.startPing();
            resolve();
          },
          reject: (error) => {
            clearTimeout(connectTimeout);
            logger.error('Connect handshake failed:', error);
            reject(error);
          },
          timeout: connectTimeout,
        });
      });

      this.ws.on('message', (data) => {
        try {
          const message = JSON.parse(data.toString());
          this.handleMessage(message);
        } catch (error) {
          logger.error('Failed to parse WebSocket message:', error);
        }
      });

      this.ws.on('close', (code, reason) => {
        const reasonStr = reason?.toString() || 'unknown';
        logger.info(`WebSocket disconnected: code=${code}, reason=${reasonStr}`);
        if (!handshakeComplete) {
          reject(new Error(`WebSocket closed before handshake: ${reasonStr}`));
          return;
        }
        if (this.status.state === 'running') {
          this.setStatus({ state: 'stopped' });
          this.scheduleReconnect();
        }
      });

      this.ws.on('error', (error) => {
        logger.error('WebSocket error:', error);
        if (!handshakeComplete) {
          reject(error);
        }
      });
    });
  }

  /**
   * Handle incoming WebSocket message
   */
  private handleMessage(message: unknown): void {
    if (typeof message !== 'object' || message === null) {
      logger.warn('Received non-object message:', message);
      return;
    }

    const msg = message as Record<string, unknown>;

    // Handle OpenClaw protocol response format: { type: "res", id: "...", ok: true/false, ... }
    if (msg.type === 'res' && typeof msg.id === 'string') {
      if (this.pendingRequests.has(msg.id)) {
        const request = this.pendingRequests.get(msg.id)!;
        clearTimeout(request.timeout);
        this.pendingRequests.delete(msg.id);

        if (msg.ok === false || msg.error) {
          const errorObj = msg.error as { message?: string; code?: number } | undefined;
          const errorMsg = errorObj?.message || JSON.stringify(msg.error) || 'Unknown error';
          request.reject(new Error(errorMsg));
        } else {
          request.resolve(msg.payload ?? msg);
        }
        return;
      }
    }

    // Handle OpenClaw protocol event format: { type: "event", event: "...", payload: {...} }
    if (msg.type === 'event' && typeof msg.event === 'string') {
      this.handleProtocolEvent(msg.event, msg.payload);
      return;
    }

    // Fallback: Check if this is a JSON-RPC 2.0 response (legacy support)
    if (isResponse(message) && message.id && this.pendingRequests.has(String(message.id))) {
      const request = this.pendingRequests.get(String(message.id))!;
      clearTimeout(request.timeout);
      this.pendingRequests.delete(String(message.id));

      if (message.error) {
        const errorMsg = typeof message.error === 'object'
          ? (message.error as { message?: string }).message || JSON.stringify(message.error)
          : String(message.error);
        request.reject(new Error(errorMsg));
      } else {
        request.resolve(message.result);
      }
      return;
    }

    // Check if this is a JSON-RPC notification (server-initiated event)
    if (isNotification(message)) {
      this.handleNotification(message);
      return;
    }

    // Emit generic message for other handlers
    this.emit('message', message);
  }

  /**
   * Handle OpenClaw protocol events
   */
  private handleProtocolEvent(event: string, payload: unknown): void {
    // Map OpenClaw events to our internal event types
    switch (event) {
      case 'tick':
        // Heartbeat tick, ignore
        break;
      case 'chat':
        this.emit('chat:message', { message: payload });
        break;
      case 'channel.status':
        this.emit('channel:status', payload as { channelId: string; status: string });
        break;
      default:
        // Forward unknown events as generic notifications
        this.emit('notification', { method: event, params: payload });
    }
  }

  /**
   * Handle server-initiated notifications
   */
  private handleNotification(notification: JsonRpcNotification): void {
    this.emit('notification', notification);

    // Route specific events
    switch (notification.method) {
      case GatewayEventType.CHANNEL_STATUS_CHANGED:
        this.emit('channel:status', notification.params as { channelId: string; status: string });
        break;

      case GatewayEventType.MESSAGE_RECEIVED:
        this.emit('chat:message', notification.params as { message: unknown });
        break;

      case GatewayEventType.ERROR: {
        const errorData = notification.params as { message?: string };
        this.emit('error', new Error(errorData.message || 'Gateway error'));
        break;
      }

      default:
        // Unknown notification type, just log it
        logger.debug('Unknown Gateway notification:', notification.method);
    }
  }

  /**
   * Start ping interval to keep connection alive
   */
  private startPing(): void {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
    }

    this.pingInterval = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.ping();
      }
    }, 30000);
  }

  /**
   * Schedule reconnection attempt with exponential backoff
   */
  private scheduleReconnect(): void {
    if (!this.shouldReconnect) {
      logger.info('Auto-reconnect disabled, not scheduling reconnect');
      return;
    }

    if (this.reconnectTimer) {
      return;
    }

    if (this.reconnectAttempts >= this.reconnectConfig.maxAttempts) {
      logger.error(`Max reconnection attempts (${this.reconnectConfig.maxAttempts}) reached`);
      this.setStatus({
        state: 'error',
        error: 'Failed to reconnect after maximum attempts',
        reconnectAttempts: this.reconnectAttempts
      });
      return;
    }

    // Calculate delay with exponential backoff
    const delay = Math.min(
      this.reconnectConfig.baseDelay * Math.pow(2, this.reconnectAttempts),
      this.reconnectConfig.maxDelay
    );

    this.reconnectAttempts++;
    logger.info(`Scheduling reconnect attempt ${this.reconnectAttempts} in ${delay}ms`);

    this.setStatus({
      state: 'reconnecting',
      reconnectAttempts: this.reconnectAttempts
    });

    this.reconnectTimer = setTimeout(async () => {
      this.reconnectTimer = null;
      try {
        // Try to find existing Gateway first
        const existing = await this.findExistingGateway();
        if (existing) {
          await this.connect(existing.port);
          this.reconnectAttempts = 0;
          this.startHealthCheck();
          return;
        }

        // Otherwise restart the process
        await this.startProcess();
        await this.waitForReady();
        await this.connect(this.status.port);
        this.reconnectAttempts = 0;
        this.startHealthCheck();
      } catch (error) {
        console.error('Reconnection failed:', error);
        this.scheduleReconnect();
      }
    }, delay);
  }

  /**
   * Update status and emit event
   */
  private setStatus(update: Partial<GatewayStatus>): void {
    const previousState = this.status.state;
    this.status = { ...this.status, ...update };

    // Calculate uptime if connected
    if (this.status.state === 'running' && this.status.connectedAt) {
      this.status.uptime = Date.now() - this.status.connectedAt;
    }

    this.emit('status', this.status);

    // Log state transitions
    if (previousState !== this.status.state) {
      console.log(`Gateway state: ${previousState} -> ${this.status.state}`);
    }
  }
}
