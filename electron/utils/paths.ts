/**
 * Path Utilities
 * Cross-platform path resolution helpers
 */
import { app } from 'electron';
import { join, dirname } from 'path';
import { homedir } from 'os';
import { existsSync, mkdirSync } from 'fs';

/**
 * Expand ~ to home directory
 */
export function expandPath(path: string): string {
  if (path.startsWith('~')) {
    return path.replace('~', homedir());
  }
  return path;
}

/**
 * Get OpenClaw config directory
 */
export function getOpenClawConfigDir(): string {
  return join(homedir(), '.openclaw');
}

/**
 * Get ClawX config directory
 */
export function getClawXConfigDir(): string {
  return join(homedir(), '.clawx');
}

/**
 * Get ClawX logs directory
 */
export function getLogsDir(): string {
  return join(app.getPath('userData'), 'logs');
}

/**
 * Get ClawX data directory
 */
export function getDataDir(): string {
  return app.getPath('userData');
}

/**
 * Ensure directory exists
 */
export function ensureDir(dir: string): void {
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
}

/**
 * Get resources directory (for bundled assets)
 */
export function getResourcesDir(): string {
  if (app.isPackaged) {
    return join(process.resourcesPath, 'resources');
  }
  return join(__dirname, '../../resources');
}

/**
 * Get preload script path
 */
export function getPreloadPath(): string {
  return join(__dirname, '../preload/index.js');
}

// Use createRequire to resolve dependencies
import { createRequire } from 'module';
const require = createRequire(import.meta.url);

/**
 * Get OpenClaw package directory (from node_modules)
 */
export function getOpenClawDir(): string {
  try {
    // Resolve the package root using the package.json location
    // This works for both dev (node_modules) and prod (bundled)
    let pkgPath = require.resolve('openclaw/package.json');

    // Use the path within ASAR directly for utilityProcess.fork
    // This allows Electron's module loader to find dependencies (like chalk) inside app.asar
    return dirname(pkgPath);
  } catch (error) {
    // Fallback logic if resolution fails
    if (app.isPackaged) {
      // In packaged app, dependencies should be in resources/app.asar.unpacked/node_modules
      return join(process.resourcesPath, 'app.asar.unpacked', 'node_modules', 'openclaw');
    }
    // In dev, standard node_modules
    return join(process.cwd(), 'node_modules', 'openclaw');
  }
}

/**
 * Get OpenClaw entry script path (dist/entry.js)
 */
export function getOpenClawEntryPath(): string {
  return join(getOpenClawDir(), 'dist', 'entry.js');
}

/**
 * Check if OpenClaw is installed (replaces submodule check)
 */
export function isOpenClawSubmodulePresent(): boolean {
  // Always true as we use the managed dependency
  return true;
}

/**
 * Check if OpenClaw is built
 */
export function isOpenClawBuilt(): boolean {
  // Always true for installed dependency
  return true;
}

/**
 * Check if OpenClaw has node_modules installed
 */
export function isOpenClawInstalled(): boolean {
  try {
    return existsSync(getOpenClawEntryPath());
  } catch {
    return false;
  }
}

/**
 * Get OpenClaw status for environment check
 */
export interface OpenClawStatus {
  submoduleExists: boolean;
  isInstalled: boolean;
  isBuilt: boolean;
  entryPath: string;
  dir: string;
}

export function getOpenClawStatus(): OpenClawStatus {
  const dir = getOpenClawDir();
  return {
    submoduleExists: isOpenClawSubmodulePresent(),
    isInstalled: isOpenClawInstalled(),
    isBuilt: isOpenClawBuilt(),
    entryPath: getOpenClawEntryPath(),
    dir,
  };
}
