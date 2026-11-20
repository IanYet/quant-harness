export type SidebarVisibility = "visible" | "hidden" | "ephemeral";
export type AgentProfile = "user" | "cron_review" | "script_triggered";

export interface AdapterStartOptions {
  prompt: string;
  profile: AgentProfile;
  visibility: SidebarVisibility;
  previousVisibility?: SidebarVisibility;
  title?: string;
  maxTurns: number;
  timeoutSec: number;
  mcpEnv: Record<string, string>;
  signal: AbortSignal;
  onSession(id: string): void;
}
export type AdapterResult = { summary: string; costUsd?: number };
export interface AgentHarnessAdapter {
  readonly name: string;
  startSession(options: AdapterStartOptions): Promise<AdapterResult & { backendSessionId: string }>;
  resumeSession(backendSessionId: string, message: string, options: Omit<AdapterStartOptions, "prompt">): Promise<AdapterResult>;
  setVisibility(backendSessionId: string, visibility: Exclude<SidebarVisibility, "ephemeral">): Promise<void>;
  cancel(backendSessionId: string): Promise<void>;
  health(): Promise<{ ok: boolean; detail?: string }>;
  close(): void | Promise<void>;
}
