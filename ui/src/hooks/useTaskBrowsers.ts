import { useQuery } from "@tanstack/react-query";
import { browserUseApi } from "@/api/browser-use";
export function useTaskBrowsers(issueId?: string) {
  return useQuery({
    queryKey: ["task-browsers", issueId],
    queryFn: () => browserUseApi.list(issueId!),
    enabled: Boolean(issueId),
    refetchInterval: 3000,
    retry: false,
  });
}

/** Open each new session once per browser tab; polling never steals focus back. */
export function claimBrowserArrival(
  accountScope: string,
  issueId: string,
  sessionId: string,
): boolean {
  const key = `paperclip:browser-arrival:v2:${accountScope}:${issueId}:${sessionId}`;
  try {
    if (sessionStorage.getItem(key)) return false;
    sessionStorage.setItem(key, "1");
  } catch {
    return false;
  }
  return true;
}
