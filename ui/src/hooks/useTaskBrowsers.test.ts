// @vitest-environment jsdom
import { beforeEach, expect, it } from "vitest";
import { claimBrowserArrival } from "./useTaskBrowsers";
beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
});
it("opens a follow-up browser even after this task's first browser was dismissed", () => {
  localStorage.setItem("paperclip:browser-arrival:user:task", "1");
  expect(claimBrowserArrival("user", "task", "first")).toBe(true);
  expect(claimBrowserArrival("user", "task", "first")).toBe(false);
  expect(claimBrowserArrival("user", "task", "next")).toBe(true);
  expect(claimBrowserArrival("user", "task", "next")).toBe(false);
});
it("keeps arrival choices scoped to the user and task", () => {
  expect(claimBrowserArrival("alice", "task", "browser")).toBe(true);
  expect(claimBrowserArrival("bob", "task", "browser")).toBe(true);
  expect(claimBrowserArrival("alice", "another-task", "browser")).toBe(true);
});
