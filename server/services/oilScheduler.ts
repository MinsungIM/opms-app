import cron from "node-cron";
import { downloadOilPriceCSV } from "./oilScraper";
import {
  parseOilPriceCSV,
  toInsertOilPriceRaw,
  type OilPriceRow,
} from "./oilParser";
import { runAnalysis } from "./oilAnalyzer";
import { storage } from "../storage";
import { sendPushToAll } from "./pushService";
import { fetchFuelAveragesWithRetry, setCachedFuelAverages } from "./opinetApi";
import { scrapeWeeklySupplyPrices } from "./weeklySupplyScraper";
import { isKoreanHoliday } from "./koreanHoliday";
import { db } from "../db";
import { sql } from "drizzle-orm";
import { invalidateAvailableDatesCache } from "../cache";

function getDateStr(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}${m}${d}`;
}

function getKSTNow(): Date {
  return new Date(Date.now() + 9 * 60 * 60 * 1000);
}

function getKSTDateStr(): string {
  return getDateStr(getKSTNow());
}

function getKSTHour(): number {
  return getKSTNow().getUTCHours();
}

// 오전/오후 수집 각각 중복 푸시 방지 (당일 기준)
let lastMorningPushDate = "";
let lastAfternoonPushDate = "";

async function sendUserPush(
  dateKey: string,
  message: string,
  slot: "morning" | "afternoon",
): Promise<void> {
  const guard =
    slot === "morning" ? lastMorningPushDate : lastAfternoonPushDate;
  if (guard === dateKey) {
    console.log(
      `[PushScheduler] ${slot === "morning" ? "오전" : "오후"} 푸시 이미 발송됨(${dateKey}), 건너뜀`,
    );
    return;
  }
  try {
    const subs = await storage.getAllPushSubscriptions();
    if (subs.length === 0) {
      console.log("[PushScheduler] 구독자 없음, 건너뜀");
      if (slot === "morning") lastMorningPushDate = dateKey;
      else lastAfternoonPushDate = dateKey;
      return;
    }
    const payload = {
      title: "유가 모니터링",
      body: message,
      icon: "/icon-192.png",
      url: "/oil-prices",
    };
    const { sent, failed, expiredEndpoints } = await sendPushToAll(
      subs,
      payload,
    );
    if (slot === "morning") lastMorningPushDate = dateKey;
    else lastAfternoonPushDate = dateKey;
    if (expiredEndpoints.length > 0) {
      await Promise.all(
        expiredEndpoints.map((ep) => storage.deletePushSubscription(ep)),
      );
      console.log(
        `[PushScheduler] 만료된 구독 ${expiredEndpoints.length}건 자동 삭제`,
      );
    }
    console.log(
      `[PushScheduler] ${slot === "morning" ? "오전" : "오후"} 푸시 발송 완료: 성공 ${sent}건, 실패 ${failed}건`,
    );
  } catch (err) {
    console.error("[PushScheduler] 푸시 발송 오류:", err);
  }
}

async function sendMasterPush(title: string, body: string): Promise<void> {
  try {
    const subs = await storage.getMasterPushSubscriptions();
    if (subs.length === 0) return;
    const payload = { title, body, icon: "/icon-192.png", url: "/oil-prices" };
    const { sent, failed, expiredEndpoints } = await sendPushToAll(
      subs,
      payload,
    );
    if (expiredEndpoints.length > 0) {
      await Promise.all(
        expiredEndpoints.map((ep) => storage.deletePushSubscription(ep)),
      );
      console.log(
        `[PushScheduler] 만료된 마스터 구독 ${expiredEndpoints.length}건 자동 삭제`,
      );
    }
    console.log(
      `[PushScheduler] 마스터 푸시 발송: 성공 ${sent}건, 실패 ${failed}건`,
    );
  } catch (err) {
    console.error("[PushScheduler] 마스터 푸시 발송 오류:", err);
  }
}

function dbRawToOilPriceRow(r: {
  stationId: string;
  stationName: string;
  address: string | null;
  region: string;
  sido: string;
  date: string;
  brand: string | null;
  isSelf: boolean;
  premiumGasoline: number | null;
  gasoline: number | null;
  diesel: number | null;
  kerosene: number | null;
}): OilPriceRow {
  return {
    stationId: r.stationId,
    stationName: r.stationName,
    address: r.address ?? "",
    region: r.region,
    sido: r.sido,
    date: r.date,
    brand: r.brand ?? "",
    isSelf: r.isSelf,
    premiumGasoline: r.premiumGasoline ?? null,
    gasoline: r.gasoline ?? null,
    diesel: r.diesel ?? null,
    kerosene: r.kerosene ?? null,
  };
}

async function runAnalysisWithDbRetry(
  allRows: OilPriceRow[],
  analysisToday: string,
  analysisYesterday: string,
  maxAttempts = 3,
  retryDelayMs = 30_000,
): Promise<number> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const analysisResults = runAnalysis(
        allRows,
        analysisToday,
        analysisYesterday,
      );
      await storage.saveOilPriceAnalysis(analysisResults);
      if (attempt > 1) {
        console.log(
          `[OilScheduler] 분석 저장 ${attempt}차 시도 성공: ${analysisResults.length}건`,
        );
      }
      return analysisResults.length;
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      if (attempt < maxAttempts) {
        console.warn(
          `[OilScheduler] 분석 저장 ${attempt}차 실패 (${msg}), ${retryDelayMs / 1000}초 후 재시도`,
        );
        await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
      } else {
        throw err;
      }
    }
  }
  return 0;
}

export async function runAnalysisOnlyFromDB(
  targetDate: string,
  yesterdayDate: string,
  jobType = "reanalyze",
): Promise<{
  success: boolean;
  analysisCount: number;
  error?: string;
}> {
  console.log(
    `[OilScheduler] DB 원본 분석 재실행: ${yesterdayDate} → ${targetDate}`,
  );
  const analysisStart = Date.now();
  try {
    const todayRaw = await storage.getOilPriceRawByDate(targetDate);
    if (todayRaw.length === 0) {
      await storage.saveOilCollectionLog({
        jobType,
        status: "failed",
        targetDate,
        yesterdayDate,
        rawCount: 0,
        analysisCount: 0,
        errorMessage: `DB에 ${targetDate} 원본 데이터 없음`,
      });
      return {
        success: false,
        analysisCount: 0,
        error: `DB에 ${targetDate} 원본 데이터 없음`,
      };
    }
    const yesterdayRaw = await storage.getOilPriceRawByDate(yesterdayDate);
    const allRows = [
      ...todayRaw.map(dbRawToOilPriceRow),
      ...yesterdayRaw.map(dbRawToOilPriceRow),
    ];
    console.log(
      `[OilScheduler] DB 원본: 오늘 ${todayRaw.length}건, 어제 ${yesterdayRaw.length}건`,
    );
    const analysisCount = await runAnalysisWithDbRetry(
      allRows,
      targetDate,
      yesterdayDate,
    );
    const analysisDurationMs = Date.now() - analysisStart;
    console.log(
      `[OilScheduler] DB 원본 분석 저장 완료: ${analysisCount}건 (${analysisDurationMs}ms)`,
    );
    await storage.saveOilCollectionLog({
      jobType,
      status: "success",
      targetDate,
      yesterdayDate,
      rawCount: todayRaw.length,
      analysisCount,
      analysisDurationMs,
    });
    return { success: true, analysisCount };
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("[OilScheduler] DB 원본 분석 오류:", msg);
    await storage.saveOilCollectionLog({
      jobType,
      status: "failed",
      targetDate,
      yesterdayDate,
      analysisDurationMs: Date.now() - analysisStart,
      errorMessage: msg,
    });
    return { success: false, analysisCount: 0, error: msg };
  }
}

export async function runOilPriceJob(
  today?: string,
  yesterday?: string,
  jobType = "manual",
): Promise<{
  success: boolean;
  rawSaved: boolean;
  rawCount: number;
  analysisCount: number;
  today: string;
  yesterday: string;
  error?: string;
}> {
  const kstNow = getKSTNow();
  const kstYesterday = new Date(kstNow);
  kstYesterday.setUTCDate(kstYesterday.getUTCDate() - 1);

  const todayStr = today ?? getDateStr(kstNow);
  const yesterdayStr = yesterday ?? getDateStr(kstYesterday);

  console.log(`[OilScheduler] 수집 시작: ${todayStr} (1일치)`);

  let rawSaved = false;
  let rawCount = 0;
  let analysisToday = todayStr;
  const analysisYesterday = yesterdayStr;
  const rawStart = Date.now();
  let rawDurationMs: number | undefined;
  let analysisDurationMs: number | undefined;

  try {
    const buffer = await downloadOilPriceCSV(todayStr);
    if (!buffer) {
      await storage.saveOilCollectionLog({
        jobType,
        status: "failed",
        targetDate: todayStr,
        yesterdayDate: yesterdayStr,
        rawCount: 0,
        analysisCount: 0,
        errorMessage: "CSV 다운로드 실패",
      });
      return {
        success: false,
        rawSaved: false,
        rawCount: 0,
        analysisCount: 0,
        today: todayStr,
        yesterday: yesterdayStr,
        error: "CSV 다운로드 실패",
      };
    }

    const rows = parseOilPriceCSV(buffer);
    console.log(`[OilScheduler] 파싱 완료: ${rows.length}건`);

    const insertRows = toInsertOilPriceRaw(rows);
    await storage.saveOilPriceRaw(insertRows);
    rawCount = insertRows.length;
    rawSaved = true;
    rawDurationMs = Date.now() - rawStart;
    console.log(
      `[OilScheduler] 원본 저장 완료: ${rawCount}건 (${rawDurationMs}ms)`,
    );

    const csvDates = [...new Set(rows.map((r) => r.date))].sort();
    analysisToday = csvDates[csvDates.length - 1] ?? todayStr;

    const dbYesterdayRaw =
      await storage.getOilPriceRawByDate(analysisYesterday);
    const dbYesterdayRows = dbYesterdayRaw.map(dbRawToOilPriceRow);
    console.log(
      `[OilScheduler] 분석 기준일: ${analysisYesterday} → ${analysisToday} (DB 어제 ${dbYesterdayRows.length}건 보완)`,
    );

    const analysisStart = Date.now();
    const allRows = [...rows, ...dbYesterdayRows];
    const analysisCount = await runAnalysisWithDbRetry(
      allRows,
      analysisToday,
      analysisYesterday,
    );
    analysisDurationMs = Date.now() - analysisStart;
    console.log(
      `[OilScheduler] 분석 저장 완료: ${analysisCount}건 (${analysisDurationMs}ms)`,
    );

    await storage.saveOilCollectionLog({
      jobType,
      status: "success",
      targetDate: analysisToday,
      yesterdayDate: analysisYesterday,
      rawCount,
      analysisCount,
      rawDurationMs,
      analysisDurationMs,
    });

    return {
      success: true,
      rawSaved: true,
      rawCount,
      analysisCount,
      today: analysisToday,
      yesterday: analysisYesterday,
    };
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("[OilScheduler] 오류:", msg);
    const status = rawSaved ? "partial" : "failed";
    await storage.saveOilCollectionLog({
      jobType,
      status,
      targetDate: analysisToday,
      yesterdayDate: analysisYesterday,
      rawCount,
      analysisCount: 0,
      rawDurationMs,
      analysisDurationMs,
      errorMessage: msg,
    });
    return {
      success: false,
      rawSaved,
      rawCount,
      analysisCount: 0,
      today: analysisToday,
      yesterday: analysisYesterday,
      error: msg,
    };
  }
}

function getMorningDates(): { today: string; yesterday: string } {
  const kstNow = getKSTNow();
  const kstYesterday = new Date(kstNow);
  kstYesterday.setUTCDate(kstYesterday.getUTCDate() - 1);
  const kstDayBefore = new Date(kstYesterday);
  kstDayBefore.setUTCDate(kstDayBefore.getUTCDate() - 1);
  return {
    today: getDateStr(kstYesterday),
    yesterday: getDateStr(kstDayBefore),
  };
}

interface RunJobOptions {
  source: string;
  slot: "morning" | "afternoon";
  pushMessage: string;
  notifyMasterOnSuccess?: boolean;
  jobDates?: { today: string; yesterday: string };
}

// ─── 슬롯별 중복 실행 방지 잠금 ───────────────────────────────────────────────
const collectionStartedAt: Record<string, number | null> = {
  morning: null,
  afternoon: null,
};
const COLLECTION_LOCK_TIMEOUT_MS = 15 * 60 * 1000;
const COLLECTION_HEARTBEAT_MS = 5 * 60 * 1000;

function isSlotRunning(slot: string): boolean {
  const startedAt = collectionStartedAt[slot];
  if (!startedAt) return false;
  if (Date.now() - startedAt > COLLECTION_LOCK_TIMEOUT_MS) {
    console.warn(
      `[OilScheduler] ${slot} 수집 잠금 타임아웃 초과(15분) — 자동 해제`,
    );
    collectionStartedAt[slot] = null;
    return false;
  }
  return true;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
// ──────────────────────────────────────────────────────────────────────────────

async function runWithRetryAndNotify(opts: RunJobOptions): Promise<void> {
  const { slot } = opts;

  if (isSlotRunning(slot)) {
    const elapsed = Math.round(
      (Date.now() - collectionStartedAt[slot]!) / 1000,
    );
    console.log(
      `[OilScheduler] ${slot} 수집 이미 진행 중 (${elapsed}초 경과) — 중복 실행 방지 skip`,
    );
    return;
  }

  collectionStartedAt[slot] = Date.now();

  const heartbeat = setInterval(() => {
    if (collectionStartedAt[slot] !== null) {
      collectionStartedAt[slot] = Date.now();
    }
  }, COLLECTION_HEARTBEAT_MS);

  try {
    await runCollectionWithRetries(opts);
  } finally {
    clearInterval(heartbeat);
    collectionStartedAt[slot] = null;
  }
}

async function runCollectionWithRetries(opts: RunJobOptions): Promise<void> {
  const {
    source,
    slot,
    pushMessage,
    notifyMasterOnSuccess = false,
    jobDates,
  } = opts;
  const baseJobType =
    slot === "morning" ? "scheduled_morning" : "scheduled_afternoon";
  const result = await runOilPriceJob(
    jobDates?.today,
    jobDates?.yesterday,
    baseJobType,
  );
  console.log(`[OilScheduler] ${source} 수집 결과:`, result);

  if (result.success && result.analysisCount > 0) {
    invalidateAvailableDatesCache();
    await sendUserPush(result.today, pushMessage, slot);
    if (notifyMasterOnSuccess) {
      await sendMasterPush(
        `${source} 수집 성공`,
        `수집 완료: 원본 ${result.rawCount}건, 분석 ${result.analysisCount}건`,
      );
    }
    try {
      const { runIfNotDoneToday, checkPriceChangeAlert } = await import(
        "./forecastService"
      );
      setImmediate(() => {
        runIfNotDoneToday().catch((e) =>
          console.error("[OilScheduler] forecastService 오류:", e),
        );
        checkPriceChangeAlert(result.today).catch((e) =>
          console.error("[OilScheduler] 가격변동 알림 오류:", e),
        );
      });
    } catch (e) {
      console.error("[OilScheduler] forecastService import 오류:", e);
    }
    return;
  }

  const analysisFailed = result.rawSaved && !result.success;

  if (result.success && result.analysisCount === 0) {
    console.log(
      `[OilScheduler] ${source}: 원본은 받았으나 분석 데이터 0건 (오피넷 미제공 가능성), 10분 후 재시도`,
    );
    await sendMasterPush(
      "수집 주의",
      `${source}: 데이터 수집됐으나 분석 0건 — 10분 후 재시도합니다.`,
    );
  } else if (analysisFailed) {
    console.log(
      `[OilScheduler] ${source}: 원본 저장 성공, 분석만 실패 (${result.error ?? "오류 미상"}) — 분석만 재시도합니다.`,
    );
    await sendMasterPush(
      "분석 저장 실패",
      `${source}: 원본 ${result.rawCount}건 저장 성공, 분석 저장 실패 (${result.error ?? "오류 미상"}) — 분석만 재시도합니다.`,
    );
  } else {
    console.log(`[OilScheduler] ${source} 실패, 10분 후 재시도 예정`);
    await sendMasterPush(
      "수집 실패",
      `${source}: 수집 실패 (${result.error ?? "오류 미상"}) — 10분 후 재시도합니다.`,
    );
  }

  const retryFn = async (label: string, retryJobType: string) => {
    if (analysisFailed || result.rawSaved) {
      console.log(
        `[OilScheduler] ${source} ${label}: 분석만 재실행 (DB 원본 사용)`,
      );
      return runAnalysisOnlyFromDB(
        result.today,
        result.yesterday,
        retryJobType,
      );
    }
    console.log(`[OilScheduler] ${source} ${label}: 전체 재수집`);
    return runOilPriceJob(jobDates?.today, jobDates?.yesterday, retryJobType);
  };

  const checkAlreadySucceeded = async (): Promise<boolean> => {
    try {
      if (slot === "morning") {
        const cutoffUTC = new Date(getKSTNow());
        cutoffUTC.setUTCHours(0, 30, 0, 0);
        return await storage.hasSuccessfulMorningLog(result.today, cutoffUTC);
      } else {
        return await storage.hasSuccessfulAfternoonLog(result.today);
      }
    } catch {
      return false;
    }
  };

  // 1차 재시도 (10분 후)
  await sleep(10 * 60 * 1000);
  if (await checkAlreadySucceeded()) {
    console.log(
      `[OilScheduler] ${source} 1차 재시도 건너뜀 — 다른 경로로 이미 수집 성공됨 (${result.today})`,
    );
    return;
  }
  console.log(`[OilScheduler] ${source} 1차 재시도 시작`);
  const retry1 = await retryFn("1차 재시도", `${baseJobType}_retry1`);
  console.log(`[OilScheduler] ${source} 1차 재시도 결과:`, retry1);

  if (retry1.success && retry1.analysisCount > 0) {
    invalidateAvailableDatesCache();
    await sendUserPush(result.today, pushMessage, slot);
    await sendMasterPush(
      `${source} 1차 재시도 성공`,
      `재시도 완료: 분석 ${retry1.analysisCount}건`,
    );
    return;
  }

  console.log(
    `[OilScheduler] ${source} 1차 재시도 실패, 10분 후 2차 재시도 예정`,
  );
  await sendMasterPush(
    "1차 재시도 실패",
    `${source}: 1차 재시도 실패 — 10분 후 2차 재시도합니다.`,
  );

  // 2차 재시도 (20분 후)
  await sleep(10 * 60 * 1000);
  if (await checkAlreadySucceeded()) {
    console.log(
      `[OilScheduler] ${source} 2차 재시도 건너뜀 — 다른 경로로 이미 수집 성공됨 (${result.today})`,
    );
    return;
  }
  console.log(`[OilScheduler] ${source} 2차 재시도 시작`);
  const retry2 = await retryFn("2차 재시도", `${baseJobType}_retry2`);
  console.log(`[OilScheduler] ${source} 2차 재시도 결과:`, retry2);

  if (retry2.success && retry2.analysisCount > 0) {
    invalidateAvailableDatesCache();
    await sendUserPush(result.today, pushMessage, slot);
    await sendMasterPush(
      `${source} 2차 재시도 성공`,
      `2차 재시도 완료: 분석 ${retry2.analysisCount}건`,
    );
  } else {
    await sendMasterPush(
      "유가 수집 최종 실패",
      `${source} 2차 재시도까지 실패했습니다.\n오류: ${retry2.error ?? `분석 ${retry2.analysisCount}건`}\n수동 수집이 필요합니다.`,
    );
  }
}

async function checkAndRecoverOnStartup(): Promise<void> {
  try {
    const kstNow = getKSTNow();
    const kstHour = kstNow.getUTCHours();
    const kstMinute = kstNow.getUTCMinutes();

    if (kstHour < 9 || (kstHour === 9 && kstMinute < 30)) {
      const targetKST = new Date(kstNow);
      targetKST.setUTCHours(9, 35, 0, 0);
      const delayMs = targetKST.getTime() - kstNow.getTime();
      console.log(
        `[OilScheduler] 시작 복구: KST ${kstHour}:${String(kstMinute).padStart(2, "0")} (9:30 이전), ${Math.round(delayMs / 60000)}분 후 재확인`,
      );
      setTimeout(() => checkAndRecoverOnStartup(), delayMs);
      return;
    }

    const kstYesterday = new Date(kstNow);
    kstYesterday.setUTCDate(kstYesterday.getUTCDate() - 1);
    const todayStr = getDateStr(kstNow);
    const yesterdayStr = getDateStr(kstYesterday);

    if (kstHour >= 16) {
      const latestAvailable = await storage.getOilPriceLatestDate();

      const hasAfternoonSuccess =
        await storage.hasSuccessfulAfternoonLog(todayStr);
      if (
        latestAvailable &&
        latestAvailable >= todayStr &&
        hasAfternoonSuccess
      ) {
        console.log(
          `[OilScheduler] 시작 복구: DB 최신(${latestAvailable}) ≥ 오늘(${todayStr}), 성공 로그 확인됨, 수집 불필요`,
        );
        setImmediate(() => {
          import("./forecastService")
            .then(({ runIfNotDoneToday }) => runIfNotDoneToday())
            .catch((e) =>
              console.error("[OilScheduler] 시작 예측 트리거(오후) 오류:", e),
            );
        });
        return;
      }

      if (latestAvailable && latestAvailable >= yesterdayStr) {
        console.log(
          `[OilScheduler] 시작 복구(오후): 오늘(${todayStr}) 데이터 없음, 잠정값 수집 시작`,
        );
        await runWithRetryAndNotify({
          source: "시작 복구(오후 잠정)",
          slot: "afternoon",
          pushMessage: "오늘 유가 데이터(잠정)가 업데이트되었습니다.",
          notifyMasterOnSuccess: true,
        });
      } else {
        console.log(
          `[OilScheduler] 시작 복구(오후): 어제(${yesterdayStr}) 데이터도 없음, 전체 수집 시작`,
        );
        await runWithRetryAndNotify({
          source: "시작 복구(오전 확정)",
          slot: "morning",
          pushMessage: "전일 유가 확정값이 업데이트되었습니다.",
          notifyMasterOnSuccess: true,
          jobDates: getMorningDates(),
        });
        await runWithRetryAndNotify({
          source: "시작 복구(오후 잠정)",
          slot: "afternoon",
          pushMessage: "오늘 유가 데이터(잠정)가 업데이트되었습니다.",
          notifyMasterOnSuccess: false,
        });
      }
      return;
    }

    const morningDates = getMorningDates();
    const todayMorningUTC = new Date(kstNow);
    todayMorningUTC.setUTCHours(0, 30, 0, 0);
    const alreadyDone = await storage.hasSuccessfulMorningLog(
      morningDates.today,
      todayMorningUTC,
    );
    if (alreadyDone) {
      console.log(
        `[OilScheduler] 시작 복구(오전): 오늘 오전 ${morningDates.today} 수집 성공 로그 확인됨, 건너뜀`,
      );
      setImmediate(() => {
        import("./forecastService")
          .then(({ runIfNotDoneToday }) => runIfNotDoneToday())
          .catch((e) =>
            console.error("[OilScheduler] 시작 예측 트리거(오전) 오류:", e),
          );
      });
      return;
    }
    console.log(
      `[OilScheduler] 시작 복구(오전): KST ${kstHour}시, 오늘 오전 수집 미완료 → 수집 시작`,
    );
    await runWithRetryAndNotify({
      source: "시작 복구(오전 확정)",
      slot: "morning",
      pushMessage: "전일 유가 확정값이 업데이트되었습니다.",
      notifyMasterOnSuccess: true,
      jobDates: morningDates,
    });
  } catch (err) {
    console.error("[OilScheduler] 시작 복구 확인 오류:", err);
  }
}

async function fetchOpinetFuelAverages(isStartup = false): Promise<void> {
  console.log("[OpinetScheduler] 유류 평균 수집 시작");
  const retryDelay = isStartup ? 10 * 1000 : 5 * 60 * 1000;
  const result = await fetchFuelAveragesWithRetry(3, retryDelay);
  if (!result) {
    setCachedFuelAverages(null);
    console.warn(
      "[OpinetScheduler] 유류 평균 수집 실패, 캐시 초기화 (DB fallback 사용)",
    );
  }
}

async function runIntlPriceCrawlerWithRetry(retryCount = 0): Promise<void> {
  const MAX_RETRIES = 2;
  const RETRY_DELAY_MS = 60 * 60 * 1000;
  const jobType =
    retryCount === 0 ? "intl_price" : `intl_price_retry${retryCount}`;
  const start = Date.now();

  try {
    const { runIntlPriceCrawler } = await import("./intlPriceCrawler");
    const result = await runIntlPriceCrawler();
    const durationMs = Date.now() - start;

    if (result.success) {
      console.log(`[IntlPriceCrawler] 수집 완료 (날짜: ${result.date})`);
      await storage.saveOilCollectionLog({
        jobType,
        status: "success",
        targetDate: result.date ?? undefined,
        rawCount: 1,
        rawDurationMs: durationMs,
      });
      return;
    }

    if (retryCount < MAX_RETRIES) {
      const errMsg = `Petronet 미갱신 또는 데이터 없음 — ${RETRY_DELAY_MS / 60000}분 후 재시도 (${retryCount + 1}/${MAX_RETRIES})`;
      console.warn(`[IntlPriceCrawler] ${errMsg}`);
      await storage.saveOilCollectionLog({
        jobType,
        status: "skipped",
        targetDate: result.date ?? undefined,
        rawDurationMs: durationMs,
        errorMessage: errMsg,
      });
      setTimeout(
        () => runIntlPriceCrawlerWithRetry(retryCount + 1),
        RETRY_DELAY_MS,
      );
    } else {
      const errMsg = "최대 재시도 횟수 초과, 이전 데이터 유지";
      console.error(`[IntlPriceCrawler] ${errMsg}`);
      await storage.saveOilCollectionLog({
        jobType,
        status: "failed",
        targetDate: result.date ?? undefined,
        rawDurationMs: durationMs,
        errorMessage: errMsg,
      });
    }
  } catch (e) {
    const durationMs = Date.now() - start;
    const msg = e instanceof Error ? e.message : String(e);
    console.error("[IntlPriceCrawler] 실행 오류:", e);
    await storage.saveOilCollectionLog({
      jobType,
      status: "failed",
      rawDurationMs: durationMs,
      errorMessage: msg,
    });
    if (retryCount < MAX_RETRIES) {
      setTimeout(
        () => runIntlPriceCrawlerWithRetry(retryCount + 1),
        RETRY_DELAY_MS,
      );
    }
  }
}

export async function runWeeklySupplyJob(retryCount = 0): Promise<void> {
  const jobType = "weekly_supply_price";
  const MAX_RETRIES = 2;
  const RETRY_DELAY_MS = 60 * 60 * 1000;
  const start = Date.now();
  console.log(
    `[WeeklySupplyScheduler] 주간공급가격 수집 시작${retryCount > 0 ? ` (재시도 ${retryCount}/${MAX_RETRIES})` : ""}`,
  );
  try {
    const latestDbWeek = await storage.getLatestWeeklySupplyWeek();
    console.log(
      `[WeeklySupplyScheduler] DB 최신 주차: ${latestDbWeek ?? "없음"}`,
    );

    const rows = await scrapeWeeklySupplyPrices();
    if (rows.length === 0) {
      await storage.saveOilCollectionLog({
        jobType,
        status: "failed",
        errorMessage: "파싱된 데이터 없음 (정유사 4사 행 미발견)",
      });
      await sendMasterPush(
        "주간공급가격 수집 실패",
        "테이블 파싱 후 대상 정유사 데이터가 없습니다.",
      );
      return;
    }

    const scrapedWeek = rows[0]?.week ?? "";

    if (latestDbWeek && scrapedWeek <= latestDbWeek) {
      console.warn(
        `[WeeklySupplyScheduler] 오피넷 미갱신 — 수집 주차(${scrapedWeek}) ≤ DB 최신(${latestDbWeek}), 새 데이터 없음`,
      );
      await storage.saveOilCollectionLog({
        jobType,
        status: "skipped",
        errorMessage: `오피넷 미갱신: 수집 주차(${scrapedWeek}) = DB 최신(${latestDbWeek})${retryCount < MAX_RETRIES ? ` — 1시간 후 재시도 (${retryCount + 1}/${MAX_RETRIES})` : " — 최대 재시도 초과"}`,
      });

      if (retryCount < MAX_RETRIES) {
        await sendMasterPush(
          "주간공급가격 미갱신",
          `오피넷이 아직 업데이트되지 않았습니다 (${scrapedWeek}).\n1시간 후 재시도합니다. (${retryCount + 1}/${MAX_RETRIES})`,
        );
        console.log(
          `[WeeklySupplyScheduler] 1시간 후 재시도 예정 (${retryCount + 1}/${MAX_RETRIES})`,
        );
        setTimeout(() => runWeeklySupplyJob(retryCount + 1), RETRY_DELAY_MS);
      } else {
        await sendMasterPush(
          "주간공급가격 수집 확인 필요",
          `${MAX_RETRIES}회 재시도 후에도 새 주차 데이터가 없습니다 (${scrapedWeek}).\n오피넷을 직접 확인하거나 수동 수집이 필요합니다.`,
        );
        console.warn(
          `[WeeklySupplyScheduler] 최대 재시도 횟수(${MAX_RETRIES}) 초과 — 수동 확인 필요`,
        );
      }
      return;
    }

    const insertRows = rows.map((r) => ({
      week: r.week,
      company: r.company,
      premiumGasoline:
        r.premiumGasoline != null ? String(r.premiumGasoline) : null,
      gasoline: r.gasoline != null ? String(r.gasoline) : null,
      diesel: r.diesel != null ? String(r.diesel) : null,
      kerosene: r.kerosene != null ? String(r.kerosene) : null,
    }));
    await storage.upsertWeeklySupplyPrices(insertRows);
    const durationMs = Date.now() - start;

    const hasGasoline = rows.some(
      (r) => r.gasoline !== null || r.premiumGasoline !== null,
    );
    const hasDiesel = rows.some((r) => r.diesel !== null);
    const hasKerosene = rows.some((r) => r.kerosene !== null);
    const missingFuels = [
      !hasGasoline && "휘발유",
      !hasDiesel && "경유",
      !hasKerosene && "등유",
    ].filter(Boolean);
    const collectionStatus = missingFuels.length === 0 ? "success" : "partial";

    if (missingFuels.length > 0) {
      console.warn(
        `[WeeklySupplyScheduler] 일부 유종 수집 미완: [${missingFuels.join(", ")}] — partial 처리`,
      );
      await sendMasterPush(
        "주간공급가격 수집 일부 미완",
        `수집 미완 유종: ${missingFuels.join(", ")} (나머지는 저장됨)`,
      );
    }

    await storage.saveOilCollectionLog({
      jobType,
      status: collectionStatus,
      rawCount: rows.length,
      analysisDurationMs: durationMs,
    });
    console.log(
      `[WeeklySupplyScheduler] 수집 완료: ${rows.length}건 (${durationMs}ms, ${collectionStatus}, 주차: ${scrapedWeek})`,
    );

    const wk = scrapedWeek;
    const pushBody =
      wk.length === 8
        ? `${wk.slice(0, 4)}년 ${wk.slice(4, 6)}월 ${parseInt(wk.slice(6, 8))}주 공급가격 데이터가 업데이트되었습니다.`
        : `${wk} 주간 공급가격 데이터가 업데이트되었습니다.`;
    const allSubs = await storage.getAllPushSubscriptions();
    if (allSubs.length > 0) {
      const payload = {
        title: "주간공급가격 업데이트",
        body: pushBody,
        icon: "/icon-192.png",
        url: "/oil-prices",
      };
      const { sent, failed, expiredEndpoints } = await sendPushToAll(
        allSubs,
        payload,
      );
      if (expiredEndpoints.length > 0) {
        await Promise.all(
          expiredEndpoints.map((ep) => storage.deletePushSubscription(ep)),
        );
      }
      console.log(
        `[WeeklySupplyScheduler] 푸시 발송: 성공 ${sent}건, 실패 ${failed}건`,
      );
    }
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("[WeeklySupplyScheduler] 오류:", msg);
    await storage.saveOilCollectionLog({
      jobType,
      status: "failed",
      errorMessage: msg,
      analysisDurationMs: Date.now() - start,
    });
    await sendMasterPush("주간공급가격 수집 오류", `오류: ${msg}`);
  }
}

// ─── ★ 수정: checkAndRecoverIntlPriceOnStartup 전체 교체 ─────────────────────
async function checkAndRecoverIntlPriceOnStartup(): Promise<void> {
  try {
    const kstNow = getKSTNow();
    const kstHour = kstNow.getUTCHours();
    const kstMinute = kstNow.getUTCMinutes();
    const kstDayOfWeek = kstNow.getUTCDay(); // 0=일, 1=월, ..., 6=토

    // 일요일(0)은 무조건 건너뜀
    if (kstDayOfWeek === 0) {
      console.log(`[IntlPriceCrawler] 시작 복구: 일요일 → 건너뜀`);
      return;
    }

    // 토요일(6)은 금요일 수집이 실패했을 때만 재시도
    if (kstDayOfWeek === 6) {
      const kstFriday = new Date(kstNow);
      kstFriday.setUTCDate(kstFriday.getUTCDate() - 1);
      const fridayStr = getDateStr(kstFriday);

      const kstThursday = new Date(kstNow);
      kstThursday.setUTCDate(kstThursday.getUTCDate() - 2);
      const thursdayStr = getDateStr(kstThursday);

      // 금요일 수집 성공 여부: 금요일 또는 목요일 날짜 데이터가 있는지 확인
      // (Petronet이 금요일에 올리는 데이터는 목요일 날짜일 수도, 금요일 날짜일 수도 있음)
      const result = await db.execute(sql`
        SELECT date FROM intl_fuel_prices
        WHERE (gasoline IS NOT NULL OR diesel IS NOT NULL OR kerosene IS NOT NULL)
          AND (date = ${fridayStr} OR date = ${thursdayStr})
        ORDER BY date DESC LIMIT 1
      `);

      if (result.rows.length > 0) {
        const existingDate = result.rows[0]?.date as string;
        console.log(
          `[IntlPriceCrawler] 시작 복구(토): 금요일 수집 데이터 존재 (${existingDate}) → 건너뜀`,
        );
        return;
      }

      console.log(
        `[IntlPriceCrawler] 시작 복구(토): 금요일 수집 실패 확인 (${fridayStr}/${thursdayStr} 없음) → 재시도 수집 시작`,
      );
      await runIntlPriceCrawlerWithRetry();
      return;
    }

    // 월~금: 08:30 이전이면 cron 대기
    if (kstHour < 8 || (kstHour === 8 && kstMinute < 30)) {
      console.log(
        `[IntlPriceCrawler] 시작 복구: KST ${kstHour}:${String(kstMinute).padStart(2, "0")} (08:30 이전) → cron 대기`,
      );
      return;
    }

    // 23시 이후면 건너뜀
    if (kstHour >= 23) {
      console.log(
        `[IntlPriceCrawler] 시작 복구: KST ${kstHour}시 (23시 이후) → 건너뜀`,
      );
      return;
    }

    // ★ 핵심 변경: 오늘/어제 날짜 데이터가 있는지 정확히 확인
    const todayStr = getDateStr(kstNow);
    const kstYesterday = new Date(kstNow);
    kstYesterday.setUTCDate(kstYesterday.getUTCDate() - 1);
    const yesterdayStr = getDateStr(kstYesterday);

    const result = await db.execute(sql`
      SELECT date FROM intl_fuel_prices
      WHERE (gasoline IS NOT NULL OR diesel IS NOT NULL OR kerosene IS NOT NULL)
        AND (date = ${todayStr} OR date = ${yesterdayStr})
      ORDER BY date DESC LIMIT 1
    `);
    const matchedDate = result.rows[0]?.date as string | undefined;

    if (matchedDate) {
      console.log(
        `[IntlPriceCrawler] 시작 복구: 오늘/어제 데이터 존재 (${matchedDate}) → 건너뜀`,
      );
      return;
    }

    console.log(
      `[IntlPriceCrawler] 시작 복구: 오늘(${todayStr})/어제(${yesterdayStr}) 데이터 없음 → 즉시 수집 시작`,
    );
    await runIntlPriceCrawlerWithRetry();
  } catch (err) {
    console.error("[IntlPriceCrawler] 시작 복구 확인 오류:", err);
  }
}
// ──────────────────────────────────────────────────────────────────────────────

async function checkAndRecoverWeeklySupplyOnStartup(): Promise<void> {
  try {
    const kstNow = getKSTNow();
    const kstHour = kstNow.getUTCHours();
    const kstDayOfWeek = kstNow.getUTCDay();

    const isFriday = kstDayOfWeek === 5;
    const isSaturday = kstDayOfWeek === 6;
    const isMonday = kstDayOfWeek === 1;
    const isTuesday = kstDayOfWeek === 2;

    if (!isFriday && !isSaturday && !isMonday && !isTuesday) {
      return;
    }
    if ((isFriday || isMonday) && kstHour < 14) {
      console.log(
        `[WeeklySupplyRecover] 수집 예정 시각(14:00) 이전 (KST ${kstHour}시) → cron 대기`,
      );
      return;
    }

    const checkFrom = new Date(kstNow);
    if (isSaturday || isTuesday) {
      checkFrom.setUTCDate(checkFrom.getUTCDate() - 1);
    } else if (isMonday) {
      const lastFriday = new Date(kstNow);
      lastFriday.setUTCDate(lastFriday.getUTCDate() - 3);
      if (!isKoreanHoliday(lastFriday)) {
        checkFrom.setUTCDate(checkFrom.getUTCDate() - 3);
      }
    }
    checkFrom.setUTCHours(5, 0, 0, 0);

    const todayStr = getDateStr(kstNow);

    const logResult = await db.execute(
      sql`SELECT
            SUM(CASE
              WHEN status IN ('success', 'partial') THEN 1
              WHEN status = 'skipped' AND error_message LIKE '%직전 금요일%' THEN 1
              ELSE 0
            END) AS success_cnt,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_cnt
          FROM oil_collection_logs
          WHERE job_type = 'weekly_supply_price'
            AND created_at >= ${checkFrom.toISOString()}`,
    );
    const successCnt = Number((logResult.rows[0] as any)?.success_cnt ?? 0);
    const failedCnt = Number((logResult.rows[0] as any)?.failed_cnt ?? 0);

    if (successCnt > 0) {
      console.log(
        `[WeeklySupplyRecover] 수집 성공 로그 확인됨 (기준: ${checkFrom.toISOString()}) → 복구 불필요`,
      );
      return;
    }

    const MAX_FAILED_ATTEMPTS = 3;
    if (failedCnt >= MAX_FAILED_ATTEMPTS) {
      console.warn(
        `[WeeklySupplyRecover] 실패 로그 ${failedCnt}건 (>=${MAX_FAILED_ATTEMPTS}) — 자동 복구 중단, 수동 확인 필요 (기준: ${checkFrom.toISOString()})`,
      );
      return;
    }

    console.log(
      `[WeeklySupplyRecover] 오늘(${todayStr}) 주간공급가격 수집 로그 없음 (성공 0건, 실패 ${failedCnt}건, 기준: ${checkFrom.toISOString()}) → 즉시 수집 시작`,
    );
    await runWeeklySupplyJob();
  } catch (err) {
    console.error("[WeeklySupplyRecover] 시작 복구 오류:", err);
  }
}

export function startOilScheduler(): void {
  // 오전 9:30 — 전날 확정값 수집
  cron.schedule(
    "30 9 * * *",
    async () => {
      console.log(
        "[OilScheduler] 오전 수집 시작 (전날 확정값, 매일 09:30 KST)",
      );
      await runWithRetryAndNotify({
        source: "오전 정기 수집",
        slot: "morning",
        pushMessage: "전일 유가 확정값이 업데이트되었습니다.",
        jobDates: getMorningDates(),
      });
    },
    { timezone: "Asia/Seoul" },
  );

  // 오후 16:30 — 당일 잠정값 수집
  cron.schedule(
    "30 16 * * *",
    async () => {
      console.log(
        "[OilScheduler] 오후 수집 확인 (당일 잠정값, 매일 16:30 KST)",
      );
      const todayStr = getKSTDateStr();
      const latestAvailable = await storage.getOilPriceLatestDate();

      if (latestAvailable && latestAvailable >= todayStr) {
        console.log(
          `[OilScheduler] 오후 수집 건너뜀: 오늘(${todayStr}) 데이터 이미 존재`,
        );
        return;
      }

      console.log(
        `[OilScheduler] 오후 수집 시작: 오늘(${todayStr}) 데이터 없음`,
      );
      await runWithRetryAndNotify({
        source: "오후 정기 수집",
        slot: "afternoon",
        pushMessage: "오늘 유가 데이터(잠정)가 업데이트되었습니다.",
      });
    },
    { timezone: "Asia/Seoul" },
  );

  cron.schedule(
    "0 9,12,16,19 * * *",
    async () => {
      console.log("[OpinetScheduler] 정기 유류 평균 수집 (KST)");
      await fetchOpinetFuelAverages();
    },
    { timezone: "Asia/Seoul" },
  );

  // 금요일 14:00 KST — 주간공급가격
  cron.schedule(
    "0 14 * * 5",
    async () => {
      const today = getKSTNow();
      console.log(
        `[WeeklySupplyScheduler] 금요일 14:00 KST 트리거 (${getDateStr(today)})`,
      );
      if (isKoreanHoliday(today)) {
        console.log(
          "[WeeklySupplyScheduler] 오늘은 한국 공휴일 → 수집 건너뜀 (월요일에 수집 예정)",
        );
        await storage.saveOilCollectionLog({
          jobType: "weekly_supply_price",
          status: "skipped",
          errorMessage: "금요일 공휴일로 인해 건너뜀",
        });
        return;
      }
      await runWeeklySupplyJob();
    },
    { timezone: "Asia/Seoul" },
  );

  // 월요일 14:00 KST — 직전 금요일이 공휴일이었으면 주간공급가격 수집
  cron.schedule(
    "0 14 * * 1",
    async () => {
      const today = getKSTNow();
      const lastFriday = new Date(today);
      lastFriday.setUTCDate(lastFriday.getUTCDate() - 3);
      console.log(
        `[WeeklySupplyScheduler] 월요일 14:00 KST 트리거 (${getDateStr(today)}), 직전 금요일: ${getDateStr(lastFriday)}`,
      );
      if (!isKoreanHoliday(lastFriday)) {
        console.log(
          "[WeeklySupplyScheduler] 직전 금요일이 평일 → 이미 금요일에 수집됨, 건너뜀",
        );
        await storage.saveOilCollectionLog({
          jobType: "weekly_supply_price",
          status: "skipped",
          errorMessage: "직전 금요일 평일 수집됨으로 건너뜀",
        });
        return;
      }
      await runWeeklySupplyJob();
    },
    { timezone: "Asia/Seoul" },
  );

  // ★ 수정: 월~금 08:30 KST — Petronet 국제가격 크롤링 (기존 화~토 → 월~금)
  cron.schedule(
    "30 8 * * 1-5",
    async () => {
      console.log("[IntlPriceCrawler] 정기 수집 시작 (월~금 08:30 KST)");

      // ★ 추가: 오늘/어제 데이터가 이미 있으면 건너뜀
      const kstNow = getKSTNow();
      const todayStr = getDateStr(kstNow);
      const kstYesterday = new Date(kstNow);
      kstYesterday.setUTCDate(kstYesterday.getUTCDate() - 1);
      const yesterdayStr = getDateStr(kstYesterday);

      try {
        const result = await db.execute(sql`
        SELECT date FROM intl_fuel_prices
        WHERE (gasoline IS NOT NULL OR diesel IS NOT NULL OR kerosene IS NOT NULL)
          AND (date = ${todayStr} OR date = ${yesterdayStr})
        ORDER BY date DESC LIMIT 1
      `);
        if (result.rows.length > 0) {
          const existingDate = result.rows[0]?.date as string;
          console.log(
            `[IntlPriceCrawler] 정기 수집 건너뜀: 오늘/어제 데이터 이미 존재 (${existingDate})`,
          );
          return;
        }
      } catch (err) {
        console.error("[IntlPriceCrawler] 정기 수집 DB 확인 오류:", err);
      }

      await runIntlPriceCrawlerWithRetry();
    },
    { timezone: "Asia/Seoul" },
  );

  // 매월 1일 02:00 KST — 임계값 자동 갱신
  cron.schedule(
    "0 2 1 * *",
    async () => {
      console.log(
        "[ForecastService] 월간 임계값 갱신 시작 (매월 1일 02:00 KST)",
      );
      try {
        const { runThresholdCalibrator } = await import("./forecastService");
        const result = await runThresholdCalibrator();
        const body = `임계값 갱신됨: ${result.oldThreshold ?? "없음"}원 → ${result.newThreshold ?? "실패"}원`;
        await sendMasterPush("AI 임계값 갱신", body);
        console.log("[ForecastService] 임계값 갱신 완료:", result);
      } catch (e) {
        console.error("[ForecastService] 임계값 갱신 오류:", e);
      }
    },
    { timezone: "Asia/Seoul" },
  );

  // ★ 수정: 로그 메시지에서 "화~토" → "월~금" 반영
  console.log(
    "[OilScheduler] 스케줄러 등록 완료 (오전 확정 09:30 / 오후 잠정 16:30 / 유류 평균 9,12,16,19시 KST / 주간공급가격 금·월 14:00 KST / 국제제품가격 월~금 08:30 KST / AI 임계값 갱신 매월1일 02:00 KST)",
  );

  setTimeout(() => checkAndRecoverOnStartup(), 5000);

  // ★ 수정된 시작 복구: 오늘/어제 데이터 기준 판단 + 토요일은 금요일 실패 시에만
  setTimeout(() => checkAndRecoverIntlPriceOnStartup(), 7000);

  setTimeout(() => checkAndRecoverWeeklySupplyOnStartup(), 9000);

  setTimeout(() => {
    console.log("[OpinetScheduler] 서버 시작 직후 유류 평균 즉시 수집");
    fetchOpinetFuelAverages(true);
  }, 3000);
}
