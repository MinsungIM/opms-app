import { chromium } from "playwright";
import { existsSync, mkdirSync } from "fs";
import { readFile, unlink } from "fs/promises";
import path from "path";

const CHROMIUM_PATH =
  process.env.PUPPETEER_EXECUTABLE_PATH ?? "/usr/bin/chromium";
const DOWNLOAD_PAGE =
  "https://www.opinet.co.kr/user/opdown/opDownload.do";
const DOWNLOAD_DIR = "/tmp/opinet_downloads";

/**
 * 오피넷 사업자별 과거 판매가격 CSV 다운로드 (fn_Download(6))
 * @param targetDate YYYYMMDD 형식 날짜 (START_DT = END_DT = targetDate)
 */
export async function downloadOilPriceCSV(targetDate: string): Promise<Buffer | null> {
  let browser;

  try {
    if (!existsSync(DOWNLOAD_DIR)) {
      mkdirSync(DOWNLOAD_DIR, { recursive: true });
    }

    console.log(`[OilScraper] 브라우저 시작 (과거판매가격 CSV, 날짜: ${targetDate})`);
    browser = await chromium.launch({
      executablePath: CHROMIUM_PATH,
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-software-rasterizer",
        "--disable-setuid-sandbox",
        "--no-first-run",
        "--no-zygote",
        "--disable-extensions",
        "--disable-background-networking",
        "--disable-default-apps",
        "--disable-sync",
        "--disable-translate",
        "--hide-scrollbars",
        "--metrics-recording-only",
        "--mute-audio",
        "--safebrowsing-disable-auto-update",
      ],
    });

    const context = await browser.newContext({
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
      locale: "ko-KR",
      acceptDownloads: true,
    });

    const page = await context.newPage();

    page.on("dialog", async (dialog) => {
      console.log(`[OilScraper] 다이얼로그 수락: ${dialog.message().substring(0, 80)}`);
      await dialog.accept();
    });

    console.log("[OilScraper] 페이지 로딩...");
    await page.goto(DOWNLOAD_PAGE, {
      waitUntil: "domcontentloaded",
      timeout: 120000,
    });

    let fnReady = false;
    await page
      .waitForFunction(
        () => typeof (window as any).fn_Download === "function",
        undefined,
        { timeout: 300000 }
      )
      .then(() => { fnReady = true; })
      .catch(() => {});

    if (!fnReady) {
      throw new Error("fn_Download 함수 로드 실패 — NetFunnel 타임아웃 (5분 초과)");
    }

    console.log("[OilScraper] fn_Download 함수 확인 완료");

    // 날짜 명시 설정 (START_DT = END_DT = targetDate)
    await page.evaluate((date: string) => {
      const $ = (window as any).$;
      $("#span_start_date_picker").val(date);
      $("#span_end_date_picker").val(date);
    }, targetDate);

    // 설정 확인
    const setDate = await page.evaluate(() => ({
      start: (document.getElementById("span_start_date_picker") as HTMLInputElement)?.value,
      end: (document.getElementById("span_end_date_picker") as HTMLInputElement)?.value,
    }));
    console.log(`[OilScraper] 날짜 설정 확인 — START: ${setDate.start}, END: ${setDate.end}`);

    if (setDate.start !== targetDate || setDate.end !== targetDate) {
      throw new Error(`날짜 설정 실패 — 기대: ${targetDate}, 실제: ${setDate.start}~${setDate.end}`);
    }

    console.log(`[OilScraper] fn_Download(6) 호출 (과거판매가격 CSV) 및 다운로드 대기 (최대 600초)...`);
    const [download] = await Promise.all([
      page.waitForEvent("download", { timeout: 600000 }),
      page.evaluate(() => { (window as any).fn_Download(6); }),
    ]);

    const suggestedFilename = download.suggestedFilename() || `opinet_past_${targetDate}.csv`;
    const savePath = path.join(DOWNLOAD_DIR, suggestedFilename);

    await download.saveAs(savePath);
    console.log(`[OilScraper] 파일 저장 완료: ${savePath}`);

    const csvBuffer = await readFile(savePath);
    console.log(`[OilScraper] 다운로드 완료: ${csvBuffer.byteLength} bytes, 파일명: ${suggestedFilename}`);

    await unlink(savePath).catch(() => {});

    return csvBuffer;
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("[OilScraper] 오류:", msg);
    return null;
  } finally {
    if (browser) await browser.close().catch(() => {});
  }
}
