import type { Category } from "./instruments.js";

/* Approximate front-month contract code (e.g. ES → ESM6) from a date and the
   product's listing cycle. Used in simulation mode and as a fallback; when
   Databento is connected, the exact code comes from symbology.resolve.

   Month codes: Jan..Dec = F G H J K M N Q U V X Z. */
const MONTH_CODES = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"];

function cycleMonths(category: Category): number[] {
  if (category === "Energy") return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]; // monthly
  if (category === "Metals") return [2, 4, 6, 8, 10, 12]; // G J M Q V Z
  return [3, 6, 9, 12]; // Equity Index quarterly: H M U Z
}

export function computeContractCode(root: string, category: Category, dateMs: number): string {
  const d = new Date(dateMs);
  const year = d.getUTCFullYear();
  const month = d.getUTCMonth() + 1; // 1-12
  const day = d.getUTCDate();
  const months = cycleMonths(category);
  // Energy rolls early in the month; index/metals roll near the 3rd-Friday expiry.
  const rollDay = category === "Energy" ? 1 : 16;

  let activeMonth = months[0]!;
  let activeYear = year + 1;
  for (let i = 0; i < 24; i++) {
    const m = ((month - 1 + i) % 12) + 1;
    const y = year + Math.floor((month - 1 + i) / 12);
    if (!months.includes(m)) continue;
    if (i === 0 && day >= rollDay) continue; // already rolled out of the near month
    activeMonth = m;
    activeYear = y;
    break;
  }
  return `${root}${MONTH_CODES[activeMonth - 1]}${activeYear % 10}`;
}
