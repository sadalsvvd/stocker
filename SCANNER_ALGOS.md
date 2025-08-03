Detailed Analysis of Stock Scanner Algorithms

  1. ATR-Based Volatility Detection (run_volatility.py)

  Algorithm:
  1. Calculate 14-day ATR using Wilder's EMA
  2. Calculate ATR percentage change day-over-day
  3. Flag days where ATR % change > 25%
  4. Separate into gains/losses based on close vs previous close

  Detailed Logic:
  - ATR Calculation: Uses J. Welles Wilder's True Range formula
    - TR = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
    - ATR = Exponential Moving Average of TR with α=1/n
  - Quality Filters:
    - Skip first 14 days (insufficient data)
    - Skip dates before 2010
    - Skip split days
    - Reject if adjusted_close % differs from close % by >5%

  Commentary:
  This is the most sophisticated approach because ATR measures volatility relative to recent price action. A 25%
  ATR spike means volatility increased by 1/4 in a single day - this catches stocks "waking up" from quiet
  periods. The adjusted close divergence check is clever for detecting data quality issues.

  Strengths:
  - Adapts to each stock's volatility baseline
  - Catches regime changes, not just big moves
  - Robust data quality checks

  Weaknesses:
  - Fixed 25% threshold may not suit all stocks
  - No consideration of volume
  - Doesn't distinguish gap vs intraday volatility

  ---
  2. Percentage Move Detection (run_moves.py)

  Algorithm:
  1. Calculate daily percentage change
  2. Flag days with moves > threshold (default 25%)
  3. Optional: Also flag smaller moves that diverge from SPY
  4. Tag as gl+ (gain) or gl- (loss)

  Detailed Logic:
  - Basic Detection: percent_change >= threshold or <= -threshold
  - Market-Relative Detection (optional):
    - Calculate stock % change vs SPY % change
    - Flag if move > lower threshold AND divergence > threshold
    - Catches stocks moving against market

  Commentary:
  This is the simplest approach but includes an interesting market-relative component. The SPY divergence feature
  helps find stocks with relative strength/weakness - a 15% gain when SPY is down 5% is more significant than a
  15% gain when SPY is up 10%.

  Strengths:
  - Simple and interpretable
  - Market-relative option adds context
  - Can tune thresholds per use case

  Weaknesses:
  - No volatility adjustment
  - Treats all 25% moves equally
  - No volume consideration

  ---
  3. Astrological Breakout Seeker (breakoutSeeker.ts)

  Algorithm:
  1. Load stocks with quality first trade dates (A or AAA rating)
  2. Generate astrological aspects for date range
  3. For each datetime, score stocks based on aspect matches
  4. Filter out recent IPOs and duplicates
  5. Sort by score and output matches

  Detailed Logic:
  - Aspect Matching:
  const ASPECT_MATCH_KEYS = [
    'T Mars|square|SP Moon',
    'T Mars|opposition|SP Moon',
    'T Saturn|square|SP Moon',
    'T Saturn|opposition|SP Moon',
  ];
  - Scoring: +1 for each matching aspect within 35' orb
  - Filters:
    - Skip IPOs < 6 months old
    - Skip duplicate aspect combinations per stock
    - Only count applying aspects

  Commentary:
  This seeks breakouts using astrological timing, specifically hard aspects from Mars/Saturn to progressed Moon.
  The 35' orb is quite tight (about 1/2 degree), suggesting these aspects need to be very exact. The focus on
  progressed Moon is interesting - in financial astrology, progressed Moon often relates to public
  sentiment/momentum.

  Strengths:
  - Unique timing approach
  - Considers stock's "natal" chart
  - Tight orbs reduce false positives

  Weaknesses:
  - No price/volume confirmation
  - Limited aspect set
  - Requires accurate birth data

  ---
  4. Extreme Days Analysis (extremeDays.ts)

  Algorithm:
  1. Load pre-filtered extreme move days from CSV
  2. Generate aspects for those specific dates
  3. Tally aspect frequencies for bull/bear days
  4. Score aspects by "exactitude" (within 2-day orb)
  5. Output ranked lists of most common aspects

  Detailed Logic:
  - Input Filter (from CSV):
    - Price >= $20
    - Date >= 2000-01-01
    - Move >= 30% or <= -30%
  - Aspect Scoring:
    - Calculate days until exact: difference / speed
    - Include if <= 2 days from exact
    - Score = 1 (was going to use graduated score)
  - Categorization:
    - Three groupings: Full aspect, planet pairing, display pairing
    - Separate tallies for bullish/bearish days

  Commentary:
  This is a pattern discovery tool - it mines historical extreme days to find which aspects appear most
  frequently. The 2-day orb using daily motion is sophisticated (faster planets get tighter time orbs). The
  three-level categorization helps identify if it's the specific aspect or just the planet combination that
  matters.

  Strengths:
  - Data-driven pattern discovery
  - Considers aspect timing precision
  - Separates bull/bear patterns

  Weaknesses:
  - Requires pre-filtered data
  - No statistical significance testing
  - Sample bias toward longer-lived stocks

  ---
  5. Peak/Valley Detection (run_peaks.py)

  Algorithm:
  1. Load price data and apply smoothing (optional)
  2. Use scipy.signal.find_peaks on close prices
  3. Invert prices and find_peaks again for valleys
  4. Filter by prominence and distance parameters
  5. Calculate peak/valley statistics and dates

  Detailed Logic:
  - Peak Detection Parameters:
    - distance: Minimum bars between peaks
    - prominence: Minimum height difference from surrounding valleys
    - Can be tuned per ticker (e.g., AAPL: [2, 2.5])
  - Valley Detection: Same but on -1 * prices
  - Output: Date, price, and metadata for each peak/valley

  Commentary:
  This uses signal processing techniques to find significant local extrema. The prominence parameter is key - it
  ensures peaks are "significant" relative to nearby price action. This is more sophisticated than simple "higher
  than N days before/after" logic. The per-ticker tuning suggests different stocks need different sensitivity.

  Strengths:
  - Robust signal processing approach
  - Adapts to price structure
  - Good for cycle analysis

  Weaknesses:
  - Retrospective (can't detect in real-time)
  - Tuning parameters is subjective
  - No volume confirmation