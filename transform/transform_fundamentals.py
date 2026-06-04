import os
import csv
import logging
import pandas as pd
from collections import Counter
from datetime import datetime
from ingestion.utils import r2_client
from transform.utils.parquet_writer import upsert_parquet


def filter_real_annual_earnings(annual_earnings: list[dict]) -> list[dict]:
    """Alpha Vantage's `annualEarnings` array consistently inserts a rolling-TTM
    entry as the first item, dated to the most recent quarter end (not the
    company's fiscal year end). E.g. AAPL has FY end Sept 30 but AV inserts
    a '2026-03-31' entry alongside the real '2025-09-30' annual row.

    Filter approach: find the dominant MM-DD across all entries and keep only
    matching ones. The spurious TTM entry will always be the outlier.

    Skips filtering if too few entries to establish a pattern (<3).
    """
    if not annual_earnings:
        return []
    if len(annual_earnings) < 3:
        return annual_earnings

    mmdd = [e.get("fiscalDateEnding", "")[-5:] for e in annual_earnings
            if e.get("fiscalDateEnding")]
    if not mmdd:
        return annual_earnings

    dominant, _ = Counter(mmdd).most_common(1)[0]
    return [e for e in annual_earnings
            if e.get("fiscalDateEnding", "")[-5:] == dominant]

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_active_tickers(config_path: str) -> list[str]:
    """Reads the ticker universe CSV and returns a list of active symbols."""
    symbols = []
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Ticker universe config file not found at: {config_path}")
    
    with open(config_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('active', '').lower() == 'true':
                symbols.append(row['symbol'].strip())
    return symbols

def clean_val(val, target_type):
    if val is None or val == "" or str(val).strip().lower() == "none":
        return None
    try:
        if target_type == 'int':
            # Handle float strings being converted to int
            return int(float(val))
        elif target_type == 'float':
            return float(val)
        else:
            return str(val).strip()
    except (ValueError, TypeError):
        return None

def find_latest_bronze_files(prefix: str, active_symbols: list[str]) -> dict[str, str]:
    """Lists keys under a prefix and returns a map of symbol -> latest key."""
    all_keys = r2_client.list_keys(prefix)
    symbol_files = {}
    for key in all_keys:
        parts = key.split('/')
        if len(parts) >= 4:
            symbol = parts[2]
            filename = parts[3]
            pull_date_str = filename.replace('.json', '')
            
            if symbol in active_symbols:
                if symbol not in symbol_files:
                    symbol_files[symbol] = []
                symbol_files[symbol].append((pull_date_str, key))
                
    latest_files = {}
    for symbol, files in symbol_files.items():
        sorted_files = sorted(files, key=lambda x: x[0])
        latest_files[symbol] = sorted_files[-1][1]
    return latest_files

def transform_income_statements(latest_files: dict[str, str]):
    logger.info("Transforming Income Statements...")
    rows = []
    for symbol, key in latest_files.items():
        try:
            data = r2_client.download_json(key)
        except Exception as e:
            logger.error(f"Failed to download income statement JSON for {symbol}: {e}")
            continue

        pull_date = data.get("pull_date", key.split('/')[-1].replace('.json', ''))
        
        # Ingest both annual and quarterly reports
        reports_map = {
            "annual": data.get("annualReports", []),
            "quarterly": data.get("quarterlyReports", [])
        }

        for period_type, reports in reports_map.items():
            for report in reports:
                # NOTE: Alpha Vantage's INCOME_STATEMENT endpoint does not
                # return any EPS fields — basicEarningsPerShare / dilutedEarningsPerShare
                # are not in the response. EPS is captured separately in
                # fact_earnings.reported_eps. Do not re-add EPS columns here.
                row = {
                    "symbol": symbol,
                    "fiscal_date_ending": clean_val(report.get("fiscalDateEnding"), "str"),
                    "period_type": period_type,
                    "reported_currency": clean_val(report.get("reportedCurrency"), "str"),
                    "total_revenue": clean_val(report.get("totalRevenue"), "int"),
                    "cost_of_revenue": clean_val(report.get("costOfRevenue"), "int"),
                    "gross_profit": clean_val(report.get("grossProfit"), "int"),
                    "operating_expenses": clean_val(report.get("operatingExpenses"), "int"),
                    "operating_income": clean_val(report.get("operatingIncome"), "int"),
                    "ebit": clean_val(report.get("ebit"), "int"),
                    "ebitda": clean_val(report.get("ebitda"), "int"),
                    "depreciation_amortization": clean_val(report.get("depreciationAndAmortization"), "int"),
                    "income_before_tax": clean_val(report.get("incomeBeforeTax"), "int"),
                    "income_tax": clean_val(report.get("incomeTaxExpense"), "int"),
                    "net_income": clean_val(report.get("netIncome"), "int"),
                    "net_income_continuing_ops": clean_val(report.get("netIncomeFromContinuingOperations"), "int"),
                    "r_and_d": clean_val(report.get("researchAndDevelopment"), "int"),
                    "sga": clean_val(report.get("sellingGeneralAndAdministrative"), "int"),
                    "interest_expense": clean_val(report.get("interestExpense"), "int"),
                    "pull_date": pull_date
                }
                rows.append(row)
                
    if rows:
        df = pd.DataFrame(rows)
        upsert_parquet(
            df, 
            "silver/fact_income_statement/fact_income_statement.parquet", 
            ["symbol", "fiscal_date_ending", "period_type"]
        )
    else:
        logger.warning("No income statement records parsed.")

def transform_balance_sheets(latest_files: dict[str, str]):
    logger.info("Transforming Balance Sheets...")
    rows = []
    for symbol, key in latest_files.items():
        try:
            data = r2_client.download_json(key)
        except Exception as e:
            logger.error(f"Failed to download balance sheet JSON for {symbol}: {e}")
            continue

        pull_date = data.get("pull_date", key.split('/')[-1].replace('.json', ''))
        
        reports_map = {
            "annual": data.get("annualReports", []),
            "quarterly": data.get("quarterlyReports", [])
        }

        for period_type, reports in reports_map.items():
            for report in reports:
                row = {
                    "symbol": symbol,
                    "fiscal_date_ending": clean_val(report.get("fiscalDateEnding"), "str"),
                    "period_type": period_type,
                    "reported_currency": clean_val(report.get("reportedCurrency"), "str"),
                    "total_assets": clean_val(report.get("totalAssets"), "int"),
                    "total_liabilities": clean_val(report.get("totalLiabilities"), "int"),
                    "total_equity": clean_val(report.get("totalShareholderEquity"), "int"),
                    "cash_and_equivalents": clean_val(report.get("cashAndCashEquivalentsAtCarryingValue"), "int"),
                    "short_term_investments": clean_val(report.get("shortTermInvestments"), "int"),
                    "current_assets": clean_val(report.get("totalCurrentAssets"), "int"),
                    "current_liabilities": clean_val(report.get("totalCurrentLiabilities"), "int"),
                    "long_term_debt": clean_val(report.get("longTermDebt"), "int"),
                    "short_term_debt": clean_val(report.get("shortTermDebt"), "int"),
                    "retained_earnings": clean_val(report.get("retainedEarnings"), "int"),
                    "goodwill": clean_val(report.get("goodwill"), "int"),
                    "intangible_assets": clean_val(report.get("intangibleAssets"), "int"),
                    "pull_date": pull_date
                }
                rows.append(row)
                
    if rows:
        df = pd.DataFrame(rows)
        upsert_parquet(
            df, 
            "silver/fact_balance_sheet/fact_balance_sheet.parquet", 
            ["symbol", "fiscal_date_ending", "period_type"]
        )
    else:
        logger.warning("No balance sheet records parsed.")

def transform_cash_flows(latest_files: dict[str, str]):
    logger.info("Transforming Cash Flows...")
    rows = []
    for symbol, key in latest_files.items():
        try:
            data = r2_client.download_json(key)
        except Exception as e:
            logger.error(f"Failed to download cash flow JSON for {symbol}: {e}")
            continue

        pull_date = data.get("pull_date", key.split('/')[-1].replace('.json', ''))
        
        reports_map = {
            "annual": data.get("annualReports", []),
            "quarterly": data.get("quarterlyReports", [])
        }

        for period_type, reports in reports_map.items():
            for report in reports:
                operating = clean_val(report.get("operatingCashflow"), "int")
                capex = clean_val(report.get("capitalExpenditures"), "int")
                
                # Derive Free Cash Flow: FCF = Operating Cash Flow - abs(Capex)
                # Treat null capex as 0 for this calculation
                capex_val = capex if capex is not None else 0
                operating_val = operating if operating is not None else 0
                fcf = operating_val - abs(capex_val)

                row = {
                    "symbol": symbol,
                    "fiscal_date_ending": clean_val(report.get("fiscalDateEnding"), "str"),
                    "period_type": period_type,
                    "reported_currency": clean_val(report.get("reportedCurrency"), "str"),
                    "operating_cashflow": operating,
                    "capex": capex,
                    "free_cash_flow": fcf,
                    "dividend_payout": clean_val(report.get("dividendPayout"), "int"),
                    "repurchase_of_stock": clean_val(report.get("paymentsForRepurchaseOfCommonStock"), "int"),
                    "proceeds_from_debt": clean_val(report.get("proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet"), "int"),
                    "repayment_of_debt": clean_val(report.get("proceedsFromRepaymentsOfShortTermDebt"), "int"),
                    "investing_cashflow": clean_val(report.get("cashflowFromInvestment"), "int"),
                    "financing_cashflow": clean_val(report.get("cashflowFromFinancing"), "int"),
                    "change_in_cash": clean_val(report.get("changeInCashAndCashEquivalents"), "int"),
                    "pull_date": pull_date
                }
                rows.append(row)
                
    if rows:
        df = pd.DataFrame(rows)
        upsert_parquet(
            df, 
            "silver/fact_cash_flow/fact_cash_flow.parquet", 
            ["symbol", "fiscal_date_ending", "period_type"]
        )
    else:
        logger.warning("No cash flow records parsed.")

def transform_earnings(latest_files: dict[str, str]):
    logger.info("Transforming Earnings...")
    rows = []
    for symbol, key in latest_files.items():
        try:
            data = r2_client.download_json(key)
        except Exception as e:
            logger.error(f"Failed to download earnings JSON for {symbol}: {e}")
            continue

        pull_date = data.get("pull_date", key.split('/')[-1].replace('.json', ''))
        
        # Ingest both annual and quarterly earnings.
        # AV's annualEarnings includes a spurious rolling-TTM entry at the most
        # recent quarter end (not the company's fiscal year end). Filter it out.
        annual_earnings_raw = data.get("annualEarnings", [])
        annual_earnings = filter_real_annual_earnings(annual_earnings_raw)
        dropped = len(annual_earnings_raw) - len(annual_earnings)
        if dropped:
            logger.debug(f"{symbol}: filtered {dropped} spurious annualEarnings row(s)")
        quarterly_earnings = data.get("quarterlyEarnings", [])

        for report in annual_earnings:
            row = {
                "symbol": symbol,
                "fiscal_date_ending": clean_val(report.get("fiscalDateEnding"), "str"),
                "period_type": "annual",
                "reported_eps": clean_val(report.get("reportedEPS"), "float"),
                "estimated_eps": None,
                "surprise": None,
                "surprise_pct": None,
                "report_date": None,
                "pull_date": pull_date
            }
            rows.append(row)

        for report in quarterly_earnings:
            row = {
                "symbol": symbol,
                "fiscal_date_ending": clean_val(report.get("fiscalDateEnding"), "str"),
                "period_type": "quarterly",
                "reported_eps": clean_val(report.get("reportedEPS"), "float"),
                "estimated_eps": clean_val(report.get("estimatedEPS"), "float"),
                "surprise": clean_val(report.get("surprise"), "float"),
                "surprise_pct": clean_val(report.get("surprisePercentage"), "float"),
                # Make sure report_date and fiscal_date_ending are separate DATE columns
                "report_date": clean_val(report.get("reportedDate"), "str"),
                "pull_date": pull_date
            }
            rows.append(row)
                
    if rows:
        df = pd.DataFrame(rows)
        upsert_parquet(
            df, 
            "silver/fact_earnings/fact_earnings.parquet", 
            ["symbol", "fiscal_date_ending", "period_type"]
        )
    else:
        logger.warning("No earnings records parsed.")

def main():
    config_path = os.path.join("config", "ticker_universe.csv")
    try:
        active_symbols = load_active_tickers(config_path)
    except Exception as e:
        logger.error(f"Failed to load ticker universe: {e}")
        return

    logger.info(f"Loaded active ticker universe: {active_symbols}")

    # Process all four fundamental endpoints
    income_files = find_latest_bronze_files("bronze/income_statement/", active_symbols)
    balance_files = find_latest_bronze_files("bronze/balance_sheet/", active_symbols)
    cash_flow_files = find_latest_bronze_files("bronze/cash_flow/", active_symbols)
    earnings_files = find_latest_bronze_files("bronze/earnings/", active_symbols)

    transform_income_statements(income_files)
    transform_balance_sheets(balance_files)
    transform_cash_flows(cash_flow_files)
    transform_earnings(earnings_files)

    logger.info("Fundamentals transformation completed successfully.")

if __name__ == "__main__":
    main()
