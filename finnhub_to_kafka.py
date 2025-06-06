import finnhub
from kafka import KafkaProducer
import json
import time
from dotenv import load_dotenv
import os

# โหลดตัวแปรสภาพแวดล้อม
load_dotenv()

# ตั้งค่า Finnhub client
finnhub_api_key = os.getenv("FINNHUB_API_KEY")
finnhub_client = finnhub.Client(api_key=finnhub_api_key)

# สร้าง Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def extract_key_financial_data(report_data):
    """ดึงข้อมูลทางการเงินที่สำคัญจาก report"""
    
    financial_summary = {
        'company_info': {
            'symbol': report_data.get('symbol'),
            'year': report_data.get('year'),
            'quarter': report_data.get('quarter'),
            'form': report_data.get('form'),
            'filed_date': report_data.get('filedDate')
        },
        'income_statement': {},
        'balance_sheet': {},
        'cash_flow': {}
    }
    
    # ดึงข้อมูลจาก Income Statement (ic)
    if 'report' in report_data and 'ic' in report_data['report']:
        ic_data = report_data['report']['ic']
        for item in ic_data:
            concept = item.get('concept', '')
            if 'RevenueFromContract' in concept:
                financial_summary['income_statement']['net_sales'] = item.get('value')
            elif 'CostOfGoodsAndServicesSold' in concept:
                financial_summary['income_statement']['cost_of_sales'] = item.get('value')
            elif 'GrossProfit' in concept:
                financial_summary['income_statement']['gross_profit'] = item.get('value')
            elif 'OperatingIncomeLoss' in concept:
                financial_summary['income_statement']['operating_income'] = item.get('value')
            elif 'NetIncomeLoss' in concept:
                financial_summary['income_statement']['net_income'] = item.get('value')
            elif 'EarningsPerShareDiluted' in concept:
                financial_summary['income_statement']['eps_diluted'] = item.get('value')
    
    # ดึงข้อมูลจาก Balance Sheet (bs)
    if 'report' in report_data and 'bs' in report_data['report']:
        bs_data = report_data['report']['bs']
        for item in bs_data:
            concept = item.get('concept', '')
            if 'Assets' in concept and concept == 'us-gaap_Assets':
                financial_summary['balance_sheet']['total_assets'] = item.get('value')
            elif 'Liabilities' in concept and concept == 'us-gaap_Liabilities':
                financial_summary['balance_sheet']['total_liabilities'] = item.get('value')
            elif 'StockholdersEquity' in concept:
                financial_summary['balance_sheet']['shareholders_equity'] = item.get('value')
            elif 'CashAndCashEquivalents' in concept:
                financial_summary['balance_sheet']['cash_and_equivalents'] = item.get('value')
    
    # ดึงข้อมูลจาก Cash Flow (cf)
    if 'report' in report_data and 'cf' in report_data['report']:
        cf_data = report_data['report']['cf']
        for item in cf_data:
            concept = item.get('concept', '')
            if 'NetCashProvidedByUsedInOperatingActivities' in concept:
                financial_summary['cash_flow']['operating_cash_flow'] = item.get('value')
            elif 'NetCashProvidedByUsedInInvestingActivities' in concept:
                financial_summary['cash_flow']['investing_cash_flow'] = item.get('value')
            elif 'NetCashProvidedByUsedInFinancingActivities' in concept:
                financial_summary['cash_flow']['financing_cash_flow'] = item.get('value')
    
    return financial_summary

# รายการหุ้น S&P 500 ที่สำคัญ
sp500_symbols = [
    'AAPL',  # Apple Inc.
    'MSFT',  # Microsoft Corporation
    'GOOGL', # Alphabet Inc. Class A
    'AMZN',  # Amazon.com Inc.
    'TSLA',  # Tesla Inc.
    'BRK.B', # Berkshire Hathaway Inc. Class B
    'META',  # Meta Platforms Inc.
    'NVDA',  # NVIDIA Corporation
    'JPM',   # JPMorgan Chase & Co.
    'JNJ',   # Johnson & Johnson
    'V',     # Visa Inc.
    'PG',    # Procter & Gamble Company
    'UNH',   # UnitedHealth Group Incorporated
    'HD',    # Home Depot Inc.
    'MA',    # Mastercard Incorporated
    'BAC',   # Bank of America Corporation
    'PFE',   # Pfizer Inc.
    'XOM',   # Exxon Mobil Corporation
    'DIS',   # Walt Disney Company
    'KO',    # Coca-Cola Company
    'NFLX',  # Netflix Inc.
    'ADBE',  # Adobe Inc.
    'CRM',   # Salesforce Inc.
    'WMT',   # Walmart Inc.
    'CVX'    # Chevron Corporation
]

frequency = 'quarterly'  # หรือ 'annual' สำหรับรายงานประจำปี

while True:
    try:
        for symbol in sp500_symbols:
            print(f"Processing {symbol}...")
            data = finnhub_client.financials_reported(symbol=symbol, freq=frequency)

            if data and "data" in data and len(data["data"]) > 0:
                # กรองเฉพาะข้อมูลปี 2025
                reports_2025 = [report for report in data["data"] if report.get('year') == 2025]
                
                if reports_2025:
                    for report in reports_2025[:1]:  # ส่งรายงานล่าสุดปี 2025 1 ฉบับต่อบริษัท
                        # ดึงข้อมูลสำคัญจาก report
                        key_financial_data = extract_key_financial_data(report)
                        
                        # ส่งข้อมูลไปยัง Kafka
                        producer.send('financial_data', value=key_financial_data)
                        
                        print(f"✓ Sent financial data for {symbol} - Year: {key_financial_data['company_info']['year']}")
                        
                        # Format numbers with commas, handle N/A cases
                        net_sales = key_financial_data['income_statement'].get('net_sales', 'N/A')
                        net_income = key_financial_data['income_statement'].get('net_income', 'N/A')
                        total_assets = key_financial_data['balance_sheet'].get('total_assets', 'N/A')
                        
                        print(f"  Net Sales: ${net_sales:,.0f}" if net_sales != 'N/A' else "  Net Sales: N/A")
                        print(f"  Net Income: ${net_income:,.0f}" if net_income != 'N/A' else "  Net Income: N/A")
                        print(f"  Total Assets: ${total_assets:,.0f}" if total_assets != 'N/A' else "  Total Assets: N/A")
                else:
                    print(f"✗ No 2025 financial data found for {symbol}")
            else:
                print(f"✗ No financial data found for {symbol}")
            
            # หน่วงเวลาระหว่างการดึงข้อมูลแต่ละบริษัท (ป้องกัน rate limit)
            time.sleep(1)
        
        print("=" * 60)
        print("Completed processing all symbols. Waiting for next cycle...")
        print("=" * 60)
        
        producer.flush()  # ให้แน่ใจว่าข้อมูลถูกส่งไปแล้ว
        time.sleep(3600)  # ดึงข้อมูลทุก 1 ชั่วโมง
    except Exception as e:
        print("Error:", e)
        time.sleep(60)