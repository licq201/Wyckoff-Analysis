
import os
from integrations.supabase_recommendation import upsert_recommendations
from datetime import datetime

# 模拟一些数据
test_symbols = [
    {"code": "000001", "name": "平安银行", "tag": "测试推荐", "initial_price": 10.5},
    {"code": "600519", "name": "贵州茅台", "tag": "测试推荐", "initial_price": 1700.0}
]
today_int = int(datetime.now().strftime("%Y%m%d"))

print(f"尝试往 Supabase 插入数据，日期: {today_int}")
ok = upsert_recommendations(today_int, test_symbols)
print(f"插入结果: {ok}")
