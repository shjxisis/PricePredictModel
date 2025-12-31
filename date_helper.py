import requests
import pandas as pd
from datetime import datetime
import time


class BinanceKlinesFetcher:
    def __init__(self, use_proxy=False, proxy_url=None, max_retries=3):
        """初始化币安K线数据获取器"""
        self.base_url = "https://api.binance.com/api/v3/klines"
        self.proxies = None
        self.max_retries = max_retries
        if use_proxy and proxy_url:
            self.proxies = {'http': proxy_url, 'https': proxy_url}

    def test_connection(self):
        """测试API连接"""
        try:
            response = requests.get(
                "https://api.binance.com/api/v3/ping",
                timeout=10,
                proxies=self.proxies
            )
            return response.status_code == 200
        except:
            return False

    def get_klines(self, symbol, interval, start_time=None, end_time=None, limit=1000):
        """
        获取K线数据（带重试机制）

        参数:
        symbol: 交易对，如 'BTCUSDT'
        interval: 时间间隔，如 '1m', '5m', '15m', '1h', '4h', '1d'
        start_time: 开始时间戳(毫秒)
        end_time: 结束时间戳(毫秒)
        limit: 每次请求的数量，最大1000
        """
        params = {
            'symbol': symbol.upper(),
            'interval': interval,
            'limit': min(limit, 1000)
        }

        if start_time is not None:
            params['startTime'] = int(start_time)
        if end_time is not None:
            params['endTime'] = int(end_time)

        for attempt in range(self.max_retries):
            try:
                response = requests.get(self.base_url, params=params, timeout=30, proxies=self.proxies)

                # 处理限流 (HTTP 429 或 418)
                if response.status_code in [429, 418]:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    print(f"触发限流，等待 {retry_after} 秒后重试...")
                    time.sleep(retry_after)
                    continue

                if response.status_code == 200:
                    return response.json()

                # 其他错误
                if attempt < self.max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    print(f"请求失败 (状态码: {response.status_code})，{wait_time}秒后重试...")
                    time.sleep(wait_time)
                    continue

                return None

            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    print(f"请求超时，{(attempt + 1) * 2}秒后重试...")
                    time.sleep((attempt + 1) * 2)
                    continue
                return None
            except Exception as e:
                if attempt < self.max_retries - 1:
                    print(f"请求异常: {e}，{(attempt + 1) * 2}秒后重试...")
                    time.sleep((attempt + 1) * 2)
                    continue
                return None

        return None

    def fetch_all_klines(self, symbol, interval, start_date=None, end_date=None, delay=0.5):
        """
        获取所有历史K线数据

        参数:
        symbol: 交易对，如 'BTCUSDT'
        interval: 时间间隔
        start_date: 开始日期字符串，如 '2023-01-01'，None则从2017年开始
        end_date: 结束日期字符串，如 '2024-01-01'，None则到现在
        delay: 每次请求间隔秒数，避免触发限流
        """
        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000) if start_date else int(
            datetime(2017, 8, 1).timestamp() * 1000)
        end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000) if end_date else int(
            datetime.now().timestamp() * 1000)

        all_klines = []
        current_ts = start_ts
        request_count = 0
        failed_count = 0

        while current_ts < end_ts:
            request_count += 1
            klines = self.get_klines(symbol, interval, current_ts, end_ts, 1000)

            if not klines:
                failed_count += 1
                if failed_count >= 5:
                    print(f"连续失败{failed_count}次，停止获取")
                    break
                print(f"第{request_count}次请求失败，继续...")
                time.sleep(delay * 2)
                continue

            failed_count = 0  # 重置失败计数

            if len(klines) == 0:
                break

            all_klines.extend(klines)
            current_ts = klines[-1][0] + 1

            if request_count % 10 == 0:
                print(f"已获取 {len(all_klines)} 条数据...")

            if len(klines) < 1000:
                break

            time.sleep(delay)  # 避免触发限流

        print(f"获取完成！总共 {len(all_klines)} 条数据")
        return all_klines

    def klines_to_dataframe(self, klines):
        """将K线数据转换为DataFrame"""
        if not klines:
            return pd.DataFrame()

        df = pd.DataFrame(klines, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignore'
        ])

        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')

        numeric_columns = ['open', 'high', 'low', 'close', 'volume',
                           'quote_volume', 'taker_buy_base', 'taker_buy_quote']
        df[numeric_columns] = df[numeric_columns].astype(float)
        df['trades'] = df['trades'].astype(int)

        return df.drop('ignore', axis=1)

    def save_to_csv(self, df, filename):
        """保存数据到CSV文件"""
        if not df.empty:
            df.to_csv(filename, index=False)
            print(f"数据已保存到 {filename}")


if __name__ == "__main__":
    # 配置
    USE_PROXY = True
    PROXY_URL = "http://127.0.0.1:7890"
    SYMBOL = "BTCUSDT"
    INTERVAL = "15m"
    MAX_RETRIES = 3  # 最大重试次数
    REQUEST_DELAY = 0.5  # 请求间隔秒数，避免限流

    # 初始化
    fetcher = BinanceKlinesFetcher(use_proxy=USE_PROXY, proxy_url=PROXY_URL, max_retries=MAX_RETRIES)

    if not fetcher.test_connection():
        print("连接失败，请检查代理设置")
        exit(1)

    print(f"开始获取 {SYMBOL} {INTERVAL} K线数据...")

    # 获取数据（选择一种方式）
    klines = fetcher.fetch_all_klines(SYMBOL, INTERVAL, delay=REQUEST_DELAY)  # 全部历史
    # klines = fetcher.fetch_all_klines(SYMBOL, INTERVAL, start_date="2024-01-01", delay=REQUEST_DELAY)  # 指定开始
    # klines = fetcher.fetch_all_klines(SYMBOL, INTERVAL, "2024-01-01", "2024-12-31", delay=REQUEST_DELAY)  # 指定范围

    # 转换并保存
    df = fetcher.klines_to_dataframe(klines)
    if not df.empty:
        print(f"数据范围: {df['open_time'].min()} 到 {df['open_time'].max()}")
        fetcher.save_to_csv(df, f"{SYMBOL}_{INTERVAL}.csv")