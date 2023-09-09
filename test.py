import websocket
import pandas as pd
from json import loads, dumps
import pandas_ta as ta
import asyncio


class BinanceWebSocketConn(websocket.WebSocketApp):
    def __init__(self, url):
        super().__init__(url, 
                         on_open=self.on_open, 
                         on_message=self.on_message,
                         on_error=self.on_error, 
                         on_close=self.on_close)
        
        self.close_prices = []
        self.rsi_length = 14

    def calculate_rsi(self, data):
        ser_data = pd.Series(data)
        diff_price = ser_data.diff(1)
        positive_diff = diff_price[diff_price > 0]
        negative_diff = abs(diff_price[diff_price < 0])

        if not positive_diff.empty:
            positive_avg = positive_diff.mean()
        else:
            positive_avg = 0

        if not negative_diff.empty:
            negative_avg = negative_diff.mean()
        else:
            negative_avg = 0     

        if negative_avg == 0:
            rsi = 100
        else:
            rsi = 100 - (100 / (1 + (positive_avg / negative_avg)))
        
        return rsi

    def on_open(self, ws):
        print("Websocket opened (binance)")
    
    def on_message(self, ws, message):

        data = loads(message)
        close_price = float(data['k']['c'])

        self.close_prices.append(close_price)

        if len(self.close_prices) > self.rsi_length:
            self.close_prices.pop(0)

        if data['k']['x'] == True and len(self.close_prices) == self.rsi_length:
            rsi = self.calculate_rsi(self.close_prices)
            print(f"Close price of Binance: {self.close_prices[-1]}, RSI =", rsi)
    
    def on_error(self, ws, error):
        print("Error", error)
    
    def on_close(self, ws):
        print("Websocket connection closed.")


class BitfinexWebSocketConn(websocket.WebSocketApp):
    def __init__(self, url):
        super().__init__(url, 
                         on_open=self.on_open, 
                         on_message=self.on_message,
                         on_error=self.on_error, 
                         on_close=self.on_close)
        
        self.candles = pd.DataFrame(
            columns=
            [
                'timestamp', 
                'open', 
                'close', 
                'high', 
                'low', 
                'volume'
            ]
        )

    def on_open(self, ws):
        print("WebSocket opened (bitfinex)")
        subscribe_msg = {
            "event": "subscribe",
            "channel": "candles",
            "key": "trade:1m:tBTCUSD"
        }
        ws.send(dumps(subscribe_msg))

    def on_message(self, ws, message):
        data = loads(message)

        if isinstance(data, list) and len(data) > 1 \
            and isinstance(data[1], list) and len(data[1]) >= 6:
            timestamp, open_price, close_price, high_price, low_price, volume = data[1]
            new_candle = pd.DataFrame(
                [
                    [
                        timestamp, 
                        open_price, 
                        close_price, 
                        high_price, 
                        low_price, 
                        volume
                    ]
                ],
                columns=[
                    'timestamp', 
                    'open', 
                    'close', 
                    'high', 
                    'low', 
                    'volume'
                ]
            )

            new_candle['timestamp'] = pd.to_datetime(new_candle['timestamp'], 
                                                    unit='ms', errors='coerce')
            
            new_candle.set_index(pd.DatetimeIndex(new_candle["timestamp"]), inplace=True)
            new_candle.sort_index()
            
            if self.candles.empty:
                self.candles = new_candle
            else:
                self.candles = pd.concat([self.candles, new_candle], ignore_index=False)

            vwap = ta.vwap(
                self.candles['high'], 
                self.candles['low'], 
                self.candles['close'], 
                self.candles['volume']
            )
            print(f"Close price of Bitfinex: {self.candles['close'].iloc[-1]}, VWAPvwap: {vwap.iloc[-1]}")
    
    def on_error(self, ws, error):
        print(f"WebSocket Error: {error}")

    def on_close(self, ws):
            print("WebSocket closed")


async def main():
    url_binance = "wss://stream.binance.com:9443/ws/btcusdt@kline_5m"
    url_bitfinex = "wss://api.bitfinex.com/ws/2/trade:1m:tBTCUSD/hist"

    binance_ws = BinanceWebSocketConn(url_binance)
    bitfinex_ws = BitfinexWebSocketConn(url_bitfinex)


    loop = asyncio.get_event_loop()

    loop.run_in_executor(None, binance_ws.run_forever)
    loop.run_in_executor(None, bitfinex_ws.run_forever)

    while True:
        await asyncio.sleep(1) 

if __name__ == "__main__":
    asyncio.run(main())
