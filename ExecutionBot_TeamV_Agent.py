import random
import sys
import os
import time
import datetime
import logging
from time import sleep

import argparse
from Management import Management
import volumeprofile as vp
import math

sys.path.append('../')

format = "%(asctime)s [%(levelname)s] %(name)s.%(funcName)s() line: %(lineno)d: %(message)s"

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                    level=logging.INFO)


class ExecutionBot(Management):

    def __init__(self, strategy, starting_money,
                 market_event_securities, market_event_queue, securities,
                 host=None, bot_id=None, username='', password=''):

        super(ExecutionBot, self).__init__(strategy, starting_money,
                                           market_event_securities, market_event_queue, securities,
                                           host, bot_id, username, password)

        self.stat = dict()
        self.start()
        self.penalty = .125
        sleep(0.01)

    def start_task(self, sym, action, size, max_t, regime, start_t):
        self.stat = {
            'sym': sym,
            'action': action,
            'qty_target': size,
            'max_t': max_t,
            'regime': regime,
            'start_t': start_t,
            'bp': self.mid_market[sym]
        }

    def task_complete(self, pv, qty, end_t, slices):
        self.stat['pv'] = pv
        self.stat['qty'] = qty
        self.stat['end_t'] = end_t
        self.stat['exec_t'] = end_t - self.stat['start_t']
        self.stat['slices'] = slices
        self.stop(self.stat, log=True)

    def moneyness(self, initial_price, current_price, side, beta=0.1):
        if not initial_price or initial_price == 0:
            return False
        if side == 'B':
            price_up = current_price / initial_price - 1
            return price_up < beta
        else:
            price_down = 1 - current_price / initial_price
            return price_down < beta

    def aggressive_orders(self, qty, action, exec_t=2):

        sym = self.securities[0]

        book_side = 'Ask' if action == 'buy' else 'Bid'
        side = 'B' if action == 'buy' else 'S'

        benchmark_price = self.stat['bp']
        # setup timer
        t_start = time.time()

        # pv = execution price * volume
        pv = 0

        qty_slice = 0
        # sending aggressive orders til all qty becomes zero
        # It could be more than one loops due to price slippage
        while qty > 0 and time.time() - t_start < exec_t:

            # search the price level that covers all qty
            book_levels = self.market_event_queue.copy()

            # determine the order price/size to be executed
            size = 0
            order_prices = []
            order_qty = []
            while size < qty and len(book_levels) > 0:
                level = book_levels.pop(0)
                try:
                    p = self.market_dict[sym][level][book_side + 'Price']
                    if not self.moneyness(benchmark_price, p, side):
                        continue
                    size_level = min(qty - size, self.market_dict[sym][level][book_side + 'Size'])
                    size += int(size_level)

                    order_prices.append(p)
                    order_qty.append(size_level)
                except KeyError:
                    continue

            # what if the whole book is insufficient? -> qty = size
            if size == 0:
                continue
            qty -= size

            # send orders
            orders = []
            for p, q in zip(order_prices, order_qty):
                order = {'symb': sym,
                         'price': p,
                         'origQty': q,
                         'status': "A",
                         'remainingQty': q,
                         'action': "A",
                         'side': side,
                         'FOK': 0,
                         'AON': 0,
                         'strategy': self.strategy,
                         'orderNo': self.internalID
                         }

                self.send_order(order)
                logging.info(f"Aggressive order sent: \n"
                             f"\t {order['symb']}: "
                             f"{order['orderNo']} | "
                             f"{order['side']} | "
                             f"{order['origQty']} | "
                             f"{order['remainingQty']} | "
                             f"{order['price']}")

                orders.append(order)
                self.internalID += 1

            # cancel orders
            for order in orders:

                in_id = order["orderNo"]

                # make sure all orders are acked on matching engine
                while in_id in self.inIds_to_orders_sent:
                    sleep(0.001)

                # cancel only if the order is not fully filled
                if in_id in self.inIds_to_orders_confirmed:
                    sleep(0.5)

                if in_id in self.inIds_to_orders_confirmed:
                    # get order msg on the book
                    order = self.inIds_to_orders_confirmed[in_id]
                    order['orderNo'] = self.inIds_to_exIds[in_id]

                    self.cancel_order(order)
                    self.logger.info(f"Cancelled order: \n"
                                     f"\t {order['symb']}: "
                                     f"{order['orderNo']} | "
                                     f"{order['side']} | "
                                     f"{order['origQty']} | "
                                     f"{order['remainingQty']} | "
                                     f"{order['price']}")

                    # qty to be filled in next round
                    qty += order['remainingQty']

                    # increment pv by filled qty
                    pv += order['price'] * (order['origQty'] - order['remainingQty'])

                # increment pv by fully filled amount
                else:
                    self.logger.info(f"Fully filled aggressive order: \n"
                                     f"\t {order['symb']}: "
                                     f"{order['orderNo']} | "
                                     f"{order['side']} | "
                                     f"{order['origQty']} | "
                                     f"{order['remainingQty']} | "
                                     f"{order['price']}")

                    pv += order['price'] * order['origQty']

        return pv, qty

    def aggressive_slices_orders(self, qty, action, max_t, beta=0.8):

        sym = self.securities[0]

        book_side = 'Ask' if action == 'buy' else 'Bid'

        qty_target = qty

        qty_slice = 0
        try:
            l1_size = self.market_dict[sym]['L1'][book_side + 'Size']
            frac = qty_target / l1_size
            # if the current market is large, aggressive with no slice
            if frac < beta:
                pv_a, qty_a = self.aggressive_orders(qty_target, action, exec_t=max_t)
                return 1, pv_a, qty_a
            else:
                n_slices = math.ceil(qty_target / (l1_size * beta))
        except(KeyError, ZeroDivisionError):
            n_slices = 10

        pv = 0
        exec_t = max_t / n_slices
        for _ in range(n_slices):

            q = min(qty, math.ceil(qty_target / n_slices) + qty_slice)
            pv_slice, qty_slice = exec_bot.aggressive_orders(q, action, exec_t=exec_t)
            pv += pv_slice
            qty -= q - qty_slice

        return n_slices, pv, qty

    def twap_orders(self, qty, action, n_slices, exec_t=3.0, randomness=10):
        """
        send evenly allocated orders within fixed sub periods, with passive orders followed by aggressive orders
        :param randomness: the randomness range of order size
        :param qty: total target qty
        :param action: 'buy' or 'sell'
        :param exec_t: x seconds
        :param n_slices: # of slices
        :return: transaction cost per share = vwap (executed) - benchmark price
        """

        sym = self.securities[0]

        book_side = 'Bid' if action == 'buy' else 'Ask'
        side = 'B' if action == 'buy' else 'S'

        # benchmark price
        benchmark_price = self.stat['bp']

        # target qty
        qty_target = qty

        # pv = execution price * volume
        pv = 0

        # print(self.market_dict[sym])

        qty_slice = 0
        i = 0
        while i < n_slices:
            try:
                p = self.market_dict[sym]['L1'][book_side + 'Price']
            except KeyError:
                p = benchmark_price

            if not self.moneyness(benchmark_price, p, side):
                continue

            q = min(math.ceil(qty_target / n_slices) + qty_slice + random.randint(-randomness, randomness),
                    qty)  # qty_slice = possible unfilled size in the previous slice

            order = {'symb': sym,
                     'price': p,
                     'origQty': q,
                     'status': "A",
                     'remainingQty': q,
                     'action': "A",
                     'side': side,
                     'FOK': 0,
                     'AON': 0,
                     'strategy': self.strategy,
                     'orderNo': self.internalID
                     }

            self.send_order(order)
            logging.info(f"Slice {i + 1} - Limit order sent: \n"
                         f"\t {order['symb']}: "
                         f"{order['orderNo']} | "
                         f"{order['side']} | "
                         f"{order['remainingQty']} | "
                         f"{order['price']}")

            self.internalID += 1

            logging.info(f'Giving {exec_t} seconds for limit orders to be filled...')
            sleep(exec_t)

            # make sure all orders are acked on matching engine
            in_id = order["orderNo"]

            while in_id in self.inIds_to_orders_sent:
                sleep(0.001)

            # cancel only if the order is not fully filled
            qty_slice = 0
            if in_id in self.inIds_to_orders_confirmed:
                # get order msg on the book
                order = self.inIds_to_orders_confirmed[in_id]
                order['orderNo'] = self.inIds_to_exIds[in_id]

                self.cancel_order(order)
                self.logger.info(f"Cancelled limit order {order['remainingQty']} out of {order['origQty']}: \n"
                                 f"\t {order['symb']}: "
                                 f"{order['orderNo']} | "
                                 f"{order['side']} | "
                                 f"{order['remainingQty']} | "
                                 f"{order['price']}")

                # qty to be filled aggressively
                qty_slice += order['remainingQty']

                # increment pv by fully filled amount
                pv += order['price'] * (order['origQty'] - order['remainingQty'])

                pv_slice, qty_slice = self.aggressive_orders(qty_slice, action)
                pv += pv_slice

            # increment pv by fully filled amount
            else:
                self.logger.info(f"Fully filled limit order: \n"
                                 f"\t {order['symb']}: "
                                 f"{order['orderNo']} | "
                                 f"{order['side']} | "
                                 f"{order['remainingQty']} | "
                                 f"{order['price']}")

                pv += order['price'] * order['origQty']

            qty -= q - qty_slice
            i += 1

        return pv, qty

    def vwap_orders(self, qty, action, n_slices, exec_t=3.0):
        """
        send orders based on volume distribution during a time period, with passive orders followed by aggressive orders
        :param qty: total target qty
        :param action: 'buy' or 'sell'
        :param exec_t: x seconds
        :param n_slices: # of slices
        :return: transaction cost per share = vwap (executed) - benchmark price
        """
        sym = self.securities[0]

        book_side = 'Bid' if action == 'buy' else 'Ask'
        side = 'B' if action == 'buy' else 'S'

        benchmark_price = self.stat['bp']
        # target qty
        qty_target = qty

        # pv = execution price * volume
        pv = 0

        qty_slice = 0

        # get volume profile
        time_s = datetime.datetime.now().strftime("%H:%M:%S")
        max_t = n_slices * exec_t
        volume_profile = vp.get_vp(time_s, max_t, n_slices, 1)

        i = 0
        while i < n_slices:
            try:
                p = self.market_dict[sym]['L1'][book_side + 'Price']
            except KeyError:
                p = benchmark_price

            if not self.moneyness(benchmark_price, p, side):
                continue

            if i == n_slices - 1:
                q = qty
            else:
                q = min(int(qty_target * volume_profile[i]) + qty_slice, qty)  # qty_slice = possible unfilled size in the previous slice

            order = {'symb': sym,
                     'price': p,
                     'origQty': q,
                     'status': "A",
                     'remainingQty': q,
                     'action': "A",
                     'side': side,
                     'FOK': 0,
                     'AON': 0,
                     'strategy': self.strategy,
                     'orderNo': self.internalID
                     }

            # sending the order
            self.send_order(order)
            logging.info(f"Slice {i + 1} - Limit order sent: \n"
                         f"\t {order['symb']}: "
                         f"{order['orderNo']} | "
                         f"{order['side']} | "
                         f"{order['remainingQty']} | "
                         f"{order['price']}")

            self.internalID += 1
            logging.info(f'Giving {exec_t} seconds for limit order to be filled...')
            sleep(exec_t)

            # make sure all orders are acked on matching engine
            in_id = order["orderNo"]

            while in_id in self.inIds_to_orders_sent:
                sleep(0.001)

            # cancel only if the order is not fully filled
            qty_slice = 0
            if in_id in self.inIds_to_orders_confirmed:
                # get order msg on the book
                order = self.inIds_to_orders_confirmed[in_id]
                order['orderNo'] = self.inIds_to_exIds[in_id]

                self.cancel_order(order)
                self.logger.info(f"Cancelled limit order {order['remainingQty']} out of {order['origQty']}: \n"
                                 f"\t {order['symb']}: "
                                 f"{order['orderNo']} | "
                                 f"{order['side']} | "
                                 f"{order['remainingQty']} | "
                                 f"{order['price']}")

                # qty to be filled aggressively
                qty_slice += order['remainingQty']

                # increment pv by fully filled amount
                pv += order['price'] * (order['origQty'] - order['remainingQty'])

                pv_slice, qty_slice = self.aggressive_orders(qty_slice, action)
                pv += pv_slice

            # increment pv by fully filled amount
            else:
                self.logger.info(f"Fully filled limit order: \n"
                                 f"\t {order['symb']}: "
                                 f"{order['orderNo']} | "
                                 f"{order['side']} | "
                                 f"{order['remainingQty']} | "
                                 f"{order['price']}")

                pv += order['price'] * order['origQty']

            qty -= q - qty_slice
            i += 1

        return pv, qty

    def pov_orders(self, qty, action, max_t=10, max_prate=0.4, min_prate=0.2, risk=5, beta=0.95, sub_window=3.0):
        """
        send orders whose size is fixed percent of market volume
        assuming the volume within [t, t+dt] = volume within [t-dt, t]
        :param qty: target quantity
        :param action: buy/sell
        :param max_t: maximum trading time
        :param max_prate: maximum participation rate
        :param min_prate: minimum participation rate
        :param risk: meta-parameter to determine how aggressively we will adjust participation rate
        :param beta: control the decrease/increase rate of participation rate
        :param sub_window: the sub window to determine order size and place slices of orders
        """

        sym = self.securities[0]

        book_side = 'Bid' if action == 'buy' else 'Ask'
        side = 'B' if action == 'buy' else 'S'

        # benchmark price
        benchmark_price = self.stat['bp']

        # target qty
        qty_target = qty

        # pv = execution price * volume
        pv = 0

        # get initial volume
        volume_s = self.traded_volume[sym]
        while self.traded_volume[sym] == volume_s:
            sleep(sub_window)
        volume_e = self.traded_volume[sym]
        prev_volume = 0

        # get volume profile
        n_slices = math.ceil(max_t / sub_window)
        u_vp = vp.get_vp(datetime.datetime.now().strftime("%H:%M:%S"), max_t, n_slices, 0)
        factor = u_vp[0]
        volume_profile = [max(0.1, p / factor) for p in u_vp]
        if volume_profile[0] / sum(volume_profile) > 0.5:
            p_rate = max_prate
        else:
            p_rate = min_prate

        # start trading
        i_slice = 0
        time_s = time.time()

        while qty > 0 and time.time() - time_s < max_t:

            # market closing: send aggressive order
            if time.time() - time_s + sub_window > max_t:
                print("the remaining time is not enough, move to aggressive.")
                pv_slice, qty_slice = self.aggressive_orders(qty, action)
                pv += pv_slice
                i_slice += 1
                break

            try:
                p = self.market_dict[sym]['L1'][book_side + 'Price']
            except KeyError:
                p = benchmark_price

            if not self.moneyness(benchmark_price, p, side):
                continue

            volume = volume_e - volume_s
            # If the current volume is larger than prediction, increase the p_rate
            if prev_volume > 0:
                predict_v = int(prev_volume / volume_profile[i_slice - 1] * volume_profile[i_slice + 1])
                if volume > predict_v * (1 + 1 / risk):
                    p_rate = beta * p_rate + (1 - beta) * max_prate
                elif volume < predict_v / (1 + 1 / risk):
                    p_rate = beta * p_rate - (1 + beta) * min_prate
                prev_volume = volume

            # minimum quantity to trade
            q = int(min(max(volume * p_rate, 0.1 * qty_target), qty))

            order = {'symb': sym,
                     'price': p,
                     'origQty': q,
                     'status': "A",
                     'remainingQty': q,
                     'action': "A",
                     'side': side,
                     'FOK': 0,
                     'AON': 0,
                     'strategy': self.strategy,
                     'orderNo': self.internalID
                     }

            self.send_order(order)
            logging.info(f"Slice {i_slice} - Passive order sent: \n"
                         f"\t {order['symb']}: "
                         f"{order['orderNo']} | "
                         f"{order['side']} | "
                         f"{order['remainingQty']} | "
                         f"{order['price']}")

            self.internalID += 1

            logging.info(f'Giving {sub_window} seconds for passive orders to be filled...')
            volume_s = self.traded_volume[sym]
            sleep(sub_window)
            # print(self.market_dict[sym])

            # make sure all orders are acked on matching engine
            in_id = order["orderNo"]
            while in_id in self.inIds_to_orders_sent:
                sleep(0.001)

            # cancel only if the order is not fully filled
            qty_slice = 0
            if in_id in self.inIds_to_orders_confirmed:
                # get order msg on the book
                order = self.inIds_to_orders_confirmed[in_id]
                order['orderNo'] = self.inIds_to_exIds[in_id]

                self.cancel_order(order)
                self.logger.info(f"Cancelled limit order {order['remainingQty']} out of {order['origQty']}: \n"
                                 f"\t {order['symb']}: "
                                 f"{order['orderNo']} | "
                                 f"{order['side']} | "
                                 f"{order['remainingQty']} | "
                                 f"{order['price']}")

                # qty to be filled aggressively
                qty_slice += order['remainingQty']

                # increment pv by fully filled amount
                pv += order['price'] * (order['origQty'] - order['remainingQty'])

                pv_slice, qty_slice = self.aggressive_orders(qty_slice, action)
                pv += pv_slice

            # increment pv by fully filled amount
            else:
                self.logger.info(f"Fully filled limit order: \n"
                                 f"\t {order['symb']}: "
                                 f"{order['orderNo']} | "
                                 f"{order['side']} | "
                                 f"{order['remainingQty']} | "
                                 f"{order['price']}")

                pv += order['price'] * order['origQty']

            volume_e = self.traded_volume[sym]
            qty -= q - qty_slice
            i_slice += 1

        return i_slice, pv, qty

if __name__ == "__main__":
    # market_event_securities = ["GEH0:MBO","GEM2:MBO","GEU0:MBO"]
    myargparser = argparse.ArgumentParser()
    myargparser.add_argument('--strategy', type=str, const="PoV", nargs='?', default="TWAP")
    myargparser.add_argument('--symbol', type=str, const="ZNH0:MBO", nargs='?', default="ZNH0:MBO")
    myargparser.add_argument('--action', type=str, const="sell", nargs='?', default="sell")
    myargparser.add_argument('--size', type=int, const=1000, nargs='?', default=100)
    myargparser.add_argument('--maxtime', type=int, const=120, nargs='?', default=120)
    myargparser.add_argument('--regime', type=str, const='text', nargs= '?', default='text')
    myargparser.add_argument('--username', type=str, default='test')
    myargparser.add_argument('--password', type=str, default='test')
    myargparser.add_argument('--bot_id', type=str, const='text', nargs='?', default='text')
    args = myargparser.parse_args()

    market_event_securities = [args.symbol]
    market_event_queue = ["L1", "L2", "L3", "L4", "L5"]
    securities = market_event_securities

    host = "localhost"
    strategy = args.strategy
    bot_id = args.bot_id
    starting_money = 1000000000.0

    start_t = datetime.datetime.now()
    exec_bot = ExecutionBot(strategy, starting_money, market_event_securities, market_event_queue, securities,
                            host, bot_id, args.username, args.password)
    exec_bot.start_task(args.symbol, args.action, args.size, args.maxtime, args.regime, start_t)

    # aggressive algo
    pv, qty, num_slices = 0, 0, 0
    if strategy == 'AggressiveWithSlice':
        num_slices, pv, qty = exec_bot.aggressive_slices_orders(args.size, args.action, args.maxtime, beta=0.5)
    elif strategy == 'PoV':
        num_slices, pv, qty = exec_bot.pov_orders(args.size, args.action, max_t=args.maxtime)
    elif strategy == 'TWAP':
        num_slices = max(10, int(args.maxtime/3))
        pv, qty = exec_bot.twap_orders(args.size, args.action, num_slices, float(args.maxtime / num_slices))
    elif strategy == 'VWAP':
        num_slices = max(10, int(args.maxtime/3))
        pv, qty = exec_bot.vwap_orders(args.size, args.action, num_slices, float(args.maxtime / num_slices))
    elif strategy == 'AggressiveNoSlices':
        num__slices, pv, qty = exec_bot.aggressive_slices_orders(args.size, args.action, args.maxtime,beta=10.)

    end_t = datetime.datetime.now()
    exec_bot.task_complete(pv, qty, end_t, num_slices)
    sys.exit()
