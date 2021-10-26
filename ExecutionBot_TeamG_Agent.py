import sys
import time
import threading
import datetime
import uuid
import logging
from time import sleep
import pandas as pd
import argparse
from Management import Management
import volumeprofile as vp
import math

sys.path.append('../')

format = "%(asctime)s [%(levelname)s] %(name)s.%(funcName)s() line: %(lineno)d: %(message)s"

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', level=logging.INFO)


"""
2021/04/11
Fixed:  Can not normally exit program due to pika consumer remain consuming message, 
should cancel all consumer or call stop_consuming inside callback function

Solution: Added a member variable Communication.terminate to indicate the thread state 
if the value is "True", the first consumer callback function will call stop_consuming 
which close all consumer, then the program will exit in normal. 
"""

class ExecutionBot(Management):

    def __init__(self, strategy, starting_money,
                 market_event_securities, market_event_queue, securities,
                 host=None, bot_id=None, username='', password=''):

        super(ExecutionBot, self).__init__(strategy, starting_money,
                                           market_event_securities, market_event_queue, securities,
                                           host, bot_id, username, password)

        # # Subscription to order book in order to passively send orders
        # self.kickoff()

        self.stat = dict()

        # actively send orders
        self.start()

        # penalty per share to execute whatever left after algo
        self.penalty = .125

        # give some time for agent to receive queue in channel since it is 'direct' for now
        sleep(10)

    def start_task(self, sym, action, size):
        self.stat = {
            'strategy': self.strategy,
            'sym': sym,
            'action': action,
            'qty_target': size,
            'bp': self.mid_market[sym],
            'vwap': self.vwap[sym]
        }

    def task_complete(self, pv, qty, time_t, slices):
        self.stat['pv'] = pv
        self.stat['qty'] = qty
        self.stat['time_t'] = time_t
        self.stat['slices'] = slices
        self.stop(self.stat, log=True)

    def aggressive_orders(self, qty, action, exec_t=2, log=False):

        sym = self.securities[0]

        book_side = 'Ask' if action == 'buy' else 'Bid'
        side = 'B' if action == 'buy' else 'S'

        # benchmark price
        benchmark_price = self.mid_market[sym]
        benchmark_vwap = self.vwap[sym]

        # target qty
        qty_target = qty

        # setup timer
        t_start = time.time()

        # pv = execution price * volume
        pv = 0

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
                    size_level = min(qty - size, self.market_dict[sym][level][book_side + 'Size'])
                    size += int(size_level)

                    order_prices.append(self.market_dict[sym][level][book_side + 'Price'])
                    order_qty.append(size_level)
                except KeyError:
                    pass

            # print(order_qty)

            # TODO: what if the whole book is insufficient? -> qty = size

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
            qty = 0

            for order in orders:

                in_id = order["orderNo"]

                # make sure all orders are acked on matching enginee
                while in_id in self.inIds_to_orders_sent:
                    sleep(0.001)
                    # print(f'waiting for pending orders...')

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
                                     f"{order['remainingQty']} | "
                                     f"{order['price']}")

                    pv += order['price'] * order['origQty']

        # avg execution price
        # is it possible to have qty > 0 still?
        if (qty_target - qty) != 0.0: 
            cost_qty = pv / (qty_target - qty) - benchmark_price*1.0
        else:
            cost_qty = 0.0
        if action == 'sell':
            cost_qty *= -1

        logging.info(
            f'\n\t Aggressive order: {action} {qty_target - qty} {sym} given {min(time.time() - t_start, exec_t)} seconds: \n'
            f'\t Transaction cost: {cost_qty} per share\n'
            f'\t Benchmark price {benchmark_price}\n'
            f'\t Benchmark VWAP: {benchmark_vwap}')

        # final liquidation
        penalty, pv_final = self.final_liquidation(qty, action)

        # final TC
        cost_qty = (pv + pv_final) / qty_target - benchmark_price

        return pv, qty

    def moneyness(self, initial_price, current_price, side, beta=0.1):
        if initial_price == 0:
            return False
        if side == 'B':
            price_up = current_price/initial_price - 1
            print("The current_price is {}, the initial price is {}, price up is {}".format(current_price, initial_price, price_up))
            return price_up < beta
        else:
            price_down = 1-current_price/initial_price
            print("The current_price is {}, the initial price is {}, price down is {}".format(current_price, initial_price, price_down))
            return price_down < beta

    def pov_orders(self, qty, action, max_t=10, max_prate=0.4, min_prate=0.2, risk=5, beta = 0.95, sub_window=3.0):
        """
        send orders whose size is fixed percent of market volume
        assuming the volume within [t, t+dt] = volume within [t-dt, t]
        :param sub_window: the sub window to determine order size and place slices of orders
        """

        sym = self.securities[0]

        book_side = 'Bid' if action == 'buy' else 'Ask'
        side = 'B' if action == 'buy' else 'S'

        # benchmark price
        benchmark_price = self.mid_market[sym]
        benchmark_vwap = self.vwap[sym]

        if benchmark_price is None:
            benchmark_price = 0

        # target qty
        qty_target = qty

        # pv = execution price * volume
        pv = 0

        # get initial volume
        volume_s = self.traded_volume[sym]
        # while self.traded_volume[sym] == volume_s:
        sleep(sub_window)
        volume_e = self.traded_volume[sym]
        prev_volume = 0

        # get volume profile
        start_t = datetime.datetime.now().strftime("%H:%M:%S")
        print("sym:{}, start_time: {}, max_time: {}, time_slices: {}".format(sym, start_t, max_t, int(max_t / sub_window)))
        # get volume profile
        u_vp = vp.get_vp(start_t, max_t, math.ceil(max_t / sub_window), 0)
        print("the original volume_profile is {}".format(u_vp))
        # normalize the volume profile with the volume of the first time slice
        factor = u_vp[0]
        volume_profile = [max(0.1, p/factor) for p in u_vp]
        print("volume profile: {}".format(volume_profile))
        # set the initial participation rate
        if volume_profile[0]/sum(volume_profile) > 0.5:
            p_rate = max_prate
        else:
            p_rate = min_prate

        # start trading
        qty_slice = 0
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
                print("{}Price doesn't exist in L1 market".format(book_side))
                p = benchmark_price

            if benchmark_price == 0 and self.mid_market[sym] is not None:
                benchmark_price = self.mid_market[sym]
            if not self.moneyness(benchmark_price, p, side):
                continue

            volume = volume_e - volume_s
            # If the current volume is larger than prediction, increase the p_rate
            if prev_volume > 0:
                predict_v = int(prev_volume/volume_profile[i_slice-1]*volume_profile[i_slice+1])
                print(f'current volume is {volume}, predict volume is {predict_v}')
                if volume > predict_v * (1 + 1/risk):
                    p_rate = beta * p_rate + (1-beta) * max_prate
                elif volume < predict_v / (1 + 1/risk):
                    p_rate = beta * p_rate - (1+beta) * min_prate
                prev_volume = volume

            # minimum quantity to trade
            min_q = int(0.1*qty_target)
            q = int(min(max(volume * p_rate, min_q), qty))

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
            wait_t = 0
            while in_id in self.inIds_to_orders_sent and wait_t < 10:
                sleep(0.001)
                wait_t += 0.001
                # print(self.inIds_to_orders_sent)
                # print(self.inIds_to_orders_confirmed)
                # print(f'waiting for pending orders...')

            # cancel only if the order is not fully filled
            in_id = order["orderNo"]
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

        time_e = time.time()

        # avg execution price
        cost_qty = pv / qty_target - benchmark_price
        if action == 'sell':
            cost_qty *= -1

        logging.info(f'\n\t Slicing order: {action} {qty_target-qty} {sym}\n'
                     f'\t Given {time_e - time_s} seconds: \n'
                     f'\t Transaction cost: {cost_qty} per share\n'
                     f'\t Benchmark price: {benchmark_price}\n'
                     f'\t Benchmark VWAP: {benchmark_vwap}')

        # final liquidation
        penalty, pv_final = self.final_liquidation(qty, action)

        # final TC
        cost_qty = (pv + pv_final) / qty_target - benchmark_price

        print("The transaction cost is {}".format(cost_qty))

        return i_slice, pv, qty

    def final_liquidation(self, qty, action, exec_t=30):

        penalty = 0
        pv_final = 0

        if qty > 0:
            pv_final, _ = self.aggressive_orders(qty, action, exec_t)
            penalty = self.penalty * qty

        return penalty, pv_final

if __name__ == "__main__":
    # market_event_securities = ["GEH0:MBO","GEM2:MBO","GEU0:MBO"]
    myargparser = argparse.ArgumentParser()
    myargparser.add_argument('--strategy', type=str, const="PoV", nargs='?', default="PoV")
    myargparser.add_argument('--symbol', type=str, const="ZNH0:MBO", nargs='?', default="ZNH0:MBO")
    myargparser.add_argument('--action', type=str, const="sell", nargs='?', default="sell")
    myargparser.add_argument('--size', type=int, const=1000, nargs='?', default=100)
    myargparser.add_argument('--maxtime', type=int, const=120, nargs='?', default=120)
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

    start_t = time.time()
    exec_bot = ExecutionBot(strategy, starting_money, market_event_securities, market_event_queue, securities,
                            host, bot_id, args.username, args.password)
    exec_bot.start_task(args.symbol, args.action, args.size)

    pv, qty, num_slices = 0, 0, 0
    if strategy == 'PoV':
        num_slices, pv, qty = exec_bot.pov_orders(args.size, args.action, max_t=5)

    end_t = time.time()
    exec_bot.task_complete(pv, qty, end_t-start_t, num_slices)
    sys.exit()
