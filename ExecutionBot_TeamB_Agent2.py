import sys
import time

sys.path.append('../')

import datetime
import uuid
import random
import logging
from time import sleep
import pandas as pd
import argparse

import pickle
import datetime
import numpy

format = "%(asctime)s [%(levelname)s] %(name)s.%(funcName)s() line: %(lineno)d: %(message)s"

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                    level=logging.INFO)

from Management import Management


class ExecutionBot(Management):

    def __init__(self, strategy, starting_money,
                 market_event_securities, market_event_queue, securities,
                 host=None, bot_id=None):

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

    #
    # def __del__(self):
    #     super().__del__()
    #     self.logger.info(f'Connection closed!')


    def start_task(self, sym, action, size):
        self.stat = {
            'strategy': self.strategy,
            'sym': sym,
            'action': action,
            'qty_target': size,
            'bp': self.mid_market[sym]
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
                size_level = min(qty - size, self.market_dict[sym][level][book_side + 'Size'])
                size += int(size_level)

                order_prices.append(self.market_dict[sym][level][book_side + 'Price'])
                order_qty.append(size_level)

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
        try:
            cost_qty = pv / (qty_target - qty) - benchmark_price * 1.
        except Exception:
            cost_qty = 999.99
            benchmark_price = 999.99
        if action == 'buy':
            cost_qty *= -1

        logging.info(
            f'\n\t Aggressive order: {action} {qty_target - qty} {sym} given {min(time.time() - t_start, exec_t)} seconds: \n'
            f'\t Transaction cost: {cost_qty} per share\n'
            f'\t Benchmark price {benchmark_price}\n'
            f'\t Benchmark VWAP: {benchmark_vwap}')

        # final liquidation
        penalty, pv_final = self.final_liquidation(qty, action)

        # final TC

        try:
            cost_qty = (pv + pv_final) / qty - benchmark_price
        except Exception:
            cost_qty = 999.99
            benchmark_price = 999.99
        if action == 'buy':
            cost_qty *= -1.

        return pv, qty

    def final_liquidation(self, qty, action, exec_t=30):

        penalty = 0
        pv_final = 0

        if qty > 0:
            pv_final, _ = self.aggressive_orders(qty, action, exec_t)
            penalty = self.penalty * qty

        return penalty, pv_final

    def customPOV_orders(self, qty, action, exec_t=10, p_rate=0.25, sub_window=3.0):
        """
        send orders whose size is fixed percent of market volume
        assuming the volume within [t, t+dt] = volume within [t-dt, t]
        :param p_rate: fixed percent
        :param sub_window: the sub window to determine order size and place slices of orders
        """

        sym = self.securities[0]

        book_side = 'Ask' if action == 'buy' else 'Bid'
        side = 'B' if action == 'buy' else 'S'

        # benchmark price
        benchmark_price = self.mid_market[sym]
        benchmark_vwap = self.vwap[sym]

        # target qty
        qty_target = qty

        # pv = execution price * volume
        pv = 0

        volume_s = self.traded_volume[sym]
        # while self.traded_volume[sym] == volume_s:
        sleep(sub_window)
        volume_e = self.traded_volume[sym]

        if volume_e == volume_s:
            volume_e = volume_s + 0.1 * qty

        time_s = time.time()

        filename1 = 'DecisionTree_ask.sav'
        filename2 = 'DecisionTree_bid.sav'
        model_ask = pickle.load(open(filename1, 'rb'))
        model_bid = pickle.load(open(filename2, 'rb'))
        mapping = {"GCG0:MBO": 1, "GEH0:MBO": 2, "GEH1:MBO": 3, "GEH2:MBO": 4, "GEH3:MBO": 5, "GEH4:MBO": 6,
                   "GEM0:MBO": 7, "GEM1:MBO": 8, "GEM2:MBO": 9, "GEM3:MBO": 10, "GEM4:MBO": 11, "GEU0:MBO": 12,
                   "GEU1:MBO": 13, "GEU2:MBO": 14, "GEU3:MBO": 15, "GEZ0:MBO": 16, "GEZ1:MBO": 17, "GEZ2:MBO": 18,
                   "GEZ3:MBO": 19, "TNH0:MBO": 20, "UBH0:MBO": 21, "ZBH0:MBO": 22, "ZFH0:MBO": 23, "ZNH0:MBO": 24,
                   "ZTH0:MBO": 25}
        sec = mapping[sym.replace(" ", "")]
        t = datetime.datetime.now()
        tme = t.hour * 60 + t.minute

        qty_slice = 0
        i_slice = 1
        pr = self.market_dict[sym]['L1'][book_side + 'Price']
        prdiff = 0
        while qty_target > 0 and time.time() - time_s < exec_t:


            l1p = self.market_dict[sym]['L1'][book_side + 'Price']
            l1s = self.market_dict[sym]['L1'][book_side + 'Size']
            l2p = self.market_dict[sym]['L2'][book_side + 'Price']
            l2s = self.market_dict[sym]['L2'][book_side + 'Size']
            l3p = self.market_dict[sym]['L3'][book_side + 'Price']
            l3s = self.market_dict[sym]['L3'][book_side + 'Size']
            l4p = self.market_dict[sym]['L4'][book_side + 'Price']
            l4s = self.market_dict[sym]['L4'][book_side + 'Size']
            l5p = self.market_dict[sym]['L5'][book_side + 'Price']
            l5s = self.market_dict[sym]['L5'][book_side + 'Size']

            X = numpy.asarray([sec, tme, l1p, l1s, l2p, l2s, l3p, l3s, l4p, l4s, l5p, l5s]).reshape(1, -1)
            if book_side == "Ask":
                y = model_ask.predict(X).tolist()[0]
            else:
                y = model_bid.predict(X).tolist()[0]

            # min qty = 0.1*qty
            qt = int(min(max((volume_e - volume_s) * p_rate, 0.1 * qty), qty_target))

            size = 0
            book_levels = self.market_event_queue.copy()
            order_prices = []
            order_qty = []
            y = 0
            lvl = 1
            while size < qt and lvl <= 5:
                level = 'L'+str(lvl)
                size_level = min(qt - size, self.market_dict[sym][level][book_side + 'Size'])

                if int(size_level) == 0:
                    lvl += 1
                    continue

                size += int(size_level)

                if y == -1:
                    p_rate += 0.1
                if y == 1:
                    p_rate -= 0.1
                order_prices.append(self.market_dict[sym][level][book_side + 'Price'])

                order_qty.append(size_level)

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
                    logging.info(f"Slice {i_slice} - Passive order sent: \n"
                                 f"\t {order['symb']}: "
                                 f"{order['orderNo']} | "
                                 f"{order['side']} | "
                                 f"{order['remainingQty']} | "
                                 f"{order['price']}")
                    orders.append(order)
                    self.internalID += 1
                lvl += 1

            logging.info(f'Giving {sub_window} seconds for passive orders to be filled...')
            volume_s = self.traded_volume[sym]
            sleep(sub_window)

            for order in orders:

                # make sure all orders are acked on matching engine
                in_id = order["orderNo"]

                while in_id in self.inIds_to_orders_sent:
                    sleep(0.001)
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
                qty_target -= q - qty_slice
                i_slice += 1

        time_e = time.time()

        # avg execution price
        # is it possible to have qty > 0 still?
        try:
            cost_qty = pv / (qty_target - qty) - benchmark_price * 1.
        except Exception:
            cost_qty = 999.99
            benchmark_price = 999.99
        if action == 'buy':
            cost_qty *= -1

        logging.info(f'\n\t Slicing order: {action} {qty - qty_target} {sym}\n'
                     f'\t Given {time_e - time_s} seconds: \n'
                     f'\t Transaction cost: {cost_qty} per share\n'
                     f'\t Benchmark price: {benchmark_price}\n'
                     f'\t Benchmark VWAP: {benchmark_vwap}')

        # final liquidation
        penalty, pv_final = self.final_liquidation(qty_target, action)

        # final TC
        try:
            cost_qty = (pv + pv_final) / qty - benchmark_price
        except Exception:
            cost_qty = 999.99
            benchmark_price = 999.99
        if action == 'buy':
            cost_qty *= -1.

        return i_slice, pv, qty

def AgentB(market_event_securities, side, size, maxtime):
    securities = market_event_securities
    market_event_queue = ["L1", "L2", "L3", "L4", "L5"]

    host = "localhost"
    strategy = "Execution"
    bot_id = 'ExecutionAgent_TeamB'
    starting_money = 1000000000.0

    exec_bot = ExecutionBot(strategy, starting_money, market_event_securities, market_event_queue, securities,
                            host, bot_id)

    time_slice = 0.1
    qty_slice = 0.1
    for _ in range(1):
        exec_bot.customPOV_orders(size, side, exec_t=maxtime, sub_window=time_slice * maxtime)

    exec_bot.task_complete()


if __name__ == "__main__":
    # market_event_securities = ["ZFH0:MBO"]
    # AgentB(market_event_securities,'buy',1000,180)

    myargparser = argparse.ArgumentParser()
    myargparser.add_argument('--strategy', type=str, const="PoV", nargs='?', default="CustomPOV")
    myargparser.add_argument('--symbol', type=str, const="ZNH0: MBO", nargs='?', default="ZNH0: MBO")
    myargparser.add_argument('--action', type=str, const="sell", nargs='?', default="buy")
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

    exec_bot = ExecutionBot(strategy, starting_money, market_event_securities, market_event_queue, securities,
                            host, bot_id, args.username, args.password)
    exec_bot.start_task(args.symbol, args.action, args.size)
    
    time_slice = 0.1
    qty_slice = 0.1
    pv, qty, num_slices = 0, 0, 0
    
    if strategy == 'CustomPOV': 
        num__slices, pv, qty = exec_bot.customPOV_orders(args.size, args.action, exec_t=args.maxtime, sub_window=time_slice * args.maxtime)
    
    end_t = time.time()
    exec_bot.task_complete(pv, qty, end_t-start_t, num_slices)
    sys.exit()

