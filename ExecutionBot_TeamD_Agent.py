import sys
import time

sys.path.append('../')

import datetime
import uuid
import random
import logging
from time import sleep
import pandas as pd
import os
import argparse
format = "%(asctime)s [%(levelname)s] %(name)s.%(funcName)s() line: %(lineno)d: %(message)s"

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                    level=logging.INFO)


from Management import Management


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
        self.penalty = 10

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
            'bp': self.mid_market[sym],
            'vwap': self.vwap[sym]
        }

    def task_complete(self, pv, qty, time_t, slices):
        self.stat['pv'] = pv
        self.stat['qty'] = qty
        self.stat['time_t'] = time_t
        self.stat['slices'] = slices

    def aggressive_orders(self, qty, action, exec_t=2, log=True):

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
                except Exception:
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
        try:
            cost_qty = pv / (qty_target - qty) - benchmark_price * 1.
        except Exception:
            if qty_target - qty==0:
                cost_qty = 0.0
            else:
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

        cost_qty = (pv + pv_final) / qty_target - benchmark_price
        if action == 'buy':
            cost_qty *= -1

        return pv, qty

    def calculate_limit(self, benchmark_price, usr_limit, ema=False, beta=0.95, theta=0.5, p=None):
        if ema:
            new_benchmark_price = beta * benchmark_price + (1-beta) * p
        else:
            new_benchmark_price = benchmark_price
        limit_b_price = (1 + theta) * new_benchmark_price
        limit_s_price = (1 - theta) * new_benchmark_price
        if usr_limit is not None:
            limit_b_price = min(limit_b_price, usr_limit)
            limit_s_price = max(limit_s_price, usr_limit)
        return limit_b_price, limit_s_price, new_benchmark_price

    def sniper_orders(self, qty, action, exec_t=10, p_rate=0.5, sub_window=3.0, usr_limit=None, log=True):
        """
        send orders whose size is fixed percent of market volume
        assuming the volume within [t, t+dt] = volume within [t-dt, t]
        :param usr_limit: user defined price limit
        :param p_rate: fixed percent
        :param sub_window: the sub window to determine order size and place slices of orders
        """

        sym = self.securities[0]

        book_side = 'Bid' if action == 'buy' else 'Ask'
        side = 'B' if action == 'buy' else 'S'

        # benchmark price
        benchmark_price = self.mid_market[sym]
        benchmark_vwap = self.vwap[sym]

        if benchmark_price is not None:
            ema_enable = False
        else:
            benchmark_price = 0
            ema_enable = True

        # user defined limit if applied


        # target qty
        qty_target = qty

        # pv = execution price * volume
        pv = 0

        # print(self.market_dict[sym])
        volume_s = self.traded_volume[sym]
        while self.traded_volume[sym] == volume_s:
            sleep(sub_window)
        volume_e = self.traded_volume[sym]

        time_s = time.time()

        qty_slice = 0
        i_slice = 1

        # percentage of limitation
        theta = 0.5
        
        while qty_target > 0 and time.time() - time_s < exec_t:
            p = self.market_dict[sym]['L1'][book_side + 'Price']
            # TODO: change limit price in transaction but not fixed
            # price limit for sale and buy
            limit_b_price, limit_s_price, benchmark_price = self.calculate_limit(benchmark_price, usr_limit, theta=theta,
                                                                                 ema=ema_enable, p=p)
            if side == 'B' and p > limit_b_price:
                logging.info("Exceed limit buying price")
                continue
            elif side == 'S' and p < limit_s_price:
                logging.info("Exceed limit selling price")
                continue

            # min qty = 0.1*qty
            q = int(min(max((volume_e - volume_s) * p_rate, 0.1 * qty), qty_target))

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
            if time.time() - time_s - exec_t < 1:
                logging.info("Less than 1 min, change to aggresive:")
                p_remain, qty_remain = self.aggressive_orders(qty_target, action)
                # log here
                return

            # make sure all orders are acked on matching engine
            in_id = order["orderNo"]

            # TODO: Do we need to solve the failure of aggressive order?
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
        cost_qty = pv / qty - benchmark_price
        if action == 'sell':
            cost_qty *= -1

        logging.info(f'\n\t Slicing order: {action} {qty - qty_target} {sym}\n'
                     f'\t Given {time_e - time_s} seconds: \n'
                     f'\t Transaction cost: {cost_qty} per share\n'
                     f'\t Benchmark price: {benchmark_price}\n'
                     f'\t Benchmark VWAP: {benchmark_vwap}')

        # final liquidation
        penalty, pv_final = self.final_liquidation(qty_target, action)

        # final TC
        cost_qty = (pv + pv_final) / qty - benchmark_price

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
    myargparser.add_argument('--strategy', type=str, const="SniperA", nargs='?', default="SniperA")
    myargparser.add_argument('--symbol', type=str, const="ZNH0:MBO", nargs='?', default="ZBH0:MBO")
    myargparser.add_argument('--action', type=str, const="buy", nargs='?', default="buy")
    myargparser.add_argument('--size', type=int, const=1000, nargs='?', default=100)
    myargparser.add_argument('--maxtime', type=int, const=120, nargs='?', default=120)
    myargparser.add_argument('--id', type=int, const=0, nargs='?', default=-1)
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

    pv, qty, num_slices = 0, 0, 5
    if strategy == 'SniperA':
        num_slices, pv, qty = exec_bot.sniper_orders(args.size, args.action, log=True)

    end_t = time.time()
    exec_bot.task_complete(pv, qty, end_t-start_t, num_slices)
    sys.exit()

# looks like we have hung thread
# argparse for the arguments - inclusive symbol
# delays too long
# import of other modules not good
# where is direction buy or sell ?
