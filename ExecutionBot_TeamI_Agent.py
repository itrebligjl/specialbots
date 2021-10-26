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
            'bp': self.mid_market[sym],
            'vwap': self.vwap[sym]
        }

    def task_complete(self, pv, qty, time_t, slices):
        self.stat['pv'] = pv
        self.stat['qty'] = qty
        self.stat['time_t'] = time_t
        self.stat['slices'] = slices
        self.stop(self.stat, log=True)
    
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
                    # --------------------debug-------------------------
                    level_size = self.market_dict[sym][level][book_side + 'Size']
                    level_price = self.market_dict[sym][level][book_side + 'Price']
                    print(f'level is {level}, size in this level is {level_size}, price in this level is {level_price}')
                    # --------------------debug-----------------------
                    size_level = min(qty-size, self.market_dict[sym][level][book_side + 'Size'])
                    size += int(size_level)

                    order_prices.append(self.market_dict[sym][level][book_side + 'Price'])
                    order_qty.append(size_level)
                    # -------------------debug-------------------
                    print(f'pty is {qty}, size_leve is {size_level}')
                    # -------------------debug-------------------
                except:
                    pass
            # print(order_qty)

            # TODO: what if the whole book is insufficient? -> qty = size

            # ----------------debug----------------
            print(order_prices)
            print(order_qty)
            # ----------------debug----------------
            
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
            cost_qty = pv / (qty_target - qty) - benchmark_price*1.
        except:
            cost_qty=999.99
            benchmark_price = 999.99
        if action == 'buy':
            cost_qty *= -1

        logging.info(f'\n\t Aggressive order: {action} {qty_target-qty} {sym} given {min(time.time() - t_start, exec_t)} seconds: \n'
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

    def twap_orders(self, qty, action, n_slices, exec_t=3.0):
        """
        send evenly allocated orders within fixed sub periods, with passive orders followed by aggressive orders
        :param qty: total target qty
        :param action: 'buy' or 'sell'
        :param exec_t: x seconds
        :param n_slices: # of slices
        :return: transaction cost per share = vwap (executed) - benchmark price
        """

        sym = self.securities[0]

        book_side = 'Ask' if action == 'buy' else 'Bid'
        side = 'B' if action == 'buy' else 'S'

        # benchmark price
        benchmark_price = self.mid_market[sym]
        benchmark_vwap = self.vwap[sym]
        
        # modify_3
        pre_vwap = benchmark_vwap

        # target qty
        qty_target = qty

        # Modify_1: setup timer
        t_start = time.time()

        # Modify_1: max time
        max_time = exec_t * n_slices

        # pv = execution price * volume
        pv = 0

        # print(self.market_dict[sym])

        qty_slice = 0
        for i in range(n_slices):
            if qty <= 0:
                break

            # ----------- Modify_2 begin ------------
            # setting p as L1 price is not reasonable -> may lead to the fact that too much order cancelled
            # p = self.market_dict[sym]['L1'][book_side + 'Price']

            ## Modify_3
            if action == 'buy':
                expand_rate = 1.2 if self.vwap[sym] - pre_vwap < 0 else 1 #expand_rate = 1.2 if vwap decrease
            else:
                expand_rate = 1 if self.vwap[sym] - pre_vwap < 0 else 1.2 #expand_rate = 1.2 if vwap decrease
            
            # ----debug----
            print(f'pre_vwap: {pre_vwap}, current vwap: {self.vwap[sym]}')
            print(f'expand_rate is {expand_rate}')
            # ----debug----

            pre_vwap = self.vwap[sym]
            target_q = int(qty / (n_slices - i) * expand_rate)
            # target_q: the total qty we want to fill in this slice
            
            # qty_slice: possible unfilled size in the previous slice
            # set qty_slice = 0 casue qty_slice has been absorbed by target_q
            qty_slice = 0
            # ----------- Modify_2 end ------------


            # ----------- Modify_2 begin ------------
            
            # search the price level that covers all qty
            book_levels = self.market_event_queue.copy()
            size = 0
            order_prices = []
            order_qty = []

            # determine the order price/size to be executed
            while size < target_q and len(book_levels) > 0:
                level = book_levels.pop(0)
                try:
                    # --------------------debug-------------------------
                    level_size = self.market_dict[sym][level][book_side + 'Size']
                    level_price = self.market_dict[sym][level][book_side + 'Price']
                    print(f'level is {level}, size in this level is {level_size}, price in this level is {level_price}')
                    # --------------------debug-----------------------

                    size_level = min(target_q - size, self.market_dict[sym][level][book_side + 'Size'])
                    size += int(size_level)

                    order_prices.append(self.market_dict[sym][level][book_side + 'Price'])
                    order_qty.append(size_level)
                except Exception:
                    print(f'{sym} dont have level {level} price')
                    pass
            
            # note that it's possible that size is finally less than target_q
            # cause the whole book might be insufficient
            
            # ----------------debug----------------
            print(order_prices)
            print(order_qty)
            # ----------------debug----------------

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
                logging.info(f"Slice {i+1} - twap order sent: \n"
                             f"\t {order['symb']}: "
                             f"{order['orderNo']} | "
                             f"{order['side']} | "
                             f"{order['origQty']} | "
                             f"{order['remainingQty']} | "
                             f"{order['price']}")

                orders.append(order)
                self.internalID += 1
            # ----------- Modify_2 end ------------


            logging.info(f'Giving {exec_t} seconds for limit orders to be filled...')
            sleep(exec_t)

            # print(self.market_dict[sym])

            # ----------- Modify_2 begin ------------
            for order in orders:
                # make sure all orders are acked on matching enginee
                in_id = order["orderNo"]

                while in_id in self.inIds_to_orders_sent:
                    sleep(0.001)
                    # print(f'waiting for pending orders...')

                # cancel only if the order is not fully filled
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

                    # qty unfilled in this order
                    qty_slice += order['remainingQty']

                    # increment pv by filled amount
                    pv += order['price'] * (order['origQty'] - order['remainingQty'])

                # Modify_1: don't do aggressive_roders at the end of every slice
                # pv_slice, qty_slice = self.aggressive_orders(qty_slice, action)
                # pv += pv_slice

                # increment pv by fully filled amount
                else:
                    self.logger.info(f"Fully filled limit order: \n"
                                     f"\t {order['symb']}: "
                                     f"{order['orderNo']} | "
                                     f"{order['side']} | "
                                     f"{order['remainingQty']} | "
                                     f"{order['price']}")

                    pv += order['price'] * order['origQty']
            # ----------- Modify_2 end ------------

            # Modify_1: update qty
            # note that "qty -= target_q - qty_slice" might be wrong
            # cause size might not be equal to target_q
            qty -= size - qty_slice
            qty_slice += target_q - size

            # Modify_1: go aggressive when don't have enough time to do next slice
            # todo: the code won't be executed
            if max_time + t_start - time.time() < 1 and qty > 0:
                print(f'Emergency, go aggressive. Remain qty = {qty}')
                pv_slice, qty_slice = self.aggressive_orders(qty, action)
                pv += pv_slice
                break

        # avg execution price
        try:
            cost_qty = pv / (qty_target - qty_slice) - benchmark_price * 1.
        except:
            cost_qty = 999.99
            benchmark_price = 999.99

        if action == 'buy':
            cost_qty *= -1

        logging.info(f'\n\t Slicing order: {action} {qty_target-qty_slice} {sym}\n'
                     f'\t Given {n_slices} slices per {exec_t} seconds: \n'
                     f'\t Transaction cost: {cost_qty} per share\n'
                     f'\t Benchmark price: {benchmark_price}\n'
                     f'\t Benchmark VWAP: {benchmark_vwap}')

        # final liquidation
        penalty, pv_final = self.final_liquidation(qty_slice, action)

        # final TC
        cost_qty = (pv + pv_final) / qty_target - benchmark_price
        if action == 'buy':
            cost_qty *= -1

        return pv, qty

    def final_liquidation(self, qty, action, exec_t=30):

        penalty = 0
        pv_final = 0

        if qty > 0:
            pv_final, _ = self.aggressive_orders(qty, action, exec_t)
            penalty = self.penalty * qty

        return penalty, pv_final

if __name__ == "__main__":
    # market_event_securities = ["GEH0:MBO","GEM2:MBO","GEU0:MBO"]
    myargparser=argparse.ArgumentParser()
    myargparser.add_argument('--strategy', type=str, const="TWAP", nargs='?', default="TWAP")
    myargparser.add_argument('--symbol',type=str,const="ZNH0:MBO",nargs='?',default="ZBH0:MBO")
    myargparser.add_argument('--action',type=str,const="buy",nargs='?',default="buy")
    myargparser.add_argument('--size',type=int,const=1000,nargs='?',default=100)
    myargparser.add_argument('--maxtime',type=int,const=120,nargs='?',default=120)
    myargparser.add_argument('--username', type=str, default='test')
    myargparser.add_argument('--password', type=str, default='test')
    myargparser.add_argument('--bot_id', type=str, const='text', nargs='?', default='text')
    args=myargparser.parse_args()
    
    market_event_securities = [args.symbol]
    market_event_queue = ["L1", "L2","L3","L4","L5"]
    securities = market_event_securities
    host = "localhost"
    strategy = args.strategy
    bot_id = args.bot_id
    starting_money = 1000000000.0

    start_t = time.time()
    exec_bot = ExecutionBot(strategy, starting_money, market_event_securities, market_event_queue, securities,
                           host, bot_id)
    exec_bot.start_task(args.symbol, args.action, args.size, args.username, args.password)

    pv, qty, num_slices = 0, 0, 10
    if strategy == 'TWAP':
        pv, qty = exec_bot.twap_orders(args.size, args.action, num_slices, int(args.maxtime/numslices))

    end_t = time.time()
    exec_bot.task_complete(pv, qty, end_t-start_t, num_slices)
    sys.exit()

# looks like we have hung thread
# argparse for the arguments - inclusive symbol
# delays too long
# import of other modules not good
# where is direction buy or sell ?
