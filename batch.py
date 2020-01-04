#!/usr/bin/env python3

import ora_trx
import concurrent.futures
import time
import random
import argparse

max_insert = 20

# a database session
def worker(userid, insert_cnt = 1, update_cnt = 1, delete_cnt = 1):
    trx = ora_trx.Trx(trx_userid=userid)
    trx.import_words('/usr/share/dict/cracklib-small')
    trx.insert_trx(cnt = insert_cnt)
    trx.update_trx(cnt = update_cnt)
    trx.delete_trx(cnt = delete_cnt)
    trx.commit()
    trx.close()


"""
trx = ora_trx.Trx(trx_userid=2)
#trx = ora_trx.Trx.from_dblogin('trx','trxpw','testdb_dga',2)
print(trx.trx_username)
trx.trx_userid = 3
print(trx.trx_username)
#print(trx)
#trx.print_users()
trx.import_words('/usr/share/dict/cracklib-small')
trx.insert_trx(10)
trx.update_trx(7)
trx.delete_trx(5)
#trx.set_dml_limit(100)
#trx.delete_trx(100)
trx.commit()
trx.close()
"""

def batch():
    # worker(2, 10, 5, 7)   # test with 1 process
    userids = range(1,11)
    # list of worker insert count
    insert_cnts = [ random.randint(1,max_insert) for _ in range(1,11) ]
    # list of worker update count, that is less than insert of each process
    update_cnts = [ random.randint(1, x) for x in insert_cnts ]
    # same as update
    delete_cnts = [ random.randint(1, x) for x in insert_cnts ]
    # print(insert_cnts, update_cnts, delete_cnts)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = executor.map(worker, userids, insert_cnts, update_cnts, delete_cnts)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--batch', type = int, required = False, default = 10, help="number of batch run, default 10")
    args = parser.parse_args()
    n = args.batch
    for i in range(1,n+1):
        start_time = time.time()
        batch()
        end_time = time.time()
        print(f'batch {i} finished, elapsed { round(end_time - start_time,2)} seconds')

if __name__ == '__main__':
    main()
