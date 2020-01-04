#!/usr/bin/env python3

# insert/update/delete trx table in testdb instance
# cx_Oracle does have bulk insert/update method, but I want to create performance problem to database, so

"""
usage:
import ora_trx
trx = ora_trx.Trx(trx_userid=2)
"""

""" trx table defination:
create table trx (
    trxid number(8),
    attr1 varchar2(10),
    attr2 varchar2(20),
    userid number(3),
    created date,
    updated date,
    constraint trx_pk primary key (trxid),
    constraint trx_nn_attr1 check (attr1 is not null),
    constraint trx_ck_attr2 check (attr1 in ('val1','val2', 'val3','val4')),
    constraint trx_nn_created check (created is not null),
    constraint trx_fk_user foreign key (userid) references users (userid)
);
"""

import cx_Oracle
from numpy import random
import time

class Trx(cx_Oracle.Connection):
    # some class variables
    # put attr2 list to class variables, trx table defination has check constraint on this column
    attr2_vals  = ['val1','val2','val3','val4']
    row_limit   = 100           # limit of row return from select trx table
    dml_limit   = 100           # limit of insert, delete, update
    dml_max     = 10000         # max value you can overwrite
    words       = None          # list of word "attr1" can chose from
    pause       = True          # sleep before each DML
    sleep_std   = 0.1           # normal dist standard deviation of 100ms
                                # 67% of values with in std away from mean(0), ~90% in 2 * std
   
    def __init__(self, cred = None, trx_userid = 1):
        self.userid = trx_userid
        if cred == None:
            cred = 'trx/trxpw@testdb_dga'
        try:
            super().__init__(cred)
        except Exception:
            print(f'unable to connect as {cred}')
            raise Exception
        else:
        #self.username
        # row limit option available after 12.1
            if self.version < '12':
                print(f"warning, database version {self.version} does not support row limit")
        #return conn
#        return super().__init__(cred)

    # alt-constructor, with pratise classmethod
    @classmethod
    # argument with default should follow arguments without default values
    # def from_dblogin(cls, trx_userid = 1, username, password, tnsname):
    def from_dblogin(cls, username, password, tnsname, trx_userid = 1):
        # the alt-constructor or factory method, can not call __init__ super directly because
        # only one __init__ is allowd in a class
        return cls(username + '/' + password + '@' + tnsname, trx_userid)

    @classmethod
    def set_dml_limit(cls, limit):
        if limit > cls.dml_max:
            print("unable to overwrite dml_limit greater than dml_max")
        else:
            cls.dml_limit = limit

    @classmethod
    def import_words(cls, file_name):
        # typical linux distro dictionary locatoin: /usr/share/dict
        cls.words = open(file_name).read().splitlines()

    # get username for this userid
    @property
    def trx_username(self):
        cur = self.cursor()
        cur.execute('select name from users where userid='+str(self.userid))
        row = cur.fetchone()
        cur.close()
        return row[0]

    # get and set userid
    @property
    def trx_userid(self):
        return self.userid

    @trx_userid.setter
    def trx_userid(self, trx_userid):
        self.userid = trx_userid

    def trx_pause(self):
        time.sleep(abs(random.normal(scale=self.sleep_std)))

    # DML operations
    # insert
    def insert_trx(self, cnt = 1):
        if cnt > self.dml_limit:
            cnt = self.dml_limit

        cur = self.cursor()
        print(f"{ self.trx_username } inserting { cnt }")
        stmt = """insert into trx 
        (trxid,attr1,attr2,userid,created) 
        values 
        (seq_trx.nextval, :attr1, :attr2, :userid, sysdate)
        """
        for i in range(cnt):
            if self.words != None:
                attr1 = random.choice(self.words)
            else:
                attr1 = 'foo'
            param = { 
                "attr1": attr1, 
                "attr2": random.choice(self.attr2_vals), 
                "userid": self.userid 
                }
            self.trx_pause()
            cur.execute(stmt, param)
        cur.close()

    # update some random transaction
    def update_trx(self, cnt = 1):
        if cnt > self.dml_limit:
            cnt = self.dml_limit
        print(f"{ self.trx_username } updating { cnt }")
        cur = self.cursor()
        stmt = """update trx set
        attr2 = :attr2,
        updated = sysdate
        where
        trxid = :trxid
        """
        for i in range(cnt):
            param = { 
                "attr2": random.choice(self.attr2_vals), 
                "trxid": int(self.get_trxid(1)) # cx_Oracle.NotSupportedError: Python value of type numpy.int64 not supported.
                }
            self.trx_pause()
            cur.execute(stmt, param)
        cur.close()

    # delete number of rows
    def delete_trx(self, cnt = 1):
        if cnt > self.dml_limit:
            cnt = self.dml_limit
        print(f"{ self.trx_username } deleting { cnt }")
        cur = self.cursor()
        # convert to list of string
        trx_list = list(map(str,self.get_trxid(cnt)))
        # trxid is primary key, should use exists if it is from a subquery
        # however we need python to sample the list, still using "in"
        stmt = "delete from trx where trxid in (" + ','.join(trx_list) +")"
        self.trx_pause() # not like insert/update, delete execute in one command
        cur.execute(stmt)
        cur.close()
 
    # pick random trx that is created by this user
    # return list of int
    def get_trxid(self, sample_cnt = 1):
        cur = self.cursor()
        stmt = "select trxid from trx where userid = :userid"
#        if self.dml_limit != None:
        if self.dml_limit > self.row_limit:
            stmt += " fetch first " + str(self.dml_limit) + " rows only"
        else:
            stmt += " fetch first " + str(self.row_limit) + " rows only"
        cur.execute(stmt, { "userid": self.userid })
        result = cur.fetchall()
        # dont use rowcount, it's for DML
        if len(result) < sample_cnt:
            # there's only one column selected, using position 0 for list compredension
            trxid=[ x[0] for x in result ]
        else:
            trxid=(random.choice([ x[0] for x in result ], sample_cnt))
        cur.close()
        return trxid

    # unnecessary for subclass
    #def __repr__(self):
    #    pass

    def __str__(self):
        return "trx table DML with userid={}".format(self.userid)
