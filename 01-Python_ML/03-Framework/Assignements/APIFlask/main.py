from flask import Flask, render_template, request, jsonify
import mysql.connector as mydb
import logging as logger
import csv
from csv import writer
import pymongo
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

app = Flask(__name__)

logger.basicConfig(filename='api.log', level= logger.INFO, format= '%(asctime)s %(message)s')

class mysql:
    def __init__(self):
        pass

    def mysqltablecreate(self,DATABASE,TABLENAME,COL1,CLOL1D,COL2,CLO21D,COL3,CLOL3D,COL4,CLOL4D):
        self.DATABASE = DATABASE
        self.TABLENAME = TABLENAME
        self.COL1 = COL1
        self.CLOL1D = CLOL1D
        self.COL2 = COL2
        self.CLO21D = CLO21D
        self.COL3 = COL3
        self.CLOL3D = CLOL3D
        self.COL4 = COL4
        self.CLOL4D = CLOL4D
        try:
            con = mydb.connect(host="localhost", user='root', passwd='Docker@123', database=self.DATABASE, use_pure=True)
            print(con.is_connected())
            query = "CREATE TABLE %s (%s %s  AUTO_INCREMENT PRIMARY KEY, %s %s, %s %s, %s %s )" % (self.TABLENAME, self.COL1, self.CLOL1D,self.COL2,self.CLO21D,self.COL3, self.CLOL3D,self.COL4, self.CLOL4D)
            print(query)
            cursor = con.cursor()
            cursor.execute(query)

        except Exception as e:
             logger.error(e)

        else:
            logger.info(f"{self.TABLENAME} Table created successfully")
            cursor.execute(f"show tables")
            return (cursor.fetchall())
        finally:
            con.close()

    def singleinsert(self,DATABASE, TABLENAME,COL1,COL2,COL3,COL4 ):
        self.DATABASE = DATABASE
        self.TABLENAME = TABLENAME
        self.COL1 = COL1
        self.COL2 = COL2
        self.COL3 = COL3
        self.COL4 = COL4

        try:
            con = mydb.connect(host="localhost", user='root', passwd='Docker@123', database=self.DATABASE, use_pure=True)
            print(con.is_connected())
            cursor = con.cursor()
            cursor.execute(f"INSERT  INTO {self.TABLENAME} VALUES('{self.COL1}','{self.COL2}','{self.COL3}','{self.COL4}')")
            con.commit()
        except Exception as e:
             logger.error(e)
             return (e)

        else:
            logger.info("Data inserted successfully")
            cursor.execute(f"select * from {self.TABLENAME}")
            logger.info (cursor.fetchall())
            cursor.execute(f"select * from {self.TABLENAME}")
            return (cursor.fetchall())
        finally:
            con.close()

    def mysqlupdatefun(self,DATABASE,TABLENAME,COL1,COL1VALUE,UPDATECOL,UPDATEVALUE):
        self.DATABASE = DATABASE
        self.TABLENAME = TABLENAME
        self.COL1 = COL1
        self.COL1VALUE = COL1VALUE
        self.UPDATECOL = UPDATECOL
        self.UPDATEVALUE = UPDATEVALUE

        try:
            con = mydb.connect(host="localhost", user='root', passwd='Docker@123', database=self.DATABASE, use_pure=True)
            print(con.is_connected())
            cursor = con.cursor()
            cursor.execute(f"UPDATE  {self.TABLENAME} SET {self.UPDATECOL}  = '{self.UPDATEVALUE}' WHERE {self.COL1} = '{self.COL1VALUE}'")
            con.commit()
        except Exception as e:
             logger.error(e)
             return (e)

        else:
            logger.info("Data updated successfully")
            cursor.execute(f"select * from {self.TABLENAME}")
            logger.info (cursor.fetchall())
            cursor.execute(f"select * from {self.TABLENAME}")
            return (cursor.fetchall())
        finally:
            con.close()
    def mysqlbulkupdatefun(self,DATABASE,TABLENAME,FILENAME):
        self.DATABASE = DATABASE
        self.TABLENAME = TABLENAME
        self.FILENAME = FILENAME

        try:
            con = mydb.connect(host="localhost", user='root', passwd='Docker@123', database=self.DATABASE, use_pure=True)
            print(con.is_connected())
            cursor = con.cursor()
            logger.info(self.FILENAME)
            with open(self.FILENAME, 'r') as f:
                next(f)
                data = csv.reader(f, delimiter="\n")
                for i in data:
                        logger.info(i)
                        cursor.execute(f'insert into {self.TABLENAME} values({i[0]}) ')
                print("Values Inserted")
                con.commit()

        except Exception as e:
             logger.error(e)
             return (e)

        else:
            logger.info("Bulk Data inserted successfully")
            cursor.execute(f"select * from {self.TABLENAME}")
            logger.info (cursor.fetchall())
            cursor.execute(f"select * from {self.TABLENAME}")
            return (cursor.fetchall())
        finally:
            con.close()

    def mysqldeleterecord(self,DATABASE,TABLENAME,COL1,COL1VALUE):
        self.DATABASE = DATABASE
        self.TABLENAME = TABLENAME
        self.COL1 = COL1
        self.COL1VALUE = COL1VALUE

        try:
            con = mydb.connect(host="localhost", user='root', passwd='Docker@123', database=self.DATABASE, use_pure=True)
            print(con.is_connected())
            cursor = con.cursor()
            cursor.execute(f"DELETE  FROM  {self.TABLENAME}  WHERE {self.COL1} = {self.COL1VALUE}")
            con.commit()
        except Exception as e:
             logger.error(e)
             return (e)

        else:
            logger.info("Data deleted successfully")
            cursor.execute(f"select * from {self.TABLENAME}")
            logger.info (cursor.fetchall())
            cursor.execute(f"select * from {self.TABLENAME}")
            return (cursor.fetchall())
        finally:
            con.close()


    def mysqlviewrecord(self,DATABASE,TABLENAME):
        self.DATABASE = DATABASE
        self.TABLENAME = TABLENAME
        try:
            con = mydb.connect(host="localhost", user='root', passwd='Docker@123', database=self.DATABASE, use_pure=True)
            print(con.is_connected())
            cursor = con.cursor()
            cursor.execute(f"select *  FROM {self.TABLENAME} ")
        except Exception as e:
             logger.error(e)
             return (e)
        else:
            return (cursor.fetchall())
        finally:
            con.close()


class mongodb:
    def __init__(self):
        pass
    def mongotablecreate(self,DATABASE,TABLE):
        self.DATABASE = DATABASE
        self.TABLE = TABLE

        try:
            client = pymongo.MongoClient()
            db = client[self.DATABASE]
            tb_1 = db[self.TABLE]
            logger.info(tb_1)
        except Exception as e:
            logger.error(e)
            return (e)
        else:
            return "Table created"

    def mongoaddrecord(self,DATABASE,TABLE,RECORD):
        self.DATABASE = DATABASE
        self.TABLE = TABLE
        self.RECORD = RECORD

        try:
            client = pymongo.MongoClient("mongodb://localhost:27017")
            db = client[self.DATABASE]
            tb_1 = db[self.TABLE]
            logger.info(self.RECORD)
            db1 = tb_1.insert_one(self.RECORD)
        except Exception as e:
            logger.error(e)
            return (e)
        else:
            return "data inserted successfully"

    def mongoupdate(self, DATABASE,TABLE,OLDRECORD,NEWRECORD):
        self.DATABASE = DATABASE
        self.TABLE = TABLE
        self.OLDRECORD =  OLDRECORD
        self.NEWRECORD = NEWRECORD

        try:
            client = pymongo.MongoClient("mongodb://localhost:27017")
            db = client[self.DATABASE]
            tb_1 = db[self.TABLE]
            tb_1.update_one(self.OLDRECORD,self.NEWRECORD)
        except Exception as e:
            logger.error(e)
            return (e)
        else:
            return "data updated  successfully"
    def mongodelrecord(self,DATABASE,TABLE,RECORD):
        self.DATABASE = DATABASE
        self.TABLE = TABLE
        self.RECORD = RECORD

        try:
            client = pymongo.MongoClient("mongodb://localhost:27017")
            db = client[self.DATABASE]
            tb_1 = db[self.TABLE]

            db1 = tb_1.delete_one(self.RECORD)
        except Exception as e:
            logger.error(e)
            return (e)
        else:
            return "data deleted successfully"

    def mongotableview(self, DATABASE, TABLE):
        self.DATABASE = DATABASE
        self.TABLE = TABLE

        try:
            client = pymongo.MongoClient("mongodb://localhost:27017")
            db = client[self.DATABASE]
            tb_1 = db[self.TABLE]
            data = tb_1.find_one()
            logger.info(data)
        except Exception as e:
            logger.error(e)
        else:
            return data


class cassendradb:
    def __init__(self):
        pass
    def cassendratablecreate(self,DATABASE,TABLE,COL1,COL1D,COL2,COL2D,COL3,COL3D):
        self.DATABASE = DATABASE
        self.TABLE = TABLE
        self.COL1 = COL1
        self.COL1D = COL1D
        self.COL2 = COL2
        self.COL2D = COL2D
        self.COL3 = COL3
        self.COL3D = COL3D


        try:
            cloud_config = {
                'secure_connect_bundle': 'E:\Cassandra\secure-connect-sachin.zip'
            }
            auth_provider = PlainTextAuthProvider('OooBtBPpsWTWhZOzaKxhvGBS','o-sxxJtw0k+hs+mJeGxiBpS9DS8+LAYXI49CpYxRMJgkGH6jok6mBSa85OQruq0WXN0Z1c1+,,bCke-Rr4+TY2ZnXFZAzJ4WRO2JXvxBHFkmkiITIAGQX7r,sw4lk6fc')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()
            row = session.execute(f"create table {self.DATABASE}.{self.TABLE }({self.COL1} {self.COL1D} PRIMARY KEY, {self.COL2} {self.COL2D},{self.COL3} {self.COL3D})").one()
        except Exception as e:
            logger.error(e)

        else:
            return "Table created successfully"

    def cassendrasingleadd(self,DATABASE,TABLE,COL1,COL1D,COL2,COL2D,COL3,COL3D):
        self.DATABASE = DATABASE
        self.TABLE = TABLE
        self.COL1 = COL1
        self.COL1D = COL1D
        self.COL2 = COL2
        self.COL2D = COL2D
        self.COL3 = COL3
        self.COL3D = COL3D

        try:
            cloud_config = {
                'secure_connect_bundle': 'E:\Cassandra\secure-connect-sachin.zip'
            }
            auth_provider = PlainTextAuthProvider('OooBtBPpsWTWhZOzaKxhvGBS',
                                                  'o-sxxJtw0k+hs+mJeGxiBpS9DS8+LAYXI49CpYxRMJgkGH6jok6mBSa85OQruq0WXN0Z1c1+,,bCke-Rr4+TY2ZnXFZAzJ4WRO2JXvxBHFkmkiITIAGQX7r,sw4lk6fc')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()
            row = session.execute(
                f"insert into {self.DATABASE}.{self.TABLE} ({self.COL1D},{self.COL2D},{self.COL3D}) values({self.COL1},'{self.COL2}','{self.COL3}')").one()
        except Exception as e:
            logger.error(e)
        else:
            return "data inserted successfully"

    def cassendrasingleupdate(self,DATABASE,TABLE,COL1,COL1D,COL2,COL2D):
        self.DATABASE = DATABASE
        self.TABLE = TABLE
        self.COL1 = COL1
        self.COL1D = COL1D
        self.COL2 = COL2
        self.COL2D = COL2D


        try:
            cloud_config = {
                'secure_connect_bundle': 'E:\Cassandra\secure-connect-sachin.zip'
            }
            auth_provider = PlainTextAuthProvider('OooBtBPpsWTWhZOzaKxhvGBS',
                                                  'o-sxxJtw0k+hs+mJeGxiBpS9DS8+LAYXI49CpYxRMJgkGH6jok6mBSa85OQruq0WXN0Z1c1+,,bCke-Rr4+TY2ZnXFZAzJ4WRO2JXvxBHFkmkiITIAGQX7r,sw4lk6fc')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            row = session.execute( f"UPDATE {self.DATABASE}.{self.TABLE} SET {self.COL1} = '{self.COL1D}' WHERE {self.COL2}={self.COL2D}").one()
        except Exception as e:
            logger.error(e)
        else:
            return "data Updated successfully"

    def cassendradeleterecord(self,DATABASE,TABLE,COL1,COL1D):
        self.DATABASE = DATABASE
        self.TABLE = TABLE
        self.COL1 = COL1
        self.COL1D = COL1D



        try:
            cloud_config = {
                'secure_connect_bundle': 'E:\Cassandra\secure-connect-sachin.zip'
            }
            auth_provider = PlainTextAuthProvider('OooBtBPpsWTWhZOzaKxhvGBS',
                                                  'o-sxxJtw0k+hs+mJeGxiBpS9DS8+LAYXI49CpYxRMJgkGH6jok6mBSa85OQruq0WXN0Z1c1+,,bCke-Rr4+TY2ZnXFZAzJ4WRO2JXvxBHFkmkiITIAGQX7r,sw4lk6fc')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            row = session.execute( f"delete from {self.DATABASE}.{self.TABLE}  WHERE {self.COL1}={self.COL1D}").one()
        except Exception as e:
            logger.error(e)
        else:
            return "data deleted successfully"


    def cassendraview(self,DATABASE,TABLE):
        self.DATABASE = DATABASE
        self.TABLE = TABLE
        try:
            cloud_config = {
                'secure_connect_bundle': 'E:\Cassandra\secure-connect-sachin.zip'
            }
            auth_provider = PlainTextAuthProvider('OooBtBPpsWTWhZOzaKxhvGBS',
                                                  'o-sxxJtw0k+hs+mJeGxiBpS9DS8+LAYXI49CpYxRMJgkGH6jok6mBSa85OQruq0WXN0Z1c1+,,bCke-Rr4+TY2ZnXFZAzJ4WRO2JXvxBHFkmkiITIAGQX7r,sw4lk6fc')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            row = session.execute( f"select *  from {self.DATABASE}.{self.TABLE} ").one()
            return (row)

        except Exception as e:
            logger.error(e)
        else:
            logger.info("Data view")


@app.route('/cassendra/view', methods=['POST'])
def cassendraview():
    DATABASE = request.json['DATABASE']
    TABLE = request.json['TABLE']


    csview = cassendradb()
    return jsonify(csview.cassendraview(DATABASE, TABLE))

@app.route('/cassendra/delete', methods=['POST'])
def cassendradelete():
    DATABASE = request.json['DATABASE']
    TABLE = request.json['TABLE']
    COL1 = request.json['COL1']
    COL1D = request.json['COL1DATA']

    csdel = cassendradb()
    return jsonify(csdel.cassendradeleterecord(DATABASE, TABLE, COL1, COL1D))



@app.route('/cassendra/update', methods=['POST'])
def cassendraupdate():
    DATABASE = request.json['DATABASE']
    TABLE = request.json['TABLE']
    COL1 = request.json['COL1']
    COL1D = request.json['COL1DATA']
    COL2 = request.json['COL2']
    COL2D = request.json['COL2DATA']

    csupdate = cassendradb()
    return jsonify(csupdate.cassendrasingleupdate(DATABASE, TABLE, COL1, COL1D, COL2, COL2D))

@app.route('/cassendra/add', methods=['POST'])
def cassendraadd():
    DATABASE = request.json['DATABASE']
    TABLE = request.json['TABLE']
    COL1 = request.json['COL1']
    COL1D = request.json['COL1DATATYPE']
    COL2 = request.json['COL2']
    COL2D = request.json['COL2DATATYPE']
    COL3 = request.json['COL3']
    COL3D = request.json['COL3DATATYPE']

    csadd = cassendradb()
    return jsonify(csadd.cassendrasingleadd(DATABASE, TABLE, COL1, COL1D, COL2, COL2D, COL3, COL3D))

@app.route('/cassendra/table', methods=['POST'])
def cassendratable():
    DATABASE = request.json['DATABASE']
    TABLE = request.json['TABLE']
    COL1 = request.json['COL1']
    COL1D = request.json['COL1DATATYPE']
    COL2 = request.json['COL2']
    COL2D = request.json['COL2DATATYPE']
    COL3 = request.json['COL3']
    COL3D = request.json['COL3DATATYPE']

    cstable = cassendradb()
    return  jsonify(cstable.cassendratablecreate(DATABASE,TABLE,COL1,COL1D,COL2,COL2D,COL3,COL3D))


@app.route('/mongo/tablecreate', methods=['POST'])
def mongotable():
    DATABASE = request.json['DATABASENAME']
    TABLE = request.json['TABLENAME']

    mongotb = mongodb()
    return jsonify(mongotb.mongotablecreate(DATABASE,TABLE))


@app.route('/mango/record', methods=['POST'])
def mongoadd():
    DATABASE = request.json['DATABASENAME']
    TABLE = request.json['TABLENAME']
    RECORD = request.json['RECORD']


    mongoadd = mongodb()
    return  jsonify(mongoadd.mongoaddrecord(DATABASE,TABLE,RECORD))

@app.route('/mongo/update', methods=['POST'])
def mongoupdate():
    DATABASE = request.json['DATABASENAME']
    TABLE = request.json['TABLENAME']
    OLDRECORD = request.json['OLDRECORD']
    NEWRECORD = request.json['NEWRECORD']

    mongoup = mongodb()
    return jsonify(mongoup.mongoupdate(DATABASE,TABLE,OLDRECORD,NEWRECORD))


@app.route('/mongo/delete', methods=['POST'])
def mongodelete():
    DATABASE = request.json['DATABASENAME']
    TABLE = request.json['TABLENAME']
    RECORD = request.json['RECORD']

    mongodel = mongodb()
    return jsonify(mongodel.mongodelrecord(DATABASE,TABLE,RECORD))


@app.route('/mongo/view', methods=['POST'])
def mongoview():
    DATABASE = request.json['DATABASENAME']
    TABLE = request.json['TABLENAME']
    print(TABLE)
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client[DATABASE]
    tb_1 = db[TABLE]
    data = tb_1.find_one()
    return (data)

    #mongoview = mongodb()
    #return jsonify(mongoview.mongotableview(DATABASE,TABLE))


@app.route('/mysql/tablecreate', methods=['POST'])
def mysqltable():
    DATABASE = request.json['DATABASENAME']
    TABLENAME = request.json['TABLENAME']
    COL1 = request.json['COL1']
    CLOL1D  = request.json['CLOLDATATYPE']
    COL2 = request.json['COL2']
    CLO21D = request.json['CLOL2DATATYPE']
    COL3 = request.json['COL3']
    CLOL3D = request.json['CLOL3DATATYPE']
    COL4 = request.json['COL4']
    CLOL4D = request.json['CLOL4DATATYPE']

    mtable=mysql()
    return jsonify(mtable.mysqltablecreate(DATABASE,TABLENAME,COL1,CLOL1D,COL2,CLO21D,COL3,CLOL3D,COL4,CLOL4D))


@app.route('/mysql/singleinsert', methods=['POST'])
def mysqlsingle():
    DATABASE = request.json['DATABASENAME']
    TABLENAME = request.json['TABLENAME']
    COL1 = request.json['COL1']
    COL2 = request.json['COL2']
    COL3 = request.json['COL3']
    COL4 = request.json['COL4']

    msingle = mysql()
    return jsonify(msingle.singleinsert(DATABASE,TABLENAME,COL1,COL2,COL3,COL4))


@app.route('/mysql/update', methods=['POST'])
def mysqlupdate():
    DATABASE = request.json['DATABASENAME']
    TABLENAME = request.json['TABLENAME']
    COL1 = request.json['COL1']
    COL1VALUE = request.json['COL1VALUE']
    UPDATECOL=request.json['UPDATECOL']
    UPDATEVALUE = request.json["UPDATEVALUE"]

    update = mysql()
    return  jsonify(update.mysqlupdatefun(DATABASE,TABLENAME,COL1,COL1VALUE,UPDATECOL,UPDATEVALUE))


@app.route('/mysql/bulk', methods=['POST'])
def mysqlbulk():
    DATABASE = request.json['DATABASENAME']
    TABLENAME = request.json['TABLENAME']
    FILENAME = request.json['FILENAME']

    bulk = mysql()
    return  jsonify(bulk.mysqlbulkupdatefun(DATABASE,TABLENAME,FILENAME))

@app.route('/mysql/delete', methods=['POST'])
def mysqldelete():
    DATABASE = request.json['DATABASENAME']
    TABLENAME = request.json['TABLENAME']
    COL1 = request.json['COL1']
    COL1VALUE = request.json['COL1VALUE']

    delete = mysql()
    return  jsonify(delete.mysqldeleterecord(DATABASE,TABLENAME,COL1,COL1VALUE))
@app.route('/mysql/view',methods=['POST'])
def view():
    DATABASE = request.json['DATABASENAME']
    TABLENAME = request.json['TABLENAME']

    tbview = mysql()
    return  jsonify(tbview.mysqlviewrecord(DATABASE,TABLENAME))



if __name__ == '__main__':
    app.run()

