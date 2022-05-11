from flask import Flask, render_template, request, jsonify
import mysql.connector as mydb

app = Flask(__name__)

@app.route('/mysql/tablecreate', methods=['POST'])
def mysqltable():
    DATABASE = request.json['DATABASENAME']
    TABLE = request.json['TABLENAME']
    COL1 = request.json['COL1']
    CLOL1D  = request.json['CLOLDATATYPE']
    COL2 = request.json['COL2']
    CLO21D = request.json['CLOL2DATATYPE']
    COL3 = request.json['COL3']
    CLOL3D = request.json['CLOL3DATATYPE']
    COL4 = request.json['COL4']
    CLOL4D = request.json['CLOL4DATATYPE']
    COL5 = request.json['COL4']
    CLOL5D = request.json['CLOL4DATATYPE']

    con = mydb.connect(host="localhost", user='root',passwd='Docker@123',database=DATABASE,use_pure=True)
    query = "CREATE TABLE TABLE (COL1 CLOL1D, COL2 CLO21D," \
            "COL3 CLOL3D,COL4 CLOL4D, COL5 CLOL5D"
    cursor = con.cursor()
    cursor.execute(query)
    con.close()



if __name__ == '__main__':
    app.run()

