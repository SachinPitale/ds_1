from flask import Flask, render_template, request, jsonify

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST']) # To render Homepage
def home_page():
    return render_template('index.html')

@app.route('/math', methods=['POST'])  # This will be called from UI
def math_operation():
    if (request.method=='POST'):
        operation=request.form['operation']
        num1=int(request.form['num1'])
        num2 = int(request.form['num2'])
        if(operation=='add'):
            r=num1+num2
            result= 'the sum of '+str(num1)+' and '+str(num2) +' is '+str(r)
        if (operation == 'subtract'):
            r = num1 - num2
            result = 'the difference of ' + str(num1) + ' and ' + str(num2) + ' is ' + str(r)
        if (operation == 'multiply'):
            r = num1 * num2
            result = 'the product of ' + str(num1) + ' and ' + str(num2) + ' is ' + str(r)
        if (operation == 'divide'):
            r = num1 / num2
            result = 'the quotient when ' + str(num1) + ' is divided by ' + str(num2) + ' is ' + str(r)
        return render_template('results.html',result=result)

@app.route('/via_postman', methods=['POST']) # for calling the API from Postman/SOAPUI
def math_operation_via_postman():
    if (request.method=='POST'):
        operation=request.json['operation']
        num1=int(request.json['num1'])
        num2 = int(request.json['num2'])
        if(operation=='add'):
            r=num1+num2
            result= 'the sum of '+str(num1)+' and '+str(num2) +' is '+str(r)
        if (operation == 'subtract'):
            r = num1 - num2
            result = 'the difference of ' + str(num1) + ' and ' + str(num2) + ' is ' + str(r)
        if (operation == 'multiply'):
            r = num1 * num2
            result = 'the product of ' + str(num1) + ' and ' + str(num2) + ' is ' + str(r)
        if (operation == 'divide'):
            r = num1 / num2
            result = 'the quotient when ' + str(num1) + ' is divided by ' + str(num2) + ' is ' + str(r)
        return jsonify(result)

@app.route('/sachin',methods=['POST'])
def numeven():
    if (request.method=='POST'):
        num = int(request.json['num'])
        if( num > 0 ):
            result = ' the user number of'+ " " +str(num) + " " +'is even number'
        else:
            result = ' the user number of'+ " " +str(num) + " " +'is odd number'
    return jsonify(result)

@app.route('/emp', methods=['POST'])
def emp1():
    if (request.method =='POST'):
        name = request.json['name']
        email = request.json['email']
        result = name + '@'+email
    return jsonify(result)

@app.route('/name', methods=['GET'])
def name():
    if (request.method =='GET'):
        result = "My name is sachin and i am learning API"
    return jsonify(result)


@app.route('/sachin_fun')
def sac_fun():
    num1 = int(request.args.get('val1'))
    num2 = int(request.args.get('val2'))
    sum = num1 + num2
    return "Total sum of both number is {}".format(sum)



#http://127.0.0.1:5000/sachin_fun?val1=2&val2=4


@app.route('/myname')
def username():
    name = request.args.get('name')


    return '''<h1>Hello {}, welcome to api world</h1>'''.format(name)


if __name__ == '__main__':
    app.run()
